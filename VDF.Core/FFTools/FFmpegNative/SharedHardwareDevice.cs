// /*
//     Copyright (C) 2026 0x90d
//     This file is part of VideoDuplicateFinder
//     VideoDuplicateFinder is free software: you can redistribute it and/or modify
//     it under the terms of the GNU Affero General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
//     VideoDuplicateFinder is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU Affero General Public License for more details.
//     You should have received a copy of the GNU Affero General Public License
//     along with VideoDuplicateFinder.  If not, see <http://www.gnu.org/licenses/>.
// */
//

using FFmpeg.AutoGen;
using VDF.Core.Utils;
using VorticeID3D11Device = Vortice.Direct3D11.ID3D11Device;

namespace VDF.Core.FFTools.FFmpegNative {
	/// <summary>
	/// One process-wide D3D11VA hardware device that every decoder shares, instead of a new
	/// device per file. Creating a D3D11VA device costs 160–240 ms wall and 170–310 ms of CPU
	/// (RTX 2070 SUPER, FFmpeg 8), more than decoding the handful of samples a file needs, so
	/// per-file creation made the GPU path use more CPU than CPU decoding.
	///
	/// Sharing is what FFmpeg supports (and what the ffmpeg CLI does with -init_hw_device):
	/// the device is created without D3D11_CREATE_DEVICE_SINGLETHREADED and with
	/// SetMultithreadProtected(TRUE), and the D3D11VA hwaccel, av_hwframe_transfer_data and
	/// our video-processor scaler all take the device's own lock (a mutex FFmpeg creates per
	/// device) around immediate-context work. Each decoder still gets its own decoder object
	/// and surface pool.
	///
	/// If the GPU removes the device (driver reset, TDR, adapter change) the cached device is
	/// dropped and the next file gets a fresh one; decoders still holding the old device keep
	/// their reference and fail on their own. Only D3D11VA is shared; other device types are
	/// still created per decoder. Opt out with VDF_DISABLE_SHARED_HWDEVICE=1.
	/// </summary>
	static unsafe class SharedHardwareDevice {
		internal const string DisableEnvVar = "VDF_DISABLE_SHARED_HWDEVICE";

		static readonly object Gate = new();
		// The cache's own reference; every decoder holds an additional av_buffer_ref of it.
		static AVBufferRef* _d3d11Device;
		static int _sharedCreations;

		/// <summary>How many shared devices this process has created (one unless a device was lost).</summary>
		internal static int SharedCreations => Volatile.Read(ref _sharedCreations);

		internal static bool IsSharingEnabled(AVHWDeviceType deviceType) =>
			deviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA
			&& OperatingSystem.IsWindows()
			&& !IsEnvFlagSet(DisableEnvVar);

		/// <summary>
		/// Returns a hardware device reference owned by the caller (for <c>AVCodecContext.hw_device_ctx</c>,
		/// which <c>avcodec_free_context</c> releases). Throws with the <c>av_hwdevice_ctx_create</c>
		/// error text when no device can be created, as before.
		/// </summary>
		internal static AVBufferRef* Acquire(AVHWDeviceType deviceType) {
			if (!IsSharingEnabled(deviceType))
				return CreateDevice(deviceType);

			lock (Gate) {
				if (_d3d11Device != null && IsDeviceRemoved(_d3d11Device, out string reason)) {
					Logger.Instance.Info($"The shared D3D11VA decode device was lost ({reason}); creating a new one for the next file.");
					DropLocked();
				}
				if (_d3d11Device == null) {
					_d3d11Device = CreateDevice(deviceType);
					Interlocked.Increment(ref _sharedCreations);
				}
				AVBufferRef* reference = ffmpeg.av_buffer_ref(_d3d11Device);
				if (reference == null)
					throw new FFInvalidExitCodeException("av_buffer_ref failed for the shared D3D11VA device.");
				return reference;
			}
		}

		/// <summary>Drops the cached device so the next <see cref="Acquire"/> creates a new one.</summary>
		internal static void Invalidate() {
			lock (Gate)
				DropLocked();
		}

		static void DropLocked() {
			if (_d3d11Device == null)
				return;
			AVBufferRef* device = _d3d11Device;
			_d3d11Device = null;
			// Decoders still using it hold their own references; this releases only the cache's.
			ffmpeg.av_buffer_unref(&device);
		}

		static AVBufferRef* CreateDevice(AVHWDeviceType deviceType) {
			AVBufferRef* device = null;
			ffmpeg.av_hwdevice_ctx_create(&device, deviceType, null, null, 0).ThrowExceptionIfError($"av_hwdevice_ctx_create({deviceType})");
			return device;
		}

		/// <summary>The D3D11 device behind an FFmpeg D3D11VA device reference, or zero.</summary>
		internal static nint GetD3D11Device(AVBufferRef* deviceRef) {
			if (deviceRef == null || deviceRef->data == null)
				return 0;
			AVHWDeviceContext* deviceContext = (AVHWDeviceContext*)deviceRef->data;
			if (deviceContext->type != AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA || deviceContext->hwctx == null)
				return 0;
			return (nint)((AVD3D11VADeviceContext*)deviceContext->hwctx)->device;
		}

		/// <summary>
		/// ID3D11Device::GetDeviceRemovedReason: S_OK while the device is usable, a DXGI_ERROR_DEVICE_*
		/// code once the GPU has removed it. ID3D11Device methods are free-threaded, so this is
		/// safe while other threads decode on the device.
		/// </summary>
		static bool IsDeviceRemoved(AVBufferRef* deviceRef, out string reason) {
			reason = string.Empty;
			nint devicePointer = GetD3D11Device(deviceRef);
			if (devicePointer == 0)
				return false;
			using var device = new VorticeID3D11Device(devicePointer);
			device.AddRef();
			SharpGen.Runtime.Result removed = device.DeviceRemovedReason;
			if (removed.Success)
				return false;
			reason = $"0x{removed.Code:X8}";
			return true;
		}

		static bool IsEnvFlagSet(string name) {
			string? value = Environment.GetEnvironmentVariable(name);
			return value != null
				&& (value == "1"
					|| value.Equals("true", StringComparison.OrdinalIgnoreCase)
					|| value.Equals("yes", StringComparison.OrdinalIgnoreCase)
					|| value.Equals("on", StringComparison.OrdinalIgnoreCase));
		}
	}
}
