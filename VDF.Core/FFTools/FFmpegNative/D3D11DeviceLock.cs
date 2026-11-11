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

namespace VDF.Core.FFTools.FFmpegNative {
	/// <summary>
	/// Scoped acquisition of an FFmpeg D3D11VA device lock — the callbacks FFmpeg itself
	/// uses around <c>ID3D11DeviceContext</c> / <c>ID3D11VideoContext</c> calls. Our own
	/// video-processor work on a decoder's device takes it so it never interleaves with
	/// the decoder's submissions on that device's immediate context.
	/// </summary>
	readonly unsafe struct D3D11DeviceLock : IDisposable {
		readonly AVD3D11VADeviceContext* _context;

		D3D11DeviceLock(AVD3D11VADeviceContext* context) {
			_context = context;
			if (_context != null && _context->@lock.Pointer != IntPtr.Zero)
				((delegate* unmanaged[Cdecl]<void*, void>)_context->@lock.Pointer)(_context->lock_ctx);
		}

		/// <summary>Locks the D3D11VA device referenced by <paramref name="deviceRef"/> (an AVHWDeviceContext buffer).</summary>
		internal static D3D11DeviceLock ForDevice(AVBufferRef* deviceRef) {
			if (deviceRef == null || deviceRef->data == null)
				return default;
			AVHWDeviceContext* deviceContext = (AVHWDeviceContext*)deviceRef->data;
			if (deviceContext->type != AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA)
				return default;
			return new D3D11DeviceLock((AVD3D11VADeviceContext*)deviceContext->hwctx);
		}

		public void Dispose() {
			if (_context != null && _context->unlock.Pointer != IntPtr.Zero)
				((delegate* unmanaged[Cdecl]<void*, void>)_context->unlock.Pointer)(_context->lock_ctx);
		}
	}
}
