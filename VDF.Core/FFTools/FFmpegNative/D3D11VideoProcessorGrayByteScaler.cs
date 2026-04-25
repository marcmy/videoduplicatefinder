// /*
//     Copyright (C) 2025 0x90d
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

using System.Diagnostics;
using FFmpeg.AutoGen;
using Vortice.Direct3D11;
using Vortice.DXGI;
using VorticeID3D11Device = Vortice.Direct3D11.ID3D11Device;
using VorticeID3D11DeviceContext = Vortice.Direct3D11.ID3D11DeviceContext;
using VorticeID3D11Texture2D = Vortice.Direct3D11.ID3D11Texture2D;
using VorticeID3D11VideoContext = Vortice.Direct3D11.ID3D11VideoContext;
using VorticeID3D11VideoDevice = Vortice.Direct3D11.ID3D11VideoDevice;

namespace VDF.Core.FFTools.FFmpegNative {
	sealed unsafe class D3D11VideoProcessorGrayByteScaler : IDisposable {
		const int GraySize = 32;
		const int PendingDownloadSlots = 3;
		VorticeID3D11Device? _device;
		VorticeID3D11DeviceContext? _deviceContext;
		VorticeID3D11VideoDevice? _videoDevice;
		VorticeID3D11VideoContext? _videoContext;
		ID3D11VideoProcessorEnumerator? _enumerator;
		ID3D11VideoProcessor? _processor;
		VorticeID3D11Texture2D? _outputTexture;
		VorticeID3D11Texture2D[]? _stagingTextures;
		int _nextStagingTextureIndex;
		ID3D11VideoProcessorOutputView? _outputView;
		Signature _signature;

		internal int PendingDownloadCapacity => PendingDownloadSlots;

		internal readonly struct PendingDownload {
			public PendingDownload(VorticeID3D11Texture2D stagingTexture, Format format, long filterMs) {
				StagingTexture = stagingTexture;
				Format = format;
				FilterMs = filterMs;
			}

			public VorticeID3D11Texture2D StagingTexture { get; }
			public Format Format { get; }
			public long FilterMs { get; }
		}

		readonly struct Signature {
			public Signature(nint device, int width, int height, Format format) {
				Device = device;
				Width = width;
				Height = height;
				Format = format;
			}

			public nint Device { get; }
			public int Width { get; }
			public int Height { get; }
			public Format Format { get; }

			public bool Equals(Signature other) =>
				Device == other.Device
				&& Width == other.Width
				&& Height == other.Height
				&& Format == other.Format;
		}

		public byte[] ScaleToGray32(AVFrame sourceFrame, out D3D11GrayByteScaleTiming timing) {
			PendingDownload pending = EnqueueScaleToGray32(sourceFrame);
			byte[] data = DownloadGray32(pending, out timing);
			return data;
		}

		internal PendingDownload EnqueueScaleToGray32(AVFrame sourceFrame) {
			if (!VideoStreamDecoder.IsHardwareFrame(sourceFrame))
				throw new FFInvalidExitCodeException($"D3D11 video processor graybyte scaler requires a hardware frame, got {(AVPixelFormat)sourceFrame.format}.");
			if (sourceFrame.data[0] == null)
				throw new FFInvalidExitCodeException("D3D11 video processor graybyte scaler requires frame data[0] texture.");

			using VorticeID3D11Texture2D sourceTexture = AddRefTexture((nint)sourceFrame.data[0]);
			Texture2DDescription sourceDescription = sourceTexture.Description;
			Format format = sourceDescription.Format;
			if (!IsSupportedFormat(format))
				throw new FFInvalidExitCodeException($"D3D11 video processor graybyte scaler does not support source texture format {format}.");

			using VorticeID3D11Device sourceDevice = sourceTexture.Device;
			Signature signature = new(sourceDevice.NativePointer, (int)sourceDescription.Width, (int)sourceDescription.Height, format);
			if (_processor == null || !_signature.Equals(signature))
				BuildPipeline(sourceDevice, signature);

			if (_videoContext == null || _processor == null || _outputView == null || _stagingTextures == null)
				throw new FFInvalidExitCodeException("D3D11 video processor graybyte scaler was not initialized.");

			VorticeID3D11Texture2D stagingTexture = _stagingTextures[_nextStagingTextureIndex];
			_nextStagingTextureIndex = (_nextStagingTextureIndex + 1) % _stagingTextures.Length;

			uint sourceArraySlice = GetTextureArraySlice(sourceFrame);
			using ID3D11VideoProcessorInputView inputView = CreateInputView(sourceTexture, sourceArraySlice);

			var phaseSw = Stopwatch.StartNew();
			var sourceRect = new Vortice.RawRect(0, 0, signature.Width, signature.Height);
			var destinationRect = new Vortice.RawRect(0, 0, GraySize, GraySize);
			_videoContext.VideoProcessorSetStreamSourceRect(_processor, 0, true, sourceRect);
			_videoContext.VideoProcessorSetStreamDestRect(_processor, 0, true, destinationRect);
			_videoContext.VideoProcessorSetStreamAutoProcessingMode(_processor, 0, false);
			_videoContext.VideoProcessorSetOutputTargetRect(_processor, true, destinationRect);
			var stream = new VideoProcessorStream {
				Enable = true,
				OutputIndex = 0,
				InputFrameOrField = 0,
				InputSurface = inputView
			};
			_videoContext.VideoProcessorBlt(_processor, _outputView, 0, 1, new[] { stream }).CheckError();
			_deviceContext!.CopyResource(stagingTexture, _outputTexture);
			return new PendingDownload(stagingTexture, format, phaseSw.ElapsedMilliseconds);
		}

		internal byte[] DownloadGray32(PendingDownload pending, out D3D11GrayByteScaleTiming timing) {
			byte[] data = CopyLumaPlaneToGray(pending.StagingTexture, pending.Format, out long mapMs, out long copyMs);
			timing = new D3D11GrayByteScaleTiming(pending.FilterMs, 0, mapMs, copyMs, 1);
			return data;
		}

		void BuildPipeline(VorticeID3D11Device sourceDevice, Signature signature) {
			ReleasePipeline();
			_device = AddRefDevice(sourceDevice);
			_deviceContext = _device.ImmediateContext;
			_videoDevice = _device.QueryInterface<VorticeID3D11VideoDevice>();
			_videoContext = _deviceContext.QueryInterface<VorticeID3D11VideoContext>();

			var contentDescription = new VideoProcessorContentDescription {
				InputFrameFormat = VideoFrameFormat.Progressive,
				InputWidth = (uint)signature.Width,
				InputHeight = (uint)signature.Height,
				InputFrameRate = new Vortice.DXGI.Rational(30, 1),
				OutputWidth = GraySize,
				OutputHeight = GraySize,
				OutputFrameRate = new Vortice.DXGI.Rational(30, 1),
				Usage = VideoUsage.PlaybackNormal
			};
			_enumerator = _videoDevice.CreateVideoProcessorEnumerator(contentDescription);
			_processor = _videoDevice.CreateVideoProcessor(_enumerator, 0);

			var outputDescription = new Texture2DDescription(signature.Format, GraySize, GraySize, 1, 1, BindFlags.RenderTarget, ResourceUsage.Default, CpuAccessFlags.None);
			_outputTexture = _device.CreateTexture2D(in outputDescription);
			var stagingDescription = new Texture2DDescription(signature.Format, GraySize, GraySize, 1, 1, BindFlags.None, ResourceUsage.Staging, CpuAccessFlags.Read);
			_stagingTextures = new VorticeID3D11Texture2D[PendingDownloadSlots];
			for (int i = 0; i < _stagingTextures.Length; i++)
				_stagingTextures[i] = _device.CreateTexture2D(in stagingDescription);
			_nextStagingTextureIndex = 0;

			var outputViewDescription = new VideoProcessorOutputViewDescription {
				ViewDimension = VideoProcessorOutputViewDimension.Texture2D,
				Texture2D = new Texture2DVideoProcessorOutputView { MipSlice = 0 }
			};
			_outputView = _videoDevice.CreateVideoProcessorOutputView(_outputTexture, _enumerator, outputViewDescription);
			_signature = signature;
		}

		ID3D11VideoProcessorInputView CreateInputView(VorticeID3D11Texture2D sourceTexture, uint sourceArraySlice) {
			var inputViewDescription = new VideoProcessorInputViewDescription {
				ViewDimension = VideoProcessorInputViewDimension.Texture2D,
				Texture2D = new Texture2DVideoProcessorInputView {
					MipSlice = 0,
					ArraySlice = sourceArraySlice
				}
			};
			return _videoDevice!.CreateVideoProcessorInputView(sourceTexture, _enumerator!, inputViewDescription);
		}

		byte[] CopyLumaPlaneToGray(VorticeID3D11Texture2D stagingTexture, Format format, out long mapMs, out long copyMs) {
			var phaseSw = Stopwatch.StartNew();
			var mapped = _deviceContext!.Map(stagingTexture, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
			mapMs = phaseSw.ElapsedMilliseconds;
			try {
				phaseSw.Restart();
				byte[] output = new byte[GraySize * GraySize];
				if (format == Format.NV12) {
					for (int y = 0; y < GraySize; y++) {
						IntPtr row = IntPtr.Add(mapped.DataPointer, y * (int)mapped.RowPitch);
						System.Runtime.InteropServices.Marshal.Copy(row, output, y * GraySize, GraySize);
					}
				}
				else {
					for (int y = 0; y < GraySize; y++) {
						byte* row = (byte*)mapped.DataPointer + (y * mapped.RowPitch);
						for (int x = 0; x < GraySize; x++)
							output[(y * GraySize) + x] = row[(x * 2) + 1];
					}
				}
				copyMs = phaseSw.ElapsedMilliseconds;
				return output;
			}
			finally {
				_deviceContext.Unmap(stagingTexture, 0);
			}
		}

		static bool IsSupportedFormat(Format format) => format == Format.NV12 || format == Format.P010;

		static uint GetTextureArraySlice(AVFrame frame) => (uint)(nint)frame.data[1];

		static VorticeID3D11Texture2D AddRefTexture(nint texturePointer) {
			var texture = new VorticeID3D11Texture2D(texturePointer);
			texture.AddRef();
			return texture;
		}

		static VorticeID3D11Device AddRefDevice(VorticeID3D11Device device) {
			var deviceReference = new VorticeID3D11Device(device.NativePointer);
			deviceReference.AddRef();
			return deviceReference;
		}

		void ReleasePipeline() {
			_outputView?.Dispose();
			if (_stagingTextures != null) {
				foreach (VorticeID3D11Texture2D stagingTexture in _stagingTextures)
					stagingTexture.Dispose();
			}
			_outputTexture?.Dispose();
			_processor?.Dispose();
			_enumerator?.Dispose();
			_videoContext?.Dispose();
			_videoDevice?.Dispose();
			_deviceContext?.Dispose();
			_device?.Dispose();
			_outputView = null;
			_stagingTextures = null;
			_outputTexture = null;
			_processor = null;
			_enumerator = null;
			_videoContext = null;
			_videoDevice = null;
			_deviceContext = null;
			_device = null;
			_signature = default;
			_nextStagingTextureIndex = 0;
		}

		public void Dispose() => ReleasePipeline();
	}
}
