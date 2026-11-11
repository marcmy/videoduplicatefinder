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
	/// <summary>
	/// Scales D3D11VA-decoded frames on the GPU with the D3D11 video processor and reads
	/// back only the tiny results: the 32×32 luma sample used for gray/pHash matching and,
	/// optionally, the 224×224 RGB frame fed to the AI embedder. This replaces downloading
	/// the full decoded surface (12 MB for a 4K NV12 frame) and scaling it on the CPU.
	///
	/// Colour handling matches the CPU swscale path the rest of VDF uses (swscale with no
	/// explicit colourspace details: BT.601 matrix, limited-range input, full-range gray
	/// and RGB output) so GPU- and CPU-produced samples compare cleanly against each other.
	///
	/// The video-processor pipeline is built once per (device, frame size, surface format)
	/// and reused for every sample of a file. Every immediate-context call runs under the
	/// FFmpeg D3D11VA device lock so it never interleaves with the decoder's submissions,
	/// and the GPU wait polls an event query outside that lock instead of blocking in Map.
	/// </summary>
	sealed unsafe class D3D11VideoProcessorGrayByteScaler : IDisposable {
		internal const int GraySize = 32;
		internal const int RgbSize = 224;
		const int PendingDownloadSlots = 1;
		static readonly TimeSpan GpuWaitTimeout = TimeSpan.FromSeconds(10);

		Pipeline? _pipeline;
		bool _pendingInFlight;

		internal int PendingDownloadCapacity => PendingDownloadSlots;

		internal readonly struct PendingDownload {
			internal PendingDownload(Pipeline pipeline, AVBufferRef* deviceRef, bool gray, bool rgb, long filterMs) {
				Pipeline = pipeline;
				DeviceRef = (nint)deviceRef;
				Gray = gray;
				Rgb = rgb;
				FilterMs = filterMs;
			}

			internal Pipeline Pipeline { get; }
			internal nint DeviceRef { get; }
			internal bool Gray { get; }
			internal bool Rgb { get; }
			public long FilterMs { get; }
		}

		internal readonly struct Signature : IEquatable<Signature> {
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

			public override bool Equals(object? obj) => obj is Signature other && Equals(other);
			public override int GetHashCode() => HashCode.Combine(Device, Width, Height, Format);
		}

		public byte[] ScaleToGray32(AVFrame sourceFrame, out D3D11GrayByteScaleTiming timing) {
			PendingDownload pending = EnqueueScaleToGray32(sourceFrame);
			return DownloadGray32(pending, out timing);
		}

		internal PendingDownload EnqueueScaleToGray32(AVFrame sourceFrame) =>
			Enqueue(sourceFrame, gray: true, rgb: false);

		internal byte[] DownloadGray32(PendingDownload pending, out D3D11GrayByteScaleTiming timing) {
			Download(pending, out byte[]? gray, out _, out timing);
			return gray!;
		}

		/// <summary>
		/// Scales one decoded hardware frame to the requested outputs. Throws
		/// <see cref="NotSupportedException"/> when the GPU cannot produce RGB output for
		/// this surface format so the caller can fall back to the CPU path for the file.
		/// </summary>
		internal void ScaleToGrayAndRgb(AVFrame sourceFrame, bool gray, bool rgb, out byte[]? gray32, out byte[]? rgb224, out D3D11GrayByteScaleTiming timing) {
			PendingDownload pending = Enqueue(sourceFrame, gray, rgb);
			Download(pending, out gray32, out rgb224, out timing);
		}

		PendingDownload Enqueue(AVFrame sourceFrame, bool gray, bool rgb) {
			if (!gray && !rgb)
				throw new ArgumentException("At least one output must be requested.");
			if (_pendingInFlight)
				throw new InvalidOperationException("D3D11 video processor scaler already has a pending download.");
			if (!VideoStreamDecoder.IsHardwareFrame(sourceFrame))
				throw new FFInvalidExitCodeException($"D3D11 video processor scaler requires a hardware frame, got {(AVPixelFormat)sourceFrame.format}.");
			if (sourceFrame.data[0] == null)
				throw new FFInvalidExitCodeException("D3D11 video processor scaler requires frame data[0] texture.");
			AVBufferRef* deviceRef = GetFrameDeviceRef(sourceFrame);
			if (deviceRef == null)
				throw new FFInvalidExitCodeException("D3D11 video processor scaler requires a D3D11VA hw_frames_ctx.");

			using VorticeID3D11Texture2D sourceTexture = AddRefTexture((nint)sourceFrame.data[0]);
			Texture2DDescription sourceDescription = sourceTexture.Description;
			Format format = sourceDescription.Format;
			if (format != Format.NV12 && format != Format.P010)
				throw new FFInvalidExitCodeException($"D3D11 video processor scaler does not support source texture format {format}.");

			// The decoder surface is padded to the codec's alignment (1920x1080 decodes into a
			// 1920x1088 texture); scale only the visible picture, as swscale does.
			int width = sourceFrame.width > 0 ? Math.Min(sourceFrame.width, (int)sourceDescription.Width) : (int)sourceDescription.Width;
			int height = sourceFrame.height > 0 ? Math.Min(sourceFrame.height, (int)sourceDescription.Height) : (int)sourceDescription.Height;

			using VorticeID3D11Device sourceDevice = sourceTexture.Device;
			Signature signature = new(sourceDevice.NativePointer, width, height, format);
			if (_pipeline == null || !_pipeline.Signature.Equals(signature)) {
				_pipeline?.Dispose();
				_pipeline = null;
				_pipeline = new Pipeline(sourceDevice, signature);
			}
			Pipeline pipeline = _pipeline;
			if (rgb && pipeline.RgbFormat == Format.Unknown)
				throw new NotSupportedException($"D3D11 video processor cannot output RGB for {format} surfaces on this GPU.");

			uint sourceArraySlice = (uint)(nint)sourceFrame.data[1];
			var phaseSw = Stopwatch.StartNew();
			using (D3D11DeviceLock.ForDevice(deviceRef)) {
				using ID3D11VideoProcessorInputView inputView = pipeline.CreateInputView(sourceTexture, sourceArraySlice);
				if (gray)
					pipeline.Blt(inputView, pipeline.GrayView!, GraySize, outputRgb: false);
				if (rgb)
					pipeline.Blt(inputView, pipeline.RgbView!, RgbSize, outputRgb: true);
				if (gray)
					pipeline.DeviceContext.CopyResource(pipeline.GrayStaging!, pipeline.GrayTexture!);
				if (rgb)
					pipeline.DeviceContext.CopyResource(pipeline.RgbStaging!, pipeline.RgbTexture!);
				pipeline.DeviceContext.End(pipeline.Query);
				pipeline.DeviceContext.Flush();
			}
			AVBufferRef* heldDeviceRef = ffmpeg.av_buffer_ref(deviceRef);
			if (heldDeviceRef == null)
				throw new FFInvalidExitCodeException("av_buffer_ref failed for the D3D11VA device.");
			_pendingInFlight = true;
			return new PendingDownload(pipeline, heldDeviceRef, gray, rgb, phaseSw.ElapsedMilliseconds);
		}

		void Download(PendingDownload pending, out byte[]? gray32, out byte[]? rgb224, out D3D11GrayByteScaleTiming timing) {
			AVBufferRef* deviceRef = (AVBufferRef*)pending.DeviceRef;
			try {
				Pipeline pipeline = pending.Pipeline;
				var phaseSw = Stopwatch.StartNew();
				WaitForGpu(pipeline, deviceRef);
				long mapMs = phaseSw.ElapsedMilliseconds;
				phaseSw.Restart();
				using (D3D11DeviceLock.ForDevice(deviceRef)) {
					gray32 = pending.Gray ? pipeline.ReadGray32() : null;
					rgb224 = pending.Rgb ? pipeline.ReadRgb224() : null;
				}
				timing = new D3D11GrayByteScaleTiming(pending.FilterMs, 0, mapMs, phaseSw.ElapsedMilliseconds, 1);
			}
			finally {
				ffmpeg.av_buffer_unref(&deviceRef);
				_pendingInFlight = false;
			}
		}

		static void WaitForGpu(Pipeline pipeline, AVBufferRef* deviceRef) {
			// Poll the event query without holding the device lock (and without a blocking
			// Map, which would hold the runtime's context mutex) so decoders sharing the
			// device keep submitting while this sample finishes on the GPU.
			long deadline = Stopwatch.GetTimestamp() + (long)(GpuWaitTimeout.TotalSeconds * Stopwatch.Frequency);
			int spins = 0;
			while (true) {
				bool done;
				using (D3D11DeviceLock.ForDevice(deviceRef))
					done = pipeline.IsQueryComplete();
				if (done)
					return;
				if (Stopwatch.GetTimestamp() > deadline)
					throw new FFInvalidExitCodeException($"D3D11 video processor scaler timed out waiting {GpuWaitTimeout.TotalSeconds:0}s for the GPU.");
				if (++spins < 16)
					Thread.SpinWait(64);
				else if (spins < 64)
					Thread.Yield();
				else
					Thread.Sleep(1);
			}
		}

		static AVBufferRef* GetFrameDeviceRef(AVFrame frame) {
			if (frame.hw_frames_ctx == null || frame.hw_frames_ctx->data == null)
				return null;
			AVHWFramesContext* framesContext = (AVHWFramesContext*)frame.hw_frames_ctx->data;
			if (framesContext->device_ctx == null || framesContext->device_ctx->type != AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA)
				return null;
			return framesContext->device_ref;
		}

		static VorticeID3D11Texture2D AddRefTexture(nint texturePointer) {
			var texture = new VorticeID3D11Texture2D(texturePointer);
			texture.AddRef();
			return texture;
		}

		public void Dispose() {
			_pipeline?.Dispose();
			_pipeline = null;
			_pendingInFlight = false;
		}

		/// <summary>One video processor plus its output/staging textures for one input signature.</summary>
		internal sealed class Pipeline : IDisposable {
			readonly VorticeID3D11Device _device;
			readonly VorticeID3D11VideoDevice _videoDevice;
			readonly VorticeID3D11VideoContext _videoContext;
			readonly ID3D11VideoProcessorEnumerator _enumerator;
			readonly ID3D11VideoProcessor _processor;

			internal Signature Signature { get; }
			internal VorticeID3D11DeviceContext DeviceContext { get; }
			internal ID3D11Query Query { get; }
			internal Format GrayFormat { get; }
			internal VorticeID3D11Texture2D? GrayTexture { get; }
			internal VorticeID3D11Texture2D? GrayStaging { get; }
			internal ID3D11VideoProcessorOutputView? GrayView { get; }
			internal Format RgbFormat { get; }
			internal VorticeID3D11Texture2D? RgbTexture { get; }
			internal VorticeID3D11Texture2D? RgbStaging { get; }
			internal ID3D11VideoProcessorOutputView? RgbView { get; }

			internal Pipeline(VorticeID3D11Device device, Signature signature) {
				Signature = signature;
				_device = new VorticeID3D11Device(device.NativePointer);
				_device.AddRef();
				try {
					DeviceContext = _device.ImmediateContext;
					_videoDevice = _device.QueryInterface<VorticeID3D11VideoDevice>();
					_videoContext = DeviceContext.QueryInterface<VorticeID3D11VideoContext>();

					var contentDescription = new VideoProcessorContentDescription {
						InputFrameFormat = VideoFrameFormat.Progressive,
						InputWidth = (uint)signature.Width,
						InputHeight = (uint)signature.Height,
						InputFrameRate = new Vortice.DXGI.Rational(30, 1),
						OutputWidth = RgbSize,
						OutputHeight = RgbSize,
						OutputFrameRate = new Vortice.DXGI.Rational(30, 1),
						Usage = VideoUsage.OptimalQuality
					};
					_enumerator = _videoDevice.CreateVideoProcessorEnumerator(contentDescription);
					_processor = _videoDevice.CreateVideoProcessor(_enumerator, 0);
					Query = _device.CreateQuery(new QueryDescription(QueryType.Event, QueryFlags.None));

					// Gray: NV12 output keeps luma as-is (8-bit even for P010 input); fall back
					// to the input format when the driver cannot convert.
					GrayFormat = SupportsOutput(Format.NV12) ? Format.NV12 : signature.Format;
					(GrayTexture, GrayStaging, GrayView) = CreateOutput(GrayFormat, GraySize);

					RgbFormat = SupportsOutput(Format.R8G8B8A8_UNorm)
						? Format.R8G8B8A8_UNorm
						: SupportsOutput(Format.B8G8R8A8_UNorm) ? Format.B8G8R8A8_UNorm : Format.Unknown;
					if (RgbFormat != Format.Unknown)
						(RgbTexture, RgbStaging, RgbView) = CreateOutput(RgbFormat, RgbSize);

					// swscale defaults: BT.601 matrix, limited-range (16-235) YCbCr input.
					_videoContext.VideoProcessorSetStreamColorSpace(_processor, 0, new VideoProcessorColorSpace {
						Usage = 0,
						RGB_Range = 0,
						YCbCr_Matrix = 0,
						YCbCr_xvYCC = 0,
						Nominal_Range = 1
					});
					_videoContext.VideoProcessorSetStreamAutoProcessingMode(_processor, 0, false);
					_videoContext.VideoProcessorSetStreamFrameFormat(_processor, 0, VideoFrameFormat.Progressive);
				}
				catch {
					Dispose();
					throw;
				}
			}

			bool SupportsOutput(Format format) {
				try {
					return (_enumerator.CheckVideoProcessorFormat(format) & VideoProcessorFormatSupport.Output) != 0;
				}
				catch {
					return false;
				}
			}

			(VorticeID3D11Texture2D, VorticeID3D11Texture2D, ID3D11VideoProcessorOutputView) CreateOutput(Format format, int size) {
				var outputDescription = new Texture2DDescription(format, (uint)size, (uint)size, 1, 1, BindFlags.RenderTarget, ResourceUsage.Default, CpuAccessFlags.None);
				VorticeID3D11Texture2D texture = _device.CreateTexture2D(in outputDescription);
				var stagingDescription = new Texture2DDescription(format, (uint)size, (uint)size, 1, 1, BindFlags.None, ResourceUsage.Staging, CpuAccessFlags.Read);
				VorticeID3D11Texture2D staging = _device.CreateTexture2D(in stagingDescription);
				var viewDescription = new VideoProcessorOutputViewDescription {
					ViewDimension = VideoProcessorOutputViewDimension.Texture2D,
					Texture2D = new Texture2DVideoProcessorOutputView { MipSlice = 0 }
				};
				ID3D11VideoProcessorOutputView view = _videoDevice.CreateVideoProcessorOutputView(texture, _enumerator, viewDescription);
				return (texture, staging, view);
			}

			internal ID3D11VideoProcessorInputView CreateInputView(VorticeID3D11Texture2D sourceTexture, uint sourceArraySlice) {
				var inputViewDescription = new VideoProcessorInputViewDescription {
					ViewDimension = VideoProcessorInputViewDimension.Texture2D,
					Texture2D = new Texture2DVideoProcessorInputView {
						MipSlice = 0,
						ArraySlice = sourceArraySlice
					}
				};
				return _videoDevice.CreateVideoProcessorInputView(sourceTexture, _enumerator, inputViewDescription);
			}

			internal void Blt(ID3D11VideoProcessorInputView inputView, ID3D11VideoProcessorOutputView outputView, int outputSize, bool outputRgb) {
				var sourceRect = new Vortice.RawRect(0, 0, Signature.Width, Signature.Height);
				var destinationRect = new Vortice.RawRect(0, 0, outputSize, outputSize);
				_videoContext.VideoProcessorSetStreamSourceRect(_processor, 0, true, sourceRect);
				_videoContext.VideoProcessorSetStreamDestRect(_processor, 0, true, destinationRect);
				_videoContext.VideoProcessorSetOutputTargetRect(_processor, true, destinationRect);
				// Full-range RGB for the AI frame; for the gray (YCbCr) output keep the input's
				// BT.601 limited-range luma untouched — it is range-expanded on readback.
				_videoContext.VideoProcessorSetOutputColorSpace(_processor, new VideoProcessorColorSpace {
					Usage = 0,
					RGB_Range = 0,
					YCbCr_Matrix = 0,
					YCbCr_xvYCC = 0,
					Nominal_Range = outputRgb ? 2u : 1u
				});
				var stream = new VideoProcessorStream {
					Enable = true,
					OutputIndex = 0,
					InputFrameOrField = 0,
					InputSurface = inputView
				};
				_videoContext.VideoProcessorBlt(_processor, outputView, 0, 1, new[] { stream }).CheckError();
			}

			internal bool IsQueryComplete() {
				// Event queries return a BOOL; S_FALSE (non-zero code) means still in flight.
				int signaled = 0;
				SharpGen.Runtime.Result result = DeviceContext.GetData(Query, (IntPtr)(&signaled), sizeof(int), AsyncGetDataFlags.DoNotFlush);
				result.CheckError();
				return result.Code == 0 && signaled != 0;
			}

			internal byte[] ReadGray32() {
				MappedSubresource mapped = DeviceContext.Map(GrayStaging!, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
				try {
					if (mapped.DataPointer == IntPtr.Zero)
						throw new FFInvalidExitCodeException("D3D11 scaler mapped gray staging texture with a null data pointer.");
					int bytesPerSample = GrayFormat == Format.P010 ? 2 : 1;
					if (mapped.RowPitch < GraySize * bytesPerSample)
						throw new FFInvalidExitCodeException($"D3D11 scaler mapped gray staging texture with invalid row pitch {mapped.RowPitch} for {GrayFormat}.");
					byte[] output = new byte[GraySize * GraySize];
					for (int y = 0; y < GraySize; y++) {
						byte* row = (byte*)mapped.DataPointer + (y * mapped.RowPitch);
						for (int x = 0; x < GraySize; x++) {
							// P010 keeps the 10-bit value in the upper bits; the high byte is the 8-bit luma.
							int luma = bytesPerSample == 2 ? row[(x * 2) + 1] : row[x];
							output[(y * GraySize) + x] = ExpandLimitedLuma(luma);
						}
					}
					return output;
				}
				finally {
					DeviceContext.Unmap(GrayStaging!, 0);
				}
			}

			internal byte[] ReadRgb224() {
				MappedSubresource mapped = DeviceContext.Map(RgbStaging!, 0, MapMode.Read, Vortice.Direct3D11.MapFlags.None);
				try {
					if (mapped.DataPointer == IntPtr.Zero)
						throw new FFInvalidExitCodeException("D3D11 scaler mapped RGB staging texture with a null data pointer.");
					if (mapped.RowPitch < RgbSize * 4)
						throw new FFInvalidExitCodeException($"D3D11 scaler mapped RGB staging texture with invalid row pitch {mapped.RowPitch}.");
					bool bgra = RgbFormat == Format.B8G8R8A8_UNorm;
					byte[] output = new byte[RgbSize * RgbSize * 3];
					int o = 0;
					for (int y = 0; y < RgbSize; y++) {
						byte* row = (byte*)mapped.DataPointer + (y * mapped.RowPitch);
						for (int x = 0; x < RgbSize; x++) {
							byte* pixel = row + (x * 4);
							if (bgra) {
								output[o++] = pixel[2];
								output[o++] = pixel[1];
								output[o++] = pixel[0];
							}
							else {
								output[o++] = pixel[0];
								output[o++] = pixel[1];
								output[o++] = pixel[2];
							}
						}
					}
					return output;
				}
				finally {
					DeviceContext.Unmap(RgbStaging!, 0);
				}
			}

			/// <summary>Limited-range (16-235) luma to full-range gray, as swscale's GRAY8 output does.</summary>
			internal static byte ExpandLimitedLuma(int luma) {
				int value = ((luma - 16) * 255 + 109) / 219;
				return (byte)Math.Clamp(value, 0, 255);
			}

			public void Dispose() {
				RgbView?.Dispose();
				RgbStaging?.Dispose();
				RgbTexture?.Dispose();
				GrayView?.Dispose();
				GrayStaging?.Dispose();
				GrayTexture?.Dispose();
				Query?.Dispose();
				_processor?.Dispose();
				_enumerator?.Dispose();
				_videoContext?.Dispose();
				_videoDevice?.Dispose();
				DeviceContext?.Dispose();
				_device.Dispose();
			}
		}
	}
}
