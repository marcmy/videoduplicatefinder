using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
		static string _FFmpegPath = string.Empty;
		// Re-probes when unresolved (or the binary vanished): a once-only static cache made
		// an FFmpeg installed/downloaded while the app was running invisible until restart,
		// so the GUI kept offering the download forever (issue #788).
		public static string FFmpegPath {
			get {
				if (_FFmpegPath.Length == 0 || !File.Exists(_FFmpegPath))
					_FFmpegPath = FFToolsUtils.GetPath(FFToolsUtils.FFTool.FFmpeg) ?? string.Empty;
				return _FFmpegPath;
			}
		}
		const int TimeoutDuration = 15_000;
		const string ForceNativeGrayByteCpuEnvVar = "VDF_FORCE_NATIVE_GRAYBYTE_CPU";
		const string DisableNativeGrayByteGpuScaleEnvVar = "VDF_DISABLE_NATIVE_GRAYBYTE_GPU_SCALE";
		const string EnableNativeGrayByteGpuScaleEnvVar = "VDF_ENABLE_NATIVE_GRAYBYTE_GPU_SCALE";
		const string DisableNativeGrayByteD3D11AdaptiveEnvVar = "VDF_DISABLE_NATIVE_GRAYBYTE_D3D11_ADAPTIVE";
		const string EnableNativeGrayByteD3D11CpuProbeEnvVar = "VDF_ENABLE_NATIVE_GRAYBYTE_D3D11_CPU_PROBE";
		const string NativeGrayByteD3D11MaxConcurrencyEnvVar = "VDF_NATIVE_GRAYBYTE_D3D11_MAX_CONCURRENCY";
		const int NativeGrayByteD3D11AutoInitialConcurrency = 2;
		const int NativeGrayByteD3D11AutoMaxConcurrency = 8;
		const int NativeGrayByteD3D11AutoTuneObservationWindow = 12;
		const long NativeGrayByteD3D11AutoQueueHighMs = 750;
		const long NativeGrayByteD3D11AutoDecodeHighMs = 1500;
		const int D3D11GrayByteAdaptiveMinimumObservations = 3;
		const int MaxCapturedFfmpegErrorLines = 80;
		const long D3D11GrayByteAdaptiveSlowPerSampleMs = 140;
		static readonly object D3D11GrayByteAdaptiveStateLock = new();
		static readonly Dictionary<string, D3D11GrayByteAdaptiveStats> D3D11GrayByteAdaptiveStatsByFamily = new(StringComparer.OrdinalIgnoreCase);
		static readonly object HardwareDecodeCodecStateLock = new();
		static readonly Dictionary<string, HardwareDecodeCodecStats> HardwareDecodeCodecStatsByModeAndCodec = new(StringComparer.OrdinalIgnoreCase);
		static readonly object D3D11GrayByteConcurrencyLock = new();
		static int D3D11GrayByteCurrentConcurrencyLimit = NativeGrayByteD3D11AutoInitialConcurrency;
		static int D3D11GrayByteActiveConcurrency;
		static int D3D11GrayByteTuningObservations;
		static long D3D11GrayByteTuningQueueMs;
		static long D3D11GrayByteTuningDecodeMs;
		static int D3D11GrayByteTuningDecodeSpikeObservations;
		public static FFHardwareAccelerationMode HardwareAccelerationMode;
		public static string CustomFFArguments = string.Empty;
		static bool _useNativeBinding;
		public static bool UseNativeBinding {
			get => _useNativeBinding;
			set {
				_useNativeBinding = value;
				ResetNativeBindingHealth();
			}
		}
		public static int ScanMaxDegreeOfParallelism = -1;
		const int DefaultJpegQuality = 90;
		static int NativeDisabledForSession;
		static int VulkanNativeWarningLogged;

		const double SequentialBatchMaxSpanSeconds = 2d;

		sealed class NativeGrayByteTiming {
			public long QueueMs;
			public long OpenMs;
			public long SeekMs;
			public long DecodeMs;
			public long TransferMs;
			public int HardwareTransfers;
			public int FullFrameTransfers;
			public long FilterMs;
			public long ConvertMs;
			public long TinyConvertMs;
			public long MapMs;
			public long CopyMs;
			public int TinyDownloads;
			public int SampledFrames;
		}

		sealed class D3D11GrayByteAdaptiveStats {
			public int Observations;
			public long TotalMs;
			public int Samples;
			public int SlowObservations;
			public bool CpuProbePending;
			public bool CpuProbeCompleted;
			public bool Bypass;
		}

		sealed class HardwareDecodeCodecStats {
			public bool Bypass;
			public string Reason = string.Empty;
		}

		internal sealed class FfmpegErrorAccumulator {
			readonly int maxLines;
			readonly StringBuilder builder = new();
			string lastLine = string.Empty;
			bool lastLineCaptured;
			int capturedLines;
			int omittedLines;
			int repeatCount;

			public FfmpegErrorAccumulator(int maxLines = MaxCapturedFfmpegErrorLines) {
				this.maxLines = Math.Max(1, maxLines);
			}

			public void AppendLine(string? line) {
				if (string.IsNullOrEmpty(line))
					return;
				string normalized = line.Replace("\r\n", "\n").Replace('\r', '\n');
				foreach (string part in normalized.Split('\n')) {
					if (part.Length > 0)
						AppendSingleLine(part);
				}
			}

			void AppendSingleLine(string line) {
				if (line == lastLine) {
					if (lastLineCaptured)
						repeatCount++;
					else
						omittedLines++;
					return;
				}

				FlushRepeat();
				lastLine = line;
				if (capturedLines < maxLines) {
					if (builder.Length > 0)
						builder.Append(Environment.NewLine);
					builder.Append(line);
					capturedLines++;
					lastLineCaptured = true;
				}
				else {
					omittedLines++;
					lastLineCaptured = false;
				}
			}

			void FlushRepeat() {
				if (repeatCount <= 0)
					return;
				builder.Append($" (repeated {repeatCount} more time{(repeatCount == 1 ? string.Empty : "s")})");
				repeatCount = 0;
			}

			public override string ToString() {
				FlushRepeat();
				if (omittedLines > 0) {
					if (builder.Length > 0)
						builder.Append(Environment.NewLine);
					builder.Append($"... omitted {omittedLines} additional FFmpeg stderr line(s)");
					omittedLines = 0;
				}
				return builder.ToString();
			}
		}

		sealed class D3D11SoftwareFrameFallbackException : Exception {
			public D3D11SoftwareFrameFallbackException(AVPixelFormat pixelFormat)
				: base($"D3D11 graybyte decode produced software frames ({pixelFormat}); retrying this file with native CPU decode.") {
			}
		}

		readonly struct GrayByteRequest {
			public GrayByteRequest(double index, TimeSpan position) {
				Index = index;
				Position = position;
			}

			public double Index { get; }
			public TimeSpan Position { get; }
		}

		readonly struct GrayByteResult {
			public GrayByteResult(double index, byte[] data, ulong pHash, bool tooDark) {
				Index = index;
				Data = data;
				PHash = pHash;
				TooDark = tooDark;
			}

			public double Index { get; }
			public byte[] Data { get; }
			public ulong PHash { get; }
			public bool TooDark { get; }
		}

		sealed class GrayByteRequestCluster {
			public GrayByteRequestCluster(GrayByteRequest firstRequest) {
				Requests.Add(firstRequest);
			}

			public List<GrayByteRequest> Requests { get; } = new();
			public TimeSpan Start => Requests[0].Position;
			public TimeSpan End => Requests[^1].Position;
			public TimeSpan Span => End - Start;
		}

		sealed class PendingD3D11GrayByteResult {
			public PendingD3D11GrayByteResult(GrayByteRequest request, D3D11VideoProcessorGrayByteScaler.PendingDownload pendingDownload) {
				Request = request;
				PendingDownload = pendingDownload;
			}

			public GrayByteRequest Request { get; }
			public D3D11VideoProcessorGrayByteScaler.PendingDownload PendingDownload { get; }
		}

		sealed class SemaphoreLease : IDisposable {
			Action? release;

			public SemaphoreLease(Action release) => this.release = release;

			public void Dispose() {
				Action? action = Interlocked.Exchange(ref release, null);
				action?.Invoke();
			}
		}

		static GrayByteResult CreateGrayByteResult(GrayByteRequest request, byte[] data) =>
			new(request.Index, data, pHash.PerceptualHash.ComputePHashFromGray32x32(data), !GrayBytesUtils.VerifyGrayScaleValues(data));

		static void CommitGrayByteResults(FileEntry videoFile, IReadOnlyList<GrayByteResult> results, ref int tooDarkCounter) {
			foreach (GrayByteResult result in results) {
				videoFile.grayBytes[result.Index] = result.Data;
				videoFile.PHashes[result.Index] = result.PHash;
				if (result.TooDark) tooDarkCounter++;
			}
		}

		static unsafe byte[] ExtractGrayFrameFromFrame(
			AVFrame convertedFrame,
			int sideLength) {
			int width = convertedFrame.width;
			int height = convertedFrame.height;
			if (width != sideLength || height != sideLength) throw new Exception($"Unexpected size {width}x{height}, expected {sideLength}.");
			if (convertedFrame.data[0] == null) throw new Exception("Converted frame has no data[0] (null).");
			if (convertedFrame.linesize[0] < width) throw new Exception($"Invalid linesize ({convertedFrame.linesize[0]}) for width {width}.");
			int srcStride = convertedFrame.linesize[0];
			byte[] outBuf = new byte[width * height];
			fixed (byte* destPtr = outBuf) {
				byte* sourcePtr = convertedFrame.data[0];
				for (int y = 0; y < height; y++)
					Buffer.MemoryCopy(sourcePtr + (y * srcStride), destPtr + (y * width), width, width);
			}
			return outBuf;
		}

		static unsafe byte[] ExtractGray32FromFrame(AVFrame convertedFrame) =>
			ExtractGrayFrameFromFrame(convertedFrame, 32);

		static List<GrayByteRequest> GetMissingGrayByteRequests(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds) {
			List<GrayByteRequest> requests = new();
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (!videoFile.grayBytes.TryGetValue(position, out byte[]? bytes) || bytes == null)
					requests.Add(new GrayByteRequest(position, TimeSpan.FromSeconds(position)));
			}
			requests.Sort((left, right) => left.Position.CompareTo(right.Position));
			return requests;
		}

		static bool IsValidPixelFormat(AVPixelFormat pixelFormat) =>
			pixelFormat >= 0 && pixelFormat < AVPixelFormat.AV_PIX_FMT_NB;

		static AVPixelFormat GetConvertiblePixelFormat(VideoStreamDecoder vsd, AVFrame frame) {
			AVPixelFormat framePixelFormat = (AVPixelFormat)frame.format;
			if (IsValidPixelFormat(framePixelFormat) && !VideoStreamDecoder.IsHardwareFrame(frame))
				return framePixelFormat;

			if (IsValidPixelFormat(vsd.PixelFormat))
				return vsd.PixelFormat;

			return framePixelFormat;
		}

		static unsafe byte[] ExtractGrayBytesFromFrame(VideoStreamDecoder vsd, AVFrame srcFrame, ref VideoFrameConverter? converter, ref Size converterSourceSize, ref AVPixelFormat converterSourcePixelFormat, out long convertMs, out long copyMs, int sideLength = 32) {
			Size sourceSize = new(srcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width, srcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);
			if (sourceSize.Width <= 0 || sourceSize.Height <= 0)
				throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");

			AVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);
			if (!IsValidPixelFormat(srcPixFmt))
				throw new Exception($"Invalid source pixel format {srcPixFmt}");

			if (converter == null || sourceSize != converterSourceSize || srcPixFmt != converterSourcePixelFormat) {
				converter?.Dispose();
				converter = new VideoFrameConverter(sourceSize, srcPixFmt, new Size(sideLength, sideLength), AVPixelFormat.AV_PIX_FMT_GRAY8, VideoFrameConverter.ScaleQuality.FastBilinear, false);
				converterSourceSize = sourceSize;
				converterSourcePixelFormat = srcPixFmt;
			}

			var phaseSw = Stopwatch.StartNew();
			phaseSw.Restart();
			AVFrame convertedFrame = converter.Convert(srcFrame);
			convertMs = phaseSw.ElapsedMilliseconds;

			phaseSw.Restart();
			byte[] outBuf = ExtractGrayFrameFromFrame(convertedFrame, sideLength);
			copyMs = phaseSw.ElapsedMilliseconds;

			return outBuf;
		}

		static unsafe byte[] ExtractJpegFromFrame(VideoStreamDecoder vsd, AVFrame srcFrame, int maxWidth, int jpegQuality, ref VideoFrameConverter? converter, ref Size converterSourceSize, ref AVPixelFormat converterSourcePixelFormat, out long convertMs, out long copyMs) {
			Size sourceSize = new(srcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width, srcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);
			if (sourceSize.Width <= 0 || sourceSize.Height <= 0)
				throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");

			AVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);
			if (!IsValidPixelFormat(srcPixFmt))
				throw new Exception($"Invalid source pixel format {srcPixFmt}");

			AVRational sampleAspectRatio = srcFrame.sample_aspect_ratio;
			if (sampleAspectRatio.num <= 0 || sampleAspectRatio.den <= 0)
				sampleAspectRatio = vsd.StreamSampleAspectRatio;
			Size displaySize = GetDisplaySizeForSampleAspectRatio(
				sourceSize, sampleAspectRatio.num, sampleAspectRatio.den);
			Size destinationSize = maxWidth == 0
				? displaySize
				: ScaleToMaxWidth(displaySize, maxWidth > 0 ? maxWidth : 100);
			if (converter == null || sourceSize != converterSourceSize || srcPixFmt != converterSourcePixelFormat) {
				converter?.Dispose();
				converter = new VideoFrameConverter(sourceSize, srcPixFmt, destinationSize, AVPixelFormat.AV_PIX_FMT_YUVJ420P, VideoFrameConverter.ScaleQuality.Bicubic, false);
				converterSourceSize = sourceSize;
				converterSourcePixelFormat = srcPixFmt;
			}

			var phaseSw = Stopwatch.StartNew();
			AVFrame convertedFrame = converter.Convert(srcFrame);
			convertMs = phaseSw.ElapsedMilliseconds;

			phaseSw.Restart();
			byte[] jpeg = JpegFrameEncoder.Encode(convertedFrame, jpegQuality > 0 ? jpegQuality : DefaultJpegQuality);
			copyMs = phaseSw.ElapsedMilliseconds;
			return jpeg;
		}

		static unsafe byte[] ExtractGrayBytesWithDecoder(VideoStreamDecoder vsd, string filePath, TimeSpan position, ref VideoFrameConverter? converter, ref Size converterSourceSize, ref AVPixelFormat converterSourcePixelFormat, NativeGrayByteTiming timing) {
			if (!vsd.TryDecodeFrame(out var srcFrame, position, out DecodedFrameTiming decodeTiming))
				throw new Exception($"TryDecodeFrame failed at pos={position} for '{filePath}'. size={vsd.FrameSize.Width}x{vsd.FrameSize.Height}");

			timing.SeekMs += decodeTiming.SeekMs;
			timing.DecodeMs += decodeTiming.DecodeMs;
			timing.TransferMs += decodeTiming.TransferMs;
			timing.HardwareTransfers += decodeTiming.HardwareTransfers;
			timing.FullFrameTransfers += decodeTiming.HardwareTransfers;
			byte[] data = ExtractGrayBytesFromFrame(vsd, srcFrame, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, out long convertMs, out long copyMs);
			timing.ConvertMs += convertMs;
			timing.CopyMs += copyMs;
			return data;
		}

		static unsafe byte[] ExtractGrayBytesWithD3D11GpuScale(VideoStreamDecoder vsd, D3D11VideoProcessorGrayByteScaler scaler, string filePath, TimeSpan position, NativeGrayByteTiming timing) {
			if (!vsd.TryDecodeFrame(out var srcFrame, position, out DecodedFrameTiming decodeTiming, FrameTransferMode.KeepHardwareFrame))
				throw new Exception($"TryDecodeFrame failed at pos={position} for '{filePath}'. size={vsd.FrameSize.Width}x{vsd.FrameSize.Height}");

			timing.SeekMs += decodeTiming.SeekMs;
			timing.DecodeMs += decodeTiming.DecodeMs;
			byte[] data = scaler.ScaleToGray32(srcFrame, out D3D11GrayByteScaleTiming scaleTiming);
			timing.FilterMs += scaleTiming.FilterMs;
			timing.TinyConvertMs += scaleTiming.TinyConvertMs;
			timing.MapMs += scaleTiming.MapMs;
			timing.CopyMs += scaleTiming.CopyMs;
			timing.TinyDownloads += scaleTiming.TinyDownloads;
			return data;
		}

		static unsafe byte[] ExtractGrayBytesWithD3D11GpuScaleOrCpu(
			VideoStreamDecoder vsd,
			D3D11VideoProcessorGrayByteScaler scaler,
			AVFrame frame,
			ref VideoFrameConverter? converter,
			ref Size converterSourceSize,
			ref AVPixelFormat converterSourcePixelFormat,
			NativeGrayByteTiming timing) {
			if (VideoStreamDecoder.IsHardwareFrame(frame)) {
				byte[] data = scaler.ScaleToGray32(frame, out D3D11GrayByteScaleTiming scaleTiming);
				timing.FilterMs += scaleTiming.FilterMs;
				timing.TinyConvertMs += scaleTiming.TinyConvertMs;
				timing.MapMs += scaleTiming.MapMs;
				timing.CopyMs += scaleTiming.CopyMs;
				timing.TinyDownloads += scaleTiming.TinyDownloads;
				return data;
			}

			byte[] cpuData = ExtractGrayBytesFromFrame(vsd, frame, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, out long convertMs, out long copyMs);
			timing.ConvertMs += convertMs;
			timing.CopyMs += copyMs;
			return cpuData;
		}

		static unsafe byte[] ExtractGrayBytesWithD3D11GpuScaleOrCpu(
			VideoStreamDecoder vsd,
			D3D11VideoProcessorGrayByteScaler scaler,
			string filePath,
			TimeSpan position,
			ref VideoFrameConverter? converter,
			ref Size converterSourceSize,
			ref AVPixelFormat converterSourcePixelFormat,
			NativeGrayByteTiming timing) {
			if (!vsd.TryDecodeFrame(out var srcFrame, position, out DecodedFrameTiming decodeTiming, FrameTransferMode.KeepHardwareFrame))
				throw new Exception($"TryDecodeFrame failed at pos={position} for '{filePath}'. size={vsd.FrameSize.Width}x{vsd.FrameSize.Height}");

			timing.SeekMs += decodeTiming.SeekMs;
			timing.DecodeMs += decodeTiming.DecodeMs;
			return ExtractGrayBytesWithD3D11GpuScaleOrCpu(vsd, scaler, srcFrame, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, timing);
		}

		static bool ShouldUseSequentialBatch(List<GrayByteRequest> requests) {
			if (requests.Count <= 1)
				return true;

			double spanSeconds = (requests[^1].Position - requests[0].Position).TotalSeconds;
			return spanSeconds <= SequentialBatchMaxSpanSeconds;
		}

		static List<GrayByteRequestCluster> BuildGrayByteRequestClusters(List<GrayByteRequest> requests) {
			List<GrayByteRequestCluster> clusters = new();

			if (ShouldUseSequentialBatch(requests)) {
				GrayByteRequestCluster cluster = new(requests[0]);
				for (int i = 1; i < requests.Count; i++)
					cluster.Requests.Add(requests[i]);
				clusters.Add(cluster);
				return clusters;
			}

			foreach (GrayByteRequest request in requests)
					clusters.Add(new GrayByteRequestCluster(request));

			return clusters;
		}

		static string GetEffectiveGrayByteHardwarePolicy(string configuredPolicy, bool useD3D11GpuScale, NativeGrayByteTiming timing) {
			if (!useD3D11GpuScale)
				return configuredPolicy;
			if (timing.TinyDownloads == 0)
				return "d3d11-software-frames";
			if (timing.TinyDownloads < timing.SampledFrames)
				return "d3d11-video-processor-gray32-mixed";
			return configuredPolicy;
		}

		static unsafe byte[] ExtractNativeGrayBytesFromDecodedFrame(
			VideoStreamDecoder vsd,
			bool useD3D11GpuScale,
			ref D3D11VideoProcessorGrayByteScaler? d3d11Scaler,
			AVFrame frame,
			ref VideoFrameConverter? converter,
			ref Size converterSourceSize,
			ref AVPixelFormat converterSourcePixelFormat,
			NativeGrayByteTiming timing) {
			if (useD3D11GpuScale && timing.SampledFrames == 0 && !VideoStreamDecoder.IsHardwareFrame(frame))
				throw new D3D11SoftwareFrameFallbackException((AVPixelFormat)frame.format);

			timing.SampledFrames++;
			if (useD3D11GpuScale && VideoStreamDecoder.IsHardwareFrame(frame)) {
				d3d11Scaler ??= new D3D11VideoProcessorGrayByteScaler();
				return ExtractGrayBytesWithD3D11GpuScaleOrCpu(vsd, d3d11Scaler, frame, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, timing);
			}

			byte[] data = ExtractGrayBytesFromFrame(vsd, frame, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, out long convertMs, out long copyMs);
			timing.ConvertMs += convertMs;
			timing.CopyMs += copyMs;
			return data;
		}

		static void AccumulateD3D11ScaleTiming(D3D11GrayByteScaleTiming scaleTiming, NativeGrayByteTiming timing) {
			timing.FilterMs += scaleTiming.FilterMs;
			timing.TinyConvertMs += scaleTiming.TinyConvertMs;
			timing.MapMs += scaleTiming.MapMs;
			timing.CopyMs += scaleTiming.CopyMs;
			timing.TinyDownloads += scaleTiming.TinyDownloads;
		}

		static void FlushOldestPendingD3D11GrayBytes(D3D11VideoProcessorGrayByteScaler scaler, List<PendingD3D11GrayByteResult> pendingDownloads, List<GrayByteResult> results, NativeGrayByteTiming timing) {
			PendingD3D11GrayByteResult pending = pendingDownloads[0];
			pendingDownloads.RemoveAt(0);
			byte[] data = scaler.DownloadGray32(pending.PendingDownload, out D3D11GrayByteScaleTiming scaleTiming);
			AccumulateD3D11ScaleTiming(scaleTiming, timing);
			results.Add(CreateGrayByteResult(pending.Request, data));
		}

		static void FlushAllPendingD3D11GrayBytes(D3D11VideoProcessorGrayByteScaler? scaler, List<PendingD3D11GrayByteResult> pendingDownloads, List<GrayByteResult> results, NativeGrayByteTiming timing) {
			if (pendingDownloads.Count == 0)
				return;
			if (scaler == null)
				throw new Exception("D3D11 graybyte download queue has pending downloads but no scaler.");
			while (pendingDownloads.Count > 0)
				FlushOldestPendingD3D11GrayBytes(scaler, pendingDownloads, results, timing);
		}

		static unsafe void QueueOrExtractNativeGrayBytesFromDecodedFrame(
			VideoStreamDecoder vsd,
			bool useD3D11GpuScale,
			ref D3D11VideoProcessorGrayByteScaler? d3d11Scaler,
			AVFrame frame,
			GrayByteRequest request,
			ref VideoFrameConverter? converter,
			ref Size converterSourceSize,
			ref AVPixelFormat converterSourcePixelFormat,
			NativeGrayByteTiming timing,
			List<PendingD3D11GrayByteResult> pendingDownloads,
			List<GrayByteResult> results) {
			bool isHardwareFrame = VideoStreamDecoder.IsHardwareFrame(frame);
			if (useD3D11GpuScale && timing.SampledFrames == 0 && !isHardwareFrame)
				throw new D3D11SoftwareFrameFallbackException((AVPixelFormat)frame.format);

			timing.SampledFrames++;
			if (useD3D11GpuScale && isHardwareFrame) {
				d3d11Scaler ??= new D3D11VideoProcessorGrayByteScaler();
				if (pendingDownloads.Count >= d3d11Scaler.PendingDownloadCapacity)
					FlushOldestPendingD3D11GrayBytes(d3d11Scaler, pendingDownloads, results, timing);
				pendingDownloads.Add(new PendingD3D11GrayByteResult(request, d3d11Scaler.EnqueueScaleToGray32(frame)));
				return;
			}

			byte[] data = ExtractGrayBytesFromFrame(vsd, frame, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, out long convertMs, out long copyMs);
			timing.ConvertMs += convertMs;
			timing.CopyMs += copyMs;
			results.Add(CreateGrayByteResult(request, data));
		}

		static unsafe bool TryGetGrayBytesFromVideoNativeBatch(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging, List<GrayByteResult> results, out FfmpegErrorCategory failureCategory, out AVHWDeviceType failureHardwareDeviceType, bool allowD3D11GpuScale = true, bool forceCpuDecode = false, string? forcedCpuPolicy = null) {
			int requestedSamples = 0;
			string hardwarePolicy = "unresolved";
			string? familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);
			string? codecName = GetPrimaryVideoCodecName(videoFile);
			AVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			failureCategory = FfmpegErrorCategory.Unknown;
			failureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			try {
				List<GrayByteRequest> requests = GetMissingGrayByteRequests(videoFile, positions, maxSamplingDurationSeconds);
				requestedSamples = requests.Count;
				if (requests.Count == 0)
					return true;

				var batchSw = Stopwatch.StartNew();
				var queueSw = Stopwatch.StartNew();
				if (forceCpuDecode) {
					hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
					hardwarePolicy = forcedCpuPolicy ?? "d3d11-software-frames-cpu-retry";
				}
				else {
					hardwareDeviceType = GetConfiguredGrayByteHardwareDeviceType(out hardwarePolicy);
					if (hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && ShouldBypassHardwareDecodeForCodec(
						codecName,
						out _,
						familyKey)) {
						hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
						hardwarePolicy = "hardware-decode-codec-bypass";
					}
					else if (hardwareDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA && ShouldBypassD3D11GrayByteForFamily(videoFile, out _)) {
						hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
						hardwarePolicy = "d3d11-software-frame-family-bypass";
					}
				}
				using IDisposable? d3d11GrayByteConcurrencyLease = EnterD3D11GrayByteConcurrencyLimiter(hardwareDeviceType);
			long queueMs = d3d11GrayByteConcurrencyLease == null ? 0 : queueSw.ElapsedMilliseconds;
			var openSw = Stopwatch.StartNew();
				FfmpegLogCapture.Reset();
				using var vsd = new VideoStreamDecoder(videoFile.Path, hardwareDeviceType);
				ThrowIfTiledHeifRequiresProcess(vsd, videoFile.Path);
				NativeGrayByteTiming nativeTiming = new() { QueueMs = queueMs, OpenMs = openSw.ElapsedMilliseconds };
				VideoFrameConverter? converter = null;
				D3D11VideoProcessorGrayByteScaler? d3d11Scaler = null;
				List<PendingD3D11GrayByteResult> pendingD3D11Downloads = new();
				Size converterSourceSize = default;
				AVPixelFormat converterSourcePixelFormat = AVPixelFormat.AV_PIX_FMT_NONE;
				bool useD3D11GpuScale = allowD3D11GpuScale && ShouldUseD3D11GrayByteGpuScale(hardwareDeviceType, ref hardwarePolicy, out _);
				if (!allowD3D11GpuScale && hardwareDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA)
					hardwarePolicy = "d3d11-video-processor-gray32-disabled-for-call";
				List<GrayByteRequestCluster> clusters = BuildGrayByteRequestClusters(requests);
				bool hasClusteredBatch = clusters.Any(cluster => cluster.Requests.Count > 1);
				string batchMode = clusters.Count == 1 && ShouldUseSequentialBatch(requests)
					? "sequential"
					: hasClusteredBatch
						? "clustered"
						: "seek-per-sample";
				try {
					foreach (GrayByteRequestCluster cluster in clusters) {
						if (cluster.Requests.Count > 1) {
							int clusterIndex = 0;
							var decodePositions = cluster.Requests.Select(request => request.Position).ToList();
							bool decodedCluster = vsd.TryDecodeFrames(decodePositions, (position, frame, frameTiming) => {
								GrayByteRequest request = cluster.Requests[clusterIndex++];
								nativeTiming.SeekMs += frameTiming.SeekMs;
								nativeTiming.DecodeMs += frameTiming.DecodeMs;
								nativeTiming.TransferMs += frameTiming.TransferMs;
								nativeTiming.HardwareTransfers += frameTiming.HardwareTransfers;
								nativeTiming.FullFrameTransfers += frameTiming.HardwareTransfers;
								QueueOrExtractNativeGrayBytesFromDecodedFrame(vsd, useD3D11GpuScale, ref d3d11Scaler, frame, request, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, nativeTiming, pendingD3D11Downloads, results);
							}, out _, useD3D11GpuScale ? FrameTransferMode.KeepHardwareFrame : FrameTransferMode.TransferHardwareFrame);
							if (!decodedCluster || clusterIndex != cluster.Requests.Count)
								throw new Exception($"Native clustered batch decoded {clusterIndex} of {cluster.Requests.Count} requested graybyte sample(s).");
						}
						else {
							GrayByteRequest request = cluster.Requests[0];
							byte[] data;
							if (useD3D11GpuScale) {
								if (!vsd.TryDecodeFrame(out var srcFrame, request.Position, out DecodedFrameTiming decodeTiming, FrameTransferMode.KeepHardwareFrame))
									throw new Exception($"TryDecodeFrame failed at pos={request.Position} for '{videoFile.Path}'. size={vsd.FrameSize.Width}x{vsd.FrameSize.Height}");
								nativeTiming.SeekMs += decodeTiming.SeekMs;
								nativeTiming.DecodeMs += decodeTiming.DecodeMs;
								QueueOrExtractNativeGrayBytesFromDecodedFrame(vsd, true, ref d3d11Scaler, srcFrame, request, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, nativeTiming, pendingD3D11Downloads, results);
							}
							else {
								data = ExtractGrayBytesWithDecoder(vsd, videoFile.Path, request.Position, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, nativeTiming);
								results.Add(CreateGrayByteResult(request, data));
							}
						}
					}

					FlushAllPendingD3D11GrayBytes(d3d11Scaler, pendingD3D11Downloads, results, nativeTiming);
					if (results.Count != requests.Count)
						throw new Exception($"Native batch decoded {results.Count} of {requests.Count} requested graybyte sample(s).");
				}
				finally {
					d3d11Scaler?.Dispose();
					converter?.Dispose();
				}
				if (ShouldLogNativeSuccessTiming(extendedLogging))
					LogNativeBatchTiming(videoFile.Path, GetD3D11GrayByteAdaptiveFamilyKey(videoFile) ?? string.Empty, vsd.IsHardwareDecode, GetEffectiveGrayByteHardwarePolicy(hardwarePolicy, useD3D11GpuScale, nativeTiming), batchMode, requests.Count, nativeTiming, batchSw.ElapsedMilliseconds);
				if (hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && vsd.IsHardwareDecode)
					RecordHardwareDecodeSuccessForCodec(
						codecName,
						familyKey);
				if (useD3D11GpuScale && vsd.IsHardwareDecode) {
					long d3d11TotalMs = batchSw.ElapsedMilliseconds;
					ObserveD3D11GrayByteConcurrency(nativeTiming);
					ObserveD3D11GrayByteFamily(videoFile, nativeTiming, d3d11TotalMs);
					if (!forceCpuDecode && ShouldProbeD3D11GrayByteFamilyWithCpu(videoFile, out string probeFamilyKey)) {
						List<GrayByteResult> d3d11Results = new(results);
						results.Clear();
						var cpuProbeSw = Stopwatch.StartNew();
						bool cpuProbeSucceeded = TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, out _, out _, allowD3D11GpuScale: false, forceCpuDecode: true, forcedCpuPolicy: "d3d11-adaptive-cpu-probe");
						long cpuTotalMs = cpuProbeSw.ElapsedMilliseconds;
						if (cpuProbeSucceeded && cpuTotalMs < d3d11TotalMs) {
							CompleteD3D11GrayByteCpuProbe(videoFile, probeFamilyKey, d3d11TotalMs, cpuTotalMs);
							return true;
						}

						CompleteD3D11GrayByteCpuProbe(videoFile, probeFamilyKey, d3d11TotalMs, cpuProbeSucceeded ? cpuTotalMs : long.MaxValue);
						results.Clear();
						results.AddRange(d3d11Results);
					}
				}
				RecordNativeSuccess();
				return true;
			}
			catch (Exception e) {
				string diagnostics = FfmpegLogCapture.GetRecent();
				failureCategory = FfmpegErrorClassifier.Categorize(
					diagnostics.Length > 0 ? $"{diagnostics} {e.Message}" : e.Message);
				failureHardwareDeviceType = hardwareDeviceType;
				if (e is TiledHeifRequiresProcessException) {
					Logger.Instance.Info(e.Message);
					return false;
				}
				if (IsNativeBindingLoadFailure(e)) {
					RecordNativeFailure(videoFile.Path, e);
					return false;
				}
				if (e is D3D11SoftwareFrameFallbackException && !forceCpuDecode) {
					RecordD3D11SoftwareFrameFallbackForCodec(
						codecName,
						e.Message,
						familyKey);
					Logger.Instance.Info($"Native FFmpeg graybyte extraction detected software frames under D3D11 on '{videoFile.Path}', retrying native batch with CPU decode. Staged {results.Count} of {requestedSamples} sample(s). Reason: {NormalizeLogReason(e.Message, 240)}");
					results.Clear();
					return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, out failureCategory, out failureHardwareDeviceType, allowD3D11GpuScale: false, forceCpuDecode: true);
				}
				string failureText = $"{hardwarePolicy} {hardwareDeviceType} {e}";
				if (!forceCpuDecode && hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && IsHardwareDecodeFailure(failureText)) {
					MarkConfiguredHardwareDecodeFailure(failureText);
					RecordHardwareDecodeFailureForCodec(
						codecName,
						failureText,
						familyKey);
					Logger.Instance.Info($"Native FFmpeg graybyte extraction hit a hardware decode failure on '{videoFile.Path}', retrying native batch with CPU decode. hwPolicy={hardwarePolicy}. Staged {results.Count} of {requestedSamples} sample(s). Reason: {NormalizeLogReason(e.Message, 240)}");
					results.Clear();
					return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, out failureCategory, out failureHardwareDeviceType, allowD3D11GpuScale: false, forceCpuDecode: true, forcedCpuPolicy: "hardware-decode-failure-cpu-retry");
				}
				RecordNativeFailure(videoFile.Path, e);
				Logger.Instance.Info($"Native FFmpeg batched graybyte extraction failed on '{videoFile.Path}', falling back to per-sample path for missing samples. hwPolicy={hardwarePolicy}. Staged {results.Count} of {requestedSamples} sample(s). Reason: {e.Message}");
				return false;
			}
		}

		/// <summary>
		/// Extracts one 32x32 grayscale frame per position, opening a single decoder and
		/// reusing one sws context for the whole file instead of paying the open/seek/teardown
		/// cost per frame. Returns an array aligned with <paramref name="positionsSeconds"/>;
		/// entries are null when that frame could not be decoded. Positions the native batch
		/// could not produce (or all of them, without the native binding) fall back to the
		/// per-frame <see cref="GetThumbnail"/> path, which itself falls back to the FFmpeg process.
		/// </summary>
		internal static unsafe byte[]?[] GetGrayFrames(string filePath, IReadOnlyList<double> positionsSeconds, bool extendedLogging, string? hardwareCodecName = null) {
			const int N = 32;
			var frames = new byte[]?[positionsSeconds.Count];
			if (ShouldUseNativeBinding) {
				AVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
				try {
					FfmpegLogCapture.Reset();
					using var vsd = new VideoStreamDecoder(filePath, hardwareDeviceType);
					ThrowIfTiledHeifRequiresProcess(vsd, filePath);
					VideoFrameConverter? converter = null;
					Size converterSourceSize = default;
					AVPixelFormat converterSrcFmt = AVPixelFormat.AV_PIX_FMT_NONE;
					try {
						for (int i = 0; i < positionsSeconds.Count; i++) {
							if (!vsd.TryDecodeFrame(out var srcFrame, TimeSpan.FromSeconds(positionsSeconds[i])))
								continue;

							Size sourceSize = new(
								srcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width,
								srcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);
							AVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);
							if (srcPixFmt < 0 || srcPixFmt >= AVPixelFormat.AV_PIX_FMT_NB ||
								sourceSize.Width <= 0 || sourceSize.Height <= 0)
								continue;

							if (converter == null || sourceSize != converterSourceSize || srcPixFmt != converterSrcFmt) {
								converter?.Dispose();
								converter = new VideoFrameConverter(
									sourceSize, srcPixFmt,
									new Size(N, N), AVPixelFormat.AV_PIX_FMT_GRAY8,
									VideoFrameConverter.ScaleQuality.Bicubic, bitExact: false);
								converterSourceSize = sourceSize;
								converterSrcFmt = srcPixFmt;
							}

							frames[i] = ExtractGray32FromFrame(converter.Convert(srcFrame));
						}
					}
					finally {
						converter?.Dispose();
					}
					if (hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && vsd.IsHardwareDecode)
						RecordHardwareDecodeSuccessForCodec(hardwareCodecName);
					RecordNativeSuccess();
				}
				catch (Exception e) {
					if (e is TiledHeifRequiresProcessException)
						Logger.Instance.Info(e.Message);
					else if (IsNativeBindingLoadFailure(e))
						RecordNativeFailure(filePath, e);
					else if (hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE) {
						string failureText = $"{hardwareDeviceType} {e}";
						if (IsHardwareDecodeFailure(failureText)) {
							MarkConfiguredHardwareDecodeFailure(failureText);
							RecordHardwareDecodeFailureForCodec(hardwareCodecName, failureText);
						}
					}
					Logger.Instance.Info($"Native batch frame extraction failed on '{filePath}', falling back to per-frame path. Exception: {e}{BuildNativeFailureDetail(e)}");
				}
			}

			for (int i = 0; i < positionsSeconds.Count; i++) {
				frames[i] ??= GetThumbnail(new FfmpegSettings {
					File = filePath,
					Position = TimeSpan.FromSeconds(positionsSeconds[i]),
					GrayScale = 1,
					HardwareCodecName = hardwareCodecName,
					SoftwareDecodeOnly = true
				}, extendedLogging);
			}
			return frames;
		}

		static int GetGrayScaleSideLength(int sideLength) =>
			sideLength > 0 ? Math.Clamp(sideLength, 16, 256) : 32;

		public static unsafe byte[]? GetThumbnail(FfmpegSettings settings, bool extendedLogging, int timeoutMilliseconds = TimeoutDuration) {
			bool isGrayByte = settings.GrayScale == 1;
			bool isRgbFrame = settings.Rgb224;
			int expectedRgbBytes = global::VDF.Core.AI.OnnxEmbedder.InputSide * global::VDF.Core.AI.OnnxEmbedder.InputSide * 3;
			int graySideLength =
				GetGrayScaleSideLength(settings.GrayScaleSize);
			int expectedGrayBytes = graySideLength * graySideLength;
			string hardwarePolicy = "unresolved";
			bool bypassHardwareForFamily = isGrayByte && ShouldBypassGrayByteHardwareForFamily(settings.HardwareFamilyKey);
			bool bypassHardwareForCodec = ShouldBypassHardwareDecodeForCodec(
				settings.HardwareCodecName,
				out _,
				settings.HardwareFamilyKey);
			bool enableHardwareAcceleration;
			if (settings.SoftwareDecodeOnly) {
				hardwarePolicy = "software-decode-only";
				enableHardwareAcceleration = false;
			}
			else if (settings.ForceCpuDecode) {
				hardwarePolicy = "hardware-decode-failure-cpu-retry";
				enableHardwareAcceleration = false;
			}
			else if (bypassHardwareForCodec) {
				hardwarePolicy = "hardware-decode-codec-bypass";
				enableHardwareAcceleration = false;
			}
			else if (bypassHardwareForFamily) {
				hardwarePolicy = "hardware-decode-failure-cpu-family-bypass";
				enableHardwareAcceleration = false;
			}
			else {
				enableHardwareAcceleration = isGrayByte
					? ShouldUseProcessHardwareAccelerationForGrayBytes(out hardwarePolicy)
					: true;
			}
			AVHWDeviceType nativeHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;

			try {
				if (!isRgbFrame && ShouldAttemptNativeSingleFrameExtraction(settings)) {
					var totalSw = Stopwatch.StartNew();
					long openMs = 0, seekMs = 0, decodeMs = 0, transferMs = 0, convertMs = 0, copyMs = 0;
					int hardwareTransfers = 0;
					var phaseSw = Stopwatch.StartNew();
					nativeHardwareDeviceType = isGrayByte
						? GetConfiguredGrayByteHardwareDeviceType(out hardwarePolicy)
						: GetConfiguredHardwareDeviceType(enableHardwareAcceleration);
					if (settings.ForceCpuDecode || bypassHardwareForFamily) {
						nativeHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
						hardwarePolicy = settings.ForceCpuDecode ? "hardware-decode-failure-cpu-retry" : "hardware-decode-failure-cpu-family-bypass";
					}
					if (settings.SoftwareDecodeOnly) {
						nativeHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
						hardwarePolicy = "software-decode-only";
					}
					if (!isGrayByte)
						hardwarePolicy = settings.SoftwareDecodeOnly ? "software-decode-only" : GetHardwarePolicy(nativeHardwareDeviceType, enableHardwareAcceleration);
					FfmpegLogCapture.Reset();
					using var vsd = new VideoStreamDecoder(settings.File, nativeHardwareDeviceType);
					ThrowIfTiledHeifRequiresProcess(vsd, settings.File);
					openMs = phaseSw.ElapsedMilliseconds;

					Size sourceSize = vsd.FrameSize;
					phaseSw.Restart();
					if (!vsd.TryDecodeFrame(out var srcFrame, settings.Position, out DecodedFrameTiming frameTiming))
						throw new Exception($"TryDecodeFrame failed at pos={settings.Position} for '{settings.File}'. size={sourceSize.Width}x{sourceSize.Height}");
					seekMs = frameTiming.SeekMs;
					decodeMs = frameTiming.DecodeMs;
					transferMs = frameTiming.TransferMs;
					hardwareTransfers = frameTiming.HardwareTransfers;

					AVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);
					if (!IsValidPixelFormat(srcPixFmt)) throw new Exception($"Invalid source pixel format {srcPixFmt}");
					if (sourceSize.Width <= 0 || sourceSize.Height <= 0) throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");

					AVRational sampleAspectRatio = vsd.StreamSampleAspectRatio;
					if (sampleAspectRatio.num <= 0 || sampleAspectRatio.den <= 0)
						sampleAspectRatio = srcFrame.sample_aspect_ratio;
					Size displaySize = isGrayByte
						? sourceSize
						: GetDisplaySizeForSampleAspectRatio(
							sourceSize, sampleAspectRatio.num, sampleAspectRatio.den);
					Size destinationSize = isGrayByte
						? new Size(graySideLength, graySideLength)
						: settings.Fullsize == 1
							? displaySize
							: ScaleToMaxWidth(displaySize, settings.MaxWidth > 0 ? settings.MaxWidth : 100);

					AVPixelFormat destinationPixelFrmt = isGrayByte ? AVPixelFormat.AV_PIX_FMT_GRAY8 : AVPixelFormat.AV_PIX_FMT_YUVJ420P;

					phaseSw.Restart();
					using var vfc = new VideoFrameConverter(sourceSize, srcPixFmt, destinationSize, destinationPixelFrmt, isGrayByte ? VideoFrameConverter.ScaleQuality.FastBilinear : VideoFrameConverter.ScaleQuality.Bicubic, false);
					AVFrame convertedFrame = vfc.Convert(srcFrame);
					convertMs = phaseSw.ElapsedMilliseconds;

					phaseSw.Restart();
					if (isGrayByte) {
						byte[] outBuf =
							ExtractGrayFrameFromFrame(
								convertedFrame,
								graySideLength);
						copyMs = phaseSw.ElapsedMilliseconds;
						if (ShouldLogNativeSuccessTiming(extendedLogging))
							LogNativeTiming(settings.File, settings.Position, true, vsd.IsHardwareDecode, hardwarePolicy, openMs, seekMs, decodeMs, transferMs, hardwareTransfers, convertMs, copyMs, totalSw.ElapsedMilliseconds);
						if (nativeHardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && vsd.IsHardwareDecode)
							RecordHardwareDecodeSuccessForCodec(
							settings.HardwareCodecName,
							settings.HardwareFamilyKey);
						RecordNativeSuccess();
						return outBuf;
					}
					else {
						if (convertedFrame.width <= 0 || convertedFrame.height <= 0)
							throw new Exception($"Invalid converted frame dimensions {convertedFrame.width}x{convertedFrame.height}.");
						byte[] jpeg = JpegFrameEncoder.Encode(convertedFrame,
							settings.JpegQuality > 0 ? settings.JpegQuality : DefaultJpegQuality);
						copyMs = phaseSw.ElapsedMilliseconds;
						if (ShouldLogNativeSuccessTiming(extendedLogging))
							LogNativeTiming(settings.File, settings.Position, false, vsd.IsHardwareDecode, hardwarePolicy, openMs, seekMs, decodeMs, transferMs, hardwareTransfers, convertMs, copyMs, totalSw.ElapsedMilliseconds);
						if (nativeHardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && vsd.IsHardwareDecode)
							RecordHardwareDecodeSuccessForCodec(
							settings.HardwareCodecName,
							settings.HardwareFamilyKey);
						RecordNativeSuccess();
						return jpeg;
					}
				}
			}
			catch (Exception e) {
				string failureText = $"{hardwarePolicy} {nativeHardwareDeviceType} {e}";
				if (IsNativeBindingLoadFailure(e)) {
					RecordNativeFailure(settings.File, e);
				}
				else if (!settings.ForceCpuDecode && enableHardwareAcceleration && nativeHardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && IsHardwareDecodeFailure(failureText)) {
					MarkConfiguredHardwareDecodeFailure(failureText);
					RecordHardwareDecodeFailureForCodec(
					settings.HardwareCodecName,
					failureText,
					settings.HardwareFamilyKey);
					Logger.Instance.Info($"Native FFmpeg extraction hit a hardware decode failure on '{settings.File}', retrying with CPU decode. hwPolicy={hardwarePolicy}. Reason: {NormalizeLogReason(e.Message, 240)}");
					return GetThumbnail(settings with { ForceCpuDecode = true }, extendedLogging, timeoutMilliseconds);
				}
				Logger.Instance.Info($"Failed using native FFmpeg binding on '{settings.File}', try switching to process mode. hwPolicy={hardwarePolicy}. Reason: {e.Message}");
			}

			var psi = new ProcessStartInfo {
				FileName = FFmpegPath,
				CreateNoWindow = true,
				RedirectStandardInput = false,
				RedirectStandardOutput = true,
				WorkingDirectory = Path.GetDirectoryName(FFmpegPath)!,
				// Always capture stderr: when FFmpeg fails, its error output is the only
				// diagnostic there is. Logged on failure regardless of the logging setting
				// (issue #780 — 'exited with: 134' with no further detail is undebuggable).
				RedirectStandardError = true,
				WindowStyle = ProcessWindowStyle.Hidden
			};

			psi.ArgumentList.Add("-hide_banner");
			psi.ArgumentList.Add("-loglevel"); psi.ArgumentList.Add("error");

			psi.ArgumentList.Add("-nostdin");

			if (!isGrayByte && hardwarePolicy == "unresolved")
				hardwarePolicy = GetHardwarePolicy(GetConfiguredHardwareDeviceType(enableHardwareAcceleration), enableHardwareAcceleration);
			bool processAttemptedHardware = enableHardwareAcceleration && !settings.SoftwareDecodeOnly && HardwareAccelerationMode != FFHardwareAccelerationMode.none;
			if (processAttemptedHardware) {
				psi.ArgumentList.Add("-hwaccel");
				psi.ArgumentList.Add(HardwareAccelerationMode.ToString());
			}

			// Skip input seeking for still images: some JPEGs otherwise reach EOF before
			// a frame enters the filter graph, producing an empty successful output (#801).
			if (!FileUtils.IsImageFile(settings.File)) {
				psi.ArgumentList.Add("-ss"); psi.ArgumentList.Add(settings.Position.ToString(null, CultureInfo.InvariantCulture));
			}
			psi.ArgumentList.Add("-i"); psi.ArgumentList.Add(FFToolsUtils.LongPathFix(settings.File));

			string? userVfFilter = null;
			var remainingCustomArgs = new List<string>();
			if (!string.IsNullOrWhiteSpace(CustomFFArguments)) {
				var tokens = TokenizeArgs(CustomFFArguments);
				for (int ti = 0; ti < tokens.Count; ti++) {
					if ((tokens[ti] == "-vf" || tokens[ti] == "-filter:v") && ti + 1 < tokens.Count) userVfFilter = tokens[++ti];
					else remainingCustomArgs.Add(tokens[ti]);
				}
			}

			if (isRgbFrame) {
				int side = global::VDF.Core.AI.OnnxEmbedder.InputSide;
				psi.ArgumentList.Add("-vf");
				psi.ArgumentList.Add($"scale={side}:{side}:flags=bicubic,format=rgb24");
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("rawvideo");
				psi.ArgumentList.Add("-pix_fmt"); psi.ArgumentList.Add("rgb24");
			}
			else if (isGrayByte) {
				string vfChain = $"scale={graySideLength}:{graySideLength}:flags=bicubic,format=gray";
				if (userVfFilter != null) vfChain = $"{userVfFilter},{vfChain}";
				psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(vfChain);
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("rawvideo");
				psi.ArgumentList.Add("-pix_fmt"); psi.ArgumentList.Add("gray");
			}
			else {
				string? vfChain = BuildSarNormalizationFilter(settings.File);

				if (settings.Fullsize != 1) {
					int maxW = settings.MaxWidth > 0 ? settings.MaxWidth : 100;
					// Apply the bounding-box resize after correcting the display aspect ratio.
					string resizeFilter = $"scale=min({maxW}\\,iw):min({maxW}\\,ih):force_original_aspect_ratio=decrease";
					vfChain = vfChain == null ? resizeFilter : $"{vfChain},{resizeFilter}";
				}

				if (userVfFilter != null)
					vfChain = vfChain == null ? userVfFilter : $"{vfChain},{userVfFilter}";

				if (vfChain != null) {
					psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(vfChain);
				}
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("mjpeg");
				// Map 1-100 quality onto MJPEG's 2-31 qscale (lower = better), same curve
				// as JpegFrameEncoder so CLI and native output comparable quality.
				int quality = settings.JpegQuality > 0 ? settings.JpegQuality : DefaultJpegQuality;
				psi.ArgumentList.Add("-q:v"); psi.ArgumentList.Add(Math.Clamp(2 + (100 - quality) / 10, 2, 31).ToString(CultureInfo.InvariantCulture));
			}

			psi.ArgumentList.Add("-frames:v"); psi.ArgumentList.Add("1");
			if (!isRgbFrame)
				foreach (var item in remainingCustomArgs) psi.ArgumentList.Add(item);
			psi.ArgumentList.Add("pipe:1");

			var processSw = Stopwatch.StartNew();
			using var process = new Process { StartInfo = psi };
			var errOut = new FfmpegErrorAccumulator();
			byte[]? bytes = null;
			try {
				process.EnableRaisingEvents = true;
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				process.ErrorDataReceived += new DataReceivedEventHandler((sender, e) => {
					errOut.AppendLine(e.Data);
				});
				process.BeginErrorReadLine();
				using var ms = new MemoryStream();
				int effectiveTimeoutMilliseconds =
					Math.Clamp(
						timeoutMilliseconds,
						250,
						TimeoutDuration);
				Task copyTask =
					process.StandardOutput.BaseStream.CopyToAsync(ms);

				if (!copyTask.Wait(effectiveTimeoutMilliseconds)) {
					throw new TimeoutException(
						$"FFmpeg timed out after " +
						$"{effectiveTimeoutMilliseconds}ms on file: " +
						$"{settings.File}");
				}

				int remainingTimeoutMilliseconds = Math.Max(
					1,
					effectiveTimeoutMilliseconds -
						(int)Math.Min(
							processSw.ElapsedMilliseconds,
							effectiveTimeoutMilliseconds - 1L));

				if (!process.WaitForExit(remainingTimeoutMilliseconds)) {
					throw new TimeoutException(
						$"FFmpeg timed out after " +
						$"{effectiveTimeoutMilliseconds}ms on file: " +
						$"{settings.File}");
				}

				process.WaitForExit(); // Flush asynchronous stderr handlers.
				copyTask.GetAwaiter().GetResult();

				if (process.ExitCode != 0) throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}");

				bytes = ms.ToArray();
				if (bytes.Length == 0) bytes = null;
				else if (isGrayByte && bytes.Length != expectedGrayBytes) {
					errOut.AppendLine($"graybytes length != {expectedGrayBytes} (got {bytes.Length})");
					bytes = null;
				}
				else if (isRgbFrame && bytes.Length != expectedRgbBytes) {
					errOut.AppendLine($"AI frame length != {expectedRgbBytes} (got {bytes.Length})");
					bytes = null;
				}
				if (bytes != null && processAttemptedHardware)
					RecordHardwareDecodeSuccessForCodec(
							settings.HardwareCodecName,
							settings.HardwareFamilyKey);
			}
			catch (Exception e) {
				errOut.AppendLine(e.Message);
				try {
					if (!process.HasExited) process.Kill();
				}
				catch { }
				bytes = null;
			}
			string ffmpegError = errOut.ToString();
			if (bytes == null && FileUtils.IsHeifImageFile(settings.File)) {
				byte[]? gridBytes = TryGetTiledHeifGridFrame(
					settings,
					isGrayByte,
					isRgbFrame,
					graySideLength,
					expectedGrayBytes,
					expectedRgbBytes,
					timeoutMilliseconds,
					out string gridError);
				if (!string.IsNullOrWhiteSpace(gridError))
					ffmpegError = string.IsNullOrWhiteSpace(ffmpegError)
						? gridError
						: $"{ffmpegError}{Environment.NewLine}HEIF tile-grid retry:{Environment.NewLine}{gridError}";
				if (gridBytes != null) {
					bytes = gridBytes;
					processAttemptedHardware = false;
					hardwarePolicy = "heif-grid-process";
				}
			}
			if (bytes == null && FileUtils.IsHeifImageFile(settings.File)) {
				byte[]? gridBytes = TryGetTiledHeifGridFrame(
					settings,
					isGrayByte,
					isRgbFrame,
					graySideLength,
					expectedGrayBytes,
					expectedRgbBytes,
					timeoutMilliseconds,
					out string gridError);
				if (!string.IsNullOrWhiteSpace(gridError))
					ffmpegError = string.IsNullOrWhiteSpace(ffmpegError)
						? gridError
						: $"{ffmpegError}{Environment.NewLine}HEIF tile-grid retry:{Environment.NewLine}{gridError}";
				if (gridBytes != null) {
					bytes = gridBytes;
					processAttemptedHardware = false;
					hardwarePolicy = "heif-grid-process";
				}
			}
			if (bytes == null && FileUtils.IsHeifImageFile(settings.File)) {
				byte[]? gridBytes = TryGetTiledHeifGridFrame(
					settings,
					isGrayByte,
					isRgbFrame,
					graySideLength,
					expectedGrayBytes,
					expectedRgbBytes,
					timeoutMilliseconds,
					out string gridError);
				if (!string.IsNullOrWhiteSpace(gridError))
					ffmpegError = string.IsNullOrWhiteSpace(ffmpegError)
						? gridError
						: $"{ffmpegError}{Environment.NewLine}HEIF tile-grid retry:{Environment.NewLine}{gridError}";
				if (gridBytes != null) {
					bytes = gridBytes;
					processAttemptedHardware = false;
					hardwarePolicy = "heif-grid-process";
				}
			}
			if (bytes == null && FileUtils.IsHeifImageFile(settings.File)) {
				byte[]? gridBytes = TryGetTiledHeifGridFrame(
					settings,
					isGrayByte,
					isRgbFrame,
					graySideLength,
					expectedGrayBytes,
					expectedRgbBytes,
					timeoutMilliseconds,
					out string gridError);
				if (!string.IsNullOrWhiteSpace(gridError))
					ffmpegError = string.IsNullOrWhiteSpace(ffmpegError)
						? gridError
						: $"{ffmpegError}{Environment.NewLine}HEIF tile-grid retry:{Environment.NewLine}{gridError}";
				if (gridBytes != null) {
					bytes = gridBytes;
					processAttemptedHardware = false;
					hardwarePolicy = "heif-grid-process";
				}
			}
			if (bytes == null && FileUtils.IsHeifImageFile(settings.File)) {
				byte[]? gridBytes = TryGetTiledHeifGridFrame(
					settings,
					isGrayByte,
					isRgbFrame,
					graySideLength,
					expectedGrayBytes,
					expectedRgbBytes,
					timeoutMilliseconds,
					out string gridError);
				if (!string.IsNullOrWhiteSpace(gridError))
					ffmpegError = string.IsNullOrWhiteSpace(ffmpegError)
						? gridError
						: $"{ffmpegError}{Environment.NewLine}HEIF tile-grid retry:{Environment.NewLine}{gridError}";
				if (gridBytes != null) {
					bytes = gridBytes;
					processAttemptedHardware = false;
					hardwarePolicy = "heif-grid-process";
				}
			}
			// When a still image was extracted successfully, discard FFmpeg's known-benign
			// PNG demuxer chatter instead of logging a false warning (#805/#809/#815).
			if (bytes != null && ffmpegError.Length > 0 && FileUtils.IsImageFile(settings.File))
				ffmpegError = FilterBenignImageDemuxerNoise(ffmpegError);
			long processTotalMs = processSw.ElapsedMilliseconds;
			// Failures always log (including FFmpeg's stderr); success-with-warnings only
			// when extended logging is enabled, to avoid noise from benign decoder chatter.
			if (bytes == null || (extendedLogging && ffmpegError.Length > 0)) {
				bool processHardwareFailure = processAttemptedHardware && IsHardwareDecodeFailure(ffmpegError);
				if (processHardwareFailure) {
					MarkConfiguredHardwareDecodeFailure(ffmpegError);
					RecordHardwareDecodeFailureForCodec(
						settings.HardwareCodecName,
						ffmpegError,
						settings.HardwareFamilyKey);
				}
				if (!settings.ForceCpuDecode && processHardwareFailure && bytes == null) {
					Logger.Instance.Info($"FFmpeg process extraction hit a hardware decode failure on '{settings.File}', retrying with CPU decode. hwPolicy={hardwarePolicy}. Reason: {NormalizeLogReason(ffmpegError, 240)}");
					return GetThumbnail(settings with { ForceCpuDecode = true }, extendedLogging, timeoutMilliseconds);
				}
				string message = $"{(bytes == null ? "ERROR: Failed to retrieve" : "WARNING: Problems while retrieving")} {(isGrayByte ? "graybytes" : "thumbnail")} from: {settings.File}";
				if (extendedLogging) {
					var args = string.Join(" ", psi.ArgumentList);
					message += $":{Environment.NewLine}{FFmpegPath} {args}";
				}
				Logger.Instance.Info($"{message}{(ffmpegError.Length > 0 ? Environment.NewLine + ffmpegError : string.Empty)}");
			}
			if (bytes != null && extendedLogging && !isGrayByte)
				LogProcessTiming(settings.File, settings.Position, false, processAttemptedHardware, hardwarePolicy, bytes.Length, processTotalMs);
			return bytes;
		}
		internal static bool GetGrayBytesFromVideo(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging, Action<int>? onSampleComplete = null) {
			List<GrayByteRequest> requests = GetMissingGrayByteRequests(videoFile, positions, maxSamplingDurationSeconds);
			int missingPositions = requests.Count;
			int completedSamples = 0;
			void ReportCompletedSample() => onSampleComplete?.Invoke(++completedSamples);
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (videoFile.grayBytes.TryGetValue(position, out byte[]? bytes) && bytes != null)
					ReportCompletedSample();
			}

			string? hardwareFamilyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);
			string? hardwareCodecName = GetPrimaryVideoCodecName(videoFile);
			if (missingPositions == 0) {
				if (ShouldLogGrayByteScanTelemetry(extendedLogging))
					LogCachedGrayByteScan(videoFile.Path, hardwareFamilyKey, hardwareCodecName, completedSamples, positions.Count);
				return true;
			}

			int tooDarkCounter = 0;
			string nativeGrayByteState = DescribeNativeGrayBytePathState();
			List<GrayByteResult> stagedResults = new(missingPositions);
			FfmpegErrorCategory nativeFailureCategory = FfmpegErrorCategory.Unknown;
			AVHWDeviceType nativeFailureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			if (nativeGrayByteState == "available" && TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, stagedResults, out nativeFailureCategory, out nativeFailureHardwareDeviceType)) {
				CommitGrayByteResults(videoFile, stagedResults, ref tooDarkCounter);
				foreach (GrayByteResult _ in stagedResults)
					ReportCompletedSample();
				if (tooDarkCounter == missingPositions) {
					videoFile.Flags.Set(EntryFlags.TooDark);
					Logger.Instance.Info($"ERROR: Graybytes too dark of: {videoFile.Path}");
					return false;
				}
				return true;
			}
			if (nativeGrayByteState == "available" &&
				ShouldSkipProcessRetryForCorruptFile(
					nativeFailureCategory,
					nativeFailureHardwareDeviceType)) {
				videoFile.Flags.Set(EntryFlags.ThumbnailError);
				Logger.Instance.Info(
					$"Skipping process-mode retry for '{videoFile.Path}': the native software decode failure indicates a truncated or corrupt file, and the FFmpeg process would run the same decoder over the same damaged bitstream.");
				return false;
			}
			if (ShouldLogGrayByteScanTelemetry(extendedLogging) && nativeGrayByteState != "available")
				LogNativeGrayByteBatchSkipped(videoFile.Path, hardwareFamilyKey, nativeGrayByteState, missingPositions);

			tooDarkCounter = 0;
			int stagedNativeSamples = stagedResults.Count;
			HashSet<double> stagedIndexes = stagedResults.Select(result => result.Index).ToHashSet();
			int reportedStagedResults = 0;
			var processBatchSw = Stopwatch.StartNew();
			foreach (GrayByteRequest request in requests) {
				if (stagedIndexes.Contains(request.Index)) continue;

				var data = GetThumbnail(new FfmpegSettings {
					File = videoFile.Path,
					Position = request.Position,
					GrayScale = 1,
					HardwareFamilyKey = hardwareFamilyKey,
					HardwareCodecName = hardwareCodecName,
				}, extendedLogging);

				if (data == null) {
					videoFile.Flags.Set(EntryFlags.ThumbnailError);
					return false;
				}
				stagedResults.Add(CreateGrayByteResult(request, data));
				stagedIndexes.Add(request.Index);
				reportedStagedResults++;
				ReportCompletedSample();
			}
			if (ShouldLogGrayByteScanTelemetry(extendedLogging))
				LogProcessGrayByteBatchTiming(videoFile.Path, hardwareFamilyKey, hardwareCodecName, nativeGrayByteState == "available" ? "fallback" : nativeGrayByteState, DescribeProcessGrayByteHardwarePolicy(hardwareFamilyKey, hardwareCodecName), reportedStagedResults, missingPositions, stagedNativeSamples, processBatchSw.ElapsedMilliseconds);
			if (stagedResults.Count != missingPositions) {
				videoFile.Flags.Set(EntryFlags.ThumbnailError);
				return false;
			}

			CommitGrayByteResults(videoFile, stagedResults, ref tooDarkCounter);
			for (int i = reportedStagedResults; i < stagedResults.Count; i++)
				ReportCompletedSample();
			if (tooDarkCounter == missingPositions) {
				videoFile.Flags.Set(EntryFlags.TooDark);
				Logger.Instance.Info($"ERROR: Graybytes too dark of: {videoFile.Path}");
				return false;
			}
			return true;
		}

		// Markers for FFmpeg PNG demuxer false-positives that occur after a still frame
		// has already been decoded successfully (issues #805/#809/#815).
		static readonly string[] BenignImageDemuxerMarkers = {
			"Invalid PNG signature",
			"chunk too big",
		};

		/// <summary>
		/// Strips known-benign FFmpeg demuxer lines (and the png decoder's follow-up
		/// "Decoding error" line) from captured stderr. Only used for still images whose
		/// frame was nonetheless extracted, so a non-fatal decode line cannot hide a real
		/// failure. Returns the surviving lines with the original leading newline layout.
		/// </summary>
		static string FilterBenignImageDemuxerNoise(string errOut) {
			var lines = errOut.Split(Environment.NewLine);
			var kept = new List<string>(lines.Length);
			foreach (var line in lines) {
				if (line.Length == 0)
					continue;
				bool benign = false;
				foreach (var marker in BenignImageDemuxerMarkers)
					if (line.Contains(marker, StringComparison.OrdinalIgnoreCase)) {
						benign = true;
						break;
					}
				// The png decoder emits a paired "Decoding error: Invalid data ..." line
				// alongside the bogus signature; drop it too when it names the png decoder.
				if (!benign && line.Contains("/png @", StringComparison.Ordinal) &&
					line.Contains("Decoding error", StringComparison.Ordinal))
					benign = true;
				if (!benign)
					kept.Add(line);
			}
			return kept.Count == 0 ? string.Empty : Environment.NewLine + string.Join(Environment.NewLine, kept);
		}

		private static List<string> TokenizeArgs(string args) {
			var tokens = new List<string>();
			var current = new System.Text.StringBuilder();
			bool inQuotes = false;
			foreach (char c in args) {
				if (c == '"') inQuotes = !inQuotes;
				else if (c == ' ' && !inQuotes) {
					if (current.Length > 0) {
						tokens.Add(current.ToString());
						current.Clear();
					}
				}
				else current.Append(c);
			}
			if (current.Length > 0) tokens.Add(current.ToString());
			return tokens;
		}

		/// <summary>
		/// Extracts a single JPEG thumbnail from a video or image file at the given
		/// position (ignored for images). FFmpeg does the scaling and encoding directly.
		/// Returns null if extraction fails.
		/// </summary>
		public static List<byte[]?> ExtractThumbnailJpegs(string filePath, IReadOnlyList<TimeSpan> positions, int maxWidth = 0, bool extendedLogging = false, int jpegQuality = 0, string? hardwareCodecName = null) {
			var frames = new byte[]?[positions.Count];
			if (positions.Count == 0)
				return frames.ToList();

			bool forceCpuForRemaining = false;
			if (ShouldUseNativeBinding)
				TryExtractThumbnailJpegsNative(filePath, positions, maxWidth, extendedLogging, jpegQuality, hardwareCodecName, frames, ref forceCpuForRemaining);

			for (int i = 0; i < positions.Count; i++) {
				frames[i] ??= GetThumbnail(new FfmpegSettings {
					File = filePath,
					Position = positions[i],
					GrayScale = 0,
					Fullsize = (byte)(maxWidth == 0 ? 1 : 0),
					MaxWidth = maxWidth,
					JpegQuality = jpegQuality,
					HardwareCodecName = hardwareCodecName,
					ForceCpuDecode = forceCpuForRemaining,
				}, extendedLogging);
			}

			return frames.ToList();
		}

		static unsafe bool TryExtractThumbnailJpegsNative(
			string filePath,
			IReadOnlyList<TimeSpan> positions,
			int maxWidth,
			bool extendedLogging,
			int jpegQuality,
			string? hardwareCodecName,
			byte[]?[] frames,
			ref bool forceCpuForRemaining,
			bool forceCpuDecode = false) {
			string hardwarePolicy = forceCpuDecode ? "hardware-decode-failure-cpu-retry" : "unresolved";
			AVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			try {
				bool bypassHardwareForCodec = ShouldBypassHardwareDecodeForCodec(hardwareCodecName, out _);
				bool enableHardwareAcceleration = !forceCpuDecode && !bypassHardwareForCodec;
				hardwareDeviceType = GetConfiguredHardwareDeviceType(enableHardwareAcceleration);
				hardwarePolicy = forceCpuDecode
					? "hardware-decode-failure-cpu-retry"
					: bypassHardwareForCodec
						? "hardware-decode-codec-bypass"
						: GetHardwarePolicy(hardwareDeviceType, enableHardwareAcceleration);

				var openSw = Stopwatch.StartNew();
				FfmpegLogCapture.Reset();
				using var vsd = new VideoStreamDecoder(filePath, hardwareDeviceType);
				long openMs = openSw.ElapsedMilliseconds;
				VideoFrameConverter? converter = null;
				Size converterSourceSize = default;
				AVPixelFormat converterSourcePixelFormat = AVPixelFormat.AV_PIX_FMT_NONE;
				bool anySuccess = false;
				try {
					for (int i = 0; i < positions.Count; i++) {
						var totalSw = Stopwatch.StartNew();
						long openForSampleMs = i == 0 ? openMs : 0;
						if (!vsd.TryDecodeFrame(out var srcFrame, positions[i], out DecodedFrameTiming frameTiming)) {
							if (!forceCpuDecode && hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE)
								throw new Exception($"TryDecodeFrame failed at pos={positions[i]} for '{filePath}'. size={vsd.FrameSize.Width}x{vsd.FrameSize.Height}");
							continue;
						}

						byte[] jpeg = ExtractJpegFromFrame(vsd, srcFrame, maxWidth, jpegQuality, ref converter, ref converterSourceSize, ref converterSourcePixelFormat, out long convertMs, out long copyMs);
						frames[i] = jpeg;
						anySuccess = true;
						if (ShouldLogNativeSuccessTiming(extendedLogging))
							LogNativeTiming(filePath, positions[i], false, vsd.IsHardwareDecode, hardwarePolicy, openForSampleMs, frameTiming.SeekMs, frameTiming.DecodeMs, frameTiming.TransferMs, frameTiming.HardwareTransfers, convertMs, copyMs, totalSw.ElapsedMilliseconds + openForSampleMs);
					}
				}
				finally {
					converter?.Dispose();
				}

				if (hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && vsd.IsHardwareDecode)
					RecordHardwareDecodeSuccessForCodec(hardwareCodecName);
				if (anySuccess)
					RecordNativeSuccess();
				return anySuccess;
			}
			catch (Exception e) {
				string failureText = $"{hardwarePolicy} {hardwareDeviceType} {e}";
				if (IsNativeBindingLoadFailure(e)) {
					RecordNativeFailure(filePath, e);
					return false;
				}
				if (!forceCpuDecode && hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && IsHardwareDecodeFailure(failureText)) {
					forceCpuForRemaining = true;
					MarkConfiguredHardwareDecodeFailure(failureText);
					RecordHardwareDecodeFailureForCodec(hardwareCodecName, failureText);
					Logger.Instance.Info($"Native FFmpeg batched thumbnail extraction hit a hardware decode failure on '{filePath}', retrying batch with CPU decode. hwPolicy={hardwarePolicy}. Reason: {NormalizeLogReason(e.Message, 240)}");
					return TryExtractThumbnailJpegsNative(filePath, positions, maxWidth, extendedLogging, jpegQuality, hardwareCodecName, frames, ref forceCpuForRemaining, forceCpuDecode: true);
				}

				Logger.Instance.Info($"Native FFmpeg batched thumbnail extraction failed on '{filePath}', falling back to per-position path. hwPolicy={hardwarePolicy}. Reason: {NormalizeLogReason(e.Message, 240)}");
				return false;
			}
		}

		public static byte[]? ExtractThumbnailJpeg(string filePath, TimeSpan position, int maxWidth = 0, bool extendedLogging = false, int jpegQuality = 0, string? hardwareCodecName = null) {
			return GetThumbnail(new FfmpegSettings {
				File = filePath,
				Position = position,
				GrayScale = 0,
				Fullsize = (byte)(maxWidth == 0 ? 1 : 0),
				MaxWidth = maxWidth,
				JpegQuality = jpegQuality,
				HardwareCodecName = hardwareCodecName,
			}, extendedLogging);
		}

		/// <summary>
		/// Converts coded video dimensions and sample aspect ratio into the square-pixel
		/// dimensions a media player displays. Coded dimensions remain untouched elsewhere.
		/// </summary>
		internal static Size GetDisplaySizeForSampleAspectRatio(
			Size source,
			int sarNumerator,
			int sarDenominator) {
			if (source.Width <= 0 || source.Height <= 0)
				return source;
			if (sarNumerator <= 0 || sarDenominator <= 0 || sarNumerator == sarDenominator)
				return source;

			double exactWidth = source.Width * (double)sarNumerator / sarDenominator;
			if (!double.IsFinite(exactWidth) || exactWidth <= 0 || exactWidth > 65536)
				return source;

			return new Size(Math.Max(1, (int)Math.Floor(exactWidth)), source.Height);
		}

		// Upstream compatibility name used by the SAR regression tests.
		internal static Size ApplySampleAspectRatio(Size codedSize, int sarNum, int sarDen) =>
			GetDisplaySizeForSampleAspectRatio(codedSize, sarNum, sarDen);

		internal static string? BuildSarNormalizationFilter(string file) {
			if (FileUtils.IsImageFile(file))
				return null;
			const string sarMul = "if(eq(sar\\,0)\\,1\\,sar)";
			return $"scale=if(gt(iw*{sarMul}\\,65536)\\,iw\\,trunc(iw*{sarMul})):ih,setsar=1";
		}

		/// <summary>Downscale-only fit into a maxDim x maxDim bounding box, preserving aspect ratio.</summary>
		static Size ScaleToMaxWidth(Size source, int maxDim) {
			if (source.Width <= maxDim && source.Height <= maxDim)
				return source;
			double factor = Math.Max(source.Width / (double)maxDim, source.Height / (double)maxDim);
			return new Size(
				Math.Max(1, (int)Math.Round(source.Width / factor)),
				Math.Max(1, (int)Math.Round(source.Height / factor)));
		}

		/// <summary>
		/// Native fast path for hashing a still image: decodes the (single) frame once and
		/// returns both the 32x32 gray bytes and the source dimensions, avoiding a separate
		/// ffprobe call. Returns false when the native binding is unavailable or decoding
		/// fails — callers fall back to the CLI path.
		/// </summary>
		internal static unsafe bool TryGetImageInfoAndGrayBytes(string path, out byte[]? grayBytes, out int width, out int height, bool extendedLogging) {
			const int N = 32;
			grayBytes = null;
			width = 0;
			height = 0;
			if (!ShouldUseNativeBinding)
				return false;
			try {
				// Stills never benefit from HW decoders (and some HW paths reject them).
				FfmpegLogCapture.Reset();
				using var vsd = new VideoStreamDecoder(path);
				ThrowIfTiledHeifRequiresProcess(vsd, path);
				if (!vsd.TryDecodeFrame(out var srcFrame, TimeSpan.Zero))
					throw new Exception($"TryDecodeFrame failed for image '{path}'");

				Size sourceSize = new(
					srcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width,
					srcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);
				AVPixelFormat srcPixFmt = vsd.IsHardwareDecode ? (AVPixelFormat)srcFrame.format : vsd.PixelFormat;
				if (srcPixFmt < 0 || srcPixFmt >= AVPixelFormat.AV_PIX_FMT_NB)
					throw new Exception($"Invalid source pixel format {srcPixFmt}");
				if (sourceSize.Width <= 0 || sourceSize.Height <= 0)
					throw new Exception($"Invalid source dimensions {sourceSize.Width}x{sourceSize.Height}");

				using var converter = new VideoFrameConverter(
					sourceSize, srcPixFmt,
					new Size(N, N), AVPixelFormat.AV_PIX_FMT_GRAY8,
					VideoFrameConverter.ScaleQuality.Bicubic, bitExact: false);
				AVFrame convertedFrame = converter.Convert(srcFrame);
				grayBytes = ExtractGray32FromFrame(convertedFrame);
				width = sourceSize.Width;
				height = sourceSize.Height;
				RecordNativeSuccess();
				return true;
			}
			catch (Exception e) {
				if (IsNativeBindingLoadFailure(e))
					RecordNativeFailure(path, e);
				if (extendedLogging)
					Logger.Instance.Info($"Native image decode failed on '{path}', falling back to process mode. Exception: {e}{BuildNativeFailureDetail(e)}");
				return false;
			}
		}

		/// <summary>
		/// Encodes raw BGRA pixels into a JPEG, optionally downscaling to
		/// <paramref name="maxWidth"/>. Used by the GUI to encode composed thumbnail
		/// strips for the on-disk cache. Native binding preferred; falls back to an
		/// FFmpeg process fed via stdin.
		/// </summary>
		public static unsafe byte[]? EncodeJpegFromBgra(byte[] bgra, int width, int height, int maxWidth = 0, int quality = 0) {
			if (bgra == null || width <= 0 || height <= 0 || bgra.Length < (long)width * height * 4)
				return null;
			if (quality <= 0) quality = DefaultJpegQuality;
			Size destSize = maxWidth > 0 ? ScaleToMaxWidth(new Size(width, height), maxWidth) : new Size(width, height);

			if (ShouldUseNativeBinding) {
				try {
					AVFrame* srcFrame = ffmpeg.av_frame_alloc();
					if (srcFrame == null) throw new FFInvalidExitCodeException("Failed to allocate AVFrame.");
					try {
						srcFrame->format = (int)AVPixelFormat.AV_PIX_FMT_BGRA;
						srcFrame->width = width;
						srcFrame->height = height;
						ffmpeg.av_frame_get_buffer(srcFrame, 0).ThrowExceptionIfError();
						int srcStride = srcFrame->linesize[0];
						int rowBytes = width * 4;
						fixed (byte* src = bgra) {
							for (int y = 0; y < height; y++)
								Buffer.MemoryCopy(src + (long)y * rowBytes, srcFrame->data[0] + (long)y * srcStride, rowBytes, rowBytes);
						}
						using var converter = new VideoFrameConverter(
							new Size(width, height), AVPixelFormat.AV_PIX_FMT_BGRA,
							destSize, AVPixelFormat.AV_PIX_FMT_YUVJ420P,
							VideoFrameConverter.ScaleQuality.Bicubic, bitExact: false);
						AVFrame converted = converter.Convert(*srcFrame);
						byte[] jpeg = JpegFrameEncoder.Encode(converted, quality);
						RecordNativeSuccess();
						return jpeg;
					}
					finally {
						ffmpeg.av_frame_free(&srcFrame);
					}
				}
				catch (Exception e) {
					if (IsNativeBindingLoadFailure(e))
						RecordNativeFailure("BGRA thumbnail strip", e);
					else
						Logger.Instance.Info($"Native BGRA->JPEG encode failed, falling back to process mode. Exception: {e}{BuildNativeFailureDetail(e)}");
				}
			}

			// CLI fallback: raw BGRA via stdin -> mjpeg via stdout.
			var psi = new ProcessStartInfo {
				FileName = FFmpegPath,
				CreateNoWindow = true,
				RedirectStandardInput = true,
				RedirectStandardOutput = true,
				WorkingDirectory = Path.GetDirectoryName(FFmpegPath)!,
				WindowStyle = ProcessWindowStyle.Hidden
			};
			psi.ArgumentList.Add("-hide_banner");
			psi.ArgumentList.Add("-loglevel"); psi.ArgumentList.Add("quiet");
			psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("rawvideo");
			psi.ArgumentList.Add("-pix_fmt"); psi.ArgumentList.Add("bgra");
			psi.ArgumentList.Add("-video_size"); psi.ArgumentList.Add($"{width}x{height}");
			psi.ArgumentList.Add("-i"); psi.ArgumentList.Add("pipe:0");
			if (destSize.Width != width)
				{ psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add($"scale={destSize.Width}:-1"); }
			psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("mjpeg");
			psi.ArgumentList.Add("-q:v"); psi.ArgumentList.Add(Math.Clamp(2 + (100 - quality) / 10, 2, 31).ToString(CultureInfo.InvariantCulture));
			psi.ArgumentList.Add("-frames:v"); psi.ArgumentList.Add("1");
			psi.ArgumentList.Add("pipe:1");

			using var process = new Process { StartInfo = psi };
			try {
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				using var ms = new MemoryStream();
				// Write input and read output concurrently to avoid pipe-buffer deadlocks.
				var readTask = process.StandardOutput.BaseStream.CopyToAsync(ms);
				process.StandardInput.BaseStream.Write(bgra, 0, width * height * 4);
				process.StandardInput.BaseStream.Flush();
				process.StandardInput.Close();
				readTask.Wait(TimeoutDuration);
				if (!process.WaitForExit(TimeoutDuration))
					throw new TimeoutException("FFmpeg timed out encoding JPEG from raw pixels.");
				if (process.ExitCode != 0)
					throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}");
				byte[] jpeg = ms.ToArray();
				return jpeg.Length > 0 ? jpeg : null;
			}
			catch (Exception e) {
				Logger.Instance.Info($"BGRA->JPEG encode via FFmpeg process failed: {e.Message}");
				try { if (!process.HasExited) process.Kill(); } catch { }
				return null;
			}
		}
	}

	internal struct FfmpegSettings {
		public byte GrayScale;
		public int GrayScaleSize;
		public byte Fullsize;
		public string File;
		public TimeSpan Position;
		public string? HardwareFamilyKey;
		public string? HardwareCodecName;
		public bool ForceCpuDecode;
		/// <summary>Target max width for non-fullsize thumbnails; 0 = default (100). Downscale only.</summary>
		public int MaxWidth;
		/// <summary>JPEG quality 1-100; 0 = default (90).</summary>
		public int JpegQuality;
		/// <summary>Skip hardware acceleration (used for still images).</summary>
		public bool SoftwareDecodeOnly;
		/// <summary>Produce a raw 224x224 RGB24 AI embedding frame.</summary>
		public bool Rgb224;
	}
}
