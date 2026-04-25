using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using FFmpeg.AutoGen;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Processing;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static class FfmpegEngine {
		public static readonly string FFmpegPath;
		const int TimeoutDuration = 15_000;
		const string ForceNativeGrayByteCpuEnvVar = "VDF_FORCE_NATIVE_GRAYBYTE_CPU";
		const string DisableNativeGrayByteGpuScaleEnvVar = "VDF_DISABLE_NATIVE_GRAYBYTE_GPU_SCALE";
		const string DisableNativeGrayByteD3D11AdaptiveEnvVar = "VDF_DISABLE_NATIVE_GRAYBYTE_D3D11_ADAPTIVE";
		const string D3D11GrayByteGpuScaleDisabledPolicy = "requested-gpu-scale-disabled-after-failure";
		const int D3D11GrayByteAdaptiveMinimumObservations = 3;
		const long D3D11GrayByteAdaptiveSlowPerSampleMs = 140;
		static readonly object D3D11GrayByteGpuScaleStateLock = new();
		static bool d3d11GrayByteGpuScaleDisabled;
		static readonly object D3D11GrayByteAdaptiveStateLock = new();
		static readonly Dictionary<string, D3D11GrayByteAdaptiveStats> D3D11GrayByteAdaptiveStatsByFamily = new(StringComparer.OrdinalIgnoreCase);
		public static FFHardwareAccelerationMode HardwareAccelerationMode;
		public static string CustomFFArguments = string.Empty;
		public static bool UseNativeBinding;
		private static readonly SixLabors.ImageSharp.Formats.Jpeg.JpegEncoder jpegEncoder = new();
		static FfmpegEngine() => FFmpegPath = FFToolsUtils.GetPath(FFToolsUtils.FFTool.FFmpeg) ?? string.Empty;

		static void LogNativeTiming(string file, TimeSpan position, bool isGrayByte, bool hwDecode, string hardwarePolicy, long openMs, long seekMs, long decodeMs, long transferMs, int hardwareTransfers, long convertMs, long copyMs, long totalMs) {
			Logger.Instance.Info($"Native FFmpeg timing on '{file}' @ {position}: mode={(isGrayByte ? "gray32" : "thumb")}, hw={(hwDecode ? "requested" : "off")}, hwPolicy={hardwarePolicy}, hwTransfers={hardwareTransfers}/1, open={openMs}ms, seek={seekMs}ms, decode={decodeMs}ms, transfer={transferMs}ms, convert={convertMs}ms, copy={copyMs}ms, total={totalMs}ms");
		}

		static void LogNativeBatchTiming(string file, bool hwDecode, string hardwarePolicy, string batchMode, int samples, NativeGrayByteTiming timing, long totalMs) {
			Logger.Instance.Info($"Native FFmpeg batched graybyte extraction completed for '{file}': mode={batchMode}, hw={(hwDecode ? "requested" : "off")}, hwPolicy={hardwarePolicy}, hwTransfers={timing.HardwareTransfers}/{samples}, fullFrameTransfers={timing.FullFrameTransfers}/{samples}, tinyDownloads={timing.TinyDownloads}/{samples}, samples={samples}, open={timing.OpenMs}ms, seek={timing.SeekMs}ms, decode={timing.DecodeMs}ms, transfer={timing.TransferMs}ms, filter={timing.FilterMs}ms, convert={timing.ConvertMs}ms, tinyConvert={timing.TinyConvertMs}ms, map={timing.MapMs}ms, copy={timing.CopyMs}ms, total={totalMs}ms");
		}

		const double SequentialBatchMaxSpanSeconds = 2d;

		sealed class NativeGrayByteTiming {
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

		static GrayByteResult CreateGrayByteResult(GrayByteRequest request, byte[] data) =>
			new(request.Index, data, pHash.PerceptualHash.ComputePHashFromGray32x32(data), !GrayBytesUtils.VerifyGrayScaleValues(data));

		static void CommitGrayByteResults(FileEntry videoFile, IReadOnlyList<GrayByteResult> results, ref int tooDarkCounter) {
			foreach (GrayByteResult result in results) {
				videoFile.grayBytes[result.Index] = result.Data;
				videoFile.PHashes[result.Index] = result.PHash;
				if (result.TooDark) tooDarkCounter++;
			}
		}

		static bool IsEnvFlagEnabled(string variableName) {
			string? value = Environment.GetEnvironmentVariable(variableName);
			return value != null
				&& (value == "1"
					|| value.Equals("true", StringComparison.OrdinalIgnoreCase)
					|| value.Equals("yes", StringComparison.OrdinalIgnoreCase)
				|| value.Equals("on", StringComparison.OrdinalIgnoreCase));
		}

		static bool IsD3D11GrayByteGpuScaleDisabled() {
			lock (D3D11GrayByteGpuScaleStateLock) {
				return d3d11GrayByteGpuScaleDisabled;
			}
		}

		static void DisableD3D11GrayByteGpuScale(string reason) {
			string normalizedReason = NormalizeLogReason(reason, 240);
			lock (D3D11GrayByteGpuScaleStateLock) {
				if (d3d11GrayByteGpuScaleDisabled)
					return;
				d3d11GrayByteGpuScaleDisabled = true;
			}

			Logger.Instance.Info($"D3D11 GPU-scale graybyte path disabled for this process after FFmpeg rejected the scale graph. Native graybytes will retry with hardware decode plus full-frame transfer. Reason: {normalizedReason}");
		}

		static string NormalizeLogReason(string reason, int maxLength) {
			string normalized = reason.Replace(Environment.NewLine, " ").Trim();
			return normalized.Length <= maxLength ? normalized : normalized[..maxLength] + "...";
		}

		static MediaInfo.StreamInfo? GetPrimaryVideoStream(FileEntry videoFile) {
			if (videoFile.mediaInfo?.Streams == null)
				return null;
			MediaInfo.StreamInfo? selectedStream = null;
			int selectedPixels = -1;
			foreach (MediaInfo.StreamInfo stream in videoFile.mediaInfo.Streams) {
				if (!string.Equals(stream.CodecType, "video", StringComparison.OrdinalIgnoreCase))
					continue;
				int pixels = Math.Max(0, stream.Width) * Math.Max(0, stream.Height);
				if (selectedStream == null || pixels >= selectedPixels) {
					selectedStream = stream;
					selectedPixels = pixels;
				}
			}
			return selectedStream;
		}

		static string? GetD3D11GrayByteAdaptiveFamilyKey(FileEntry videoFile) {
			MediaInfo.StreamInfo? stream = GetPrimaryVideoStream(videoFile);
			if (stream == null)
				return null;
			string codec = string.IsNullOrWhiteSpace(stream.CodecName) ? "unknown-codec" : stream.CodecName.Trim();
			string pixelFormat = string.IsNullOrWhiteSpace(stream.PixelFormat) ? "unknown-pixfmt" : stream.PixelFormat.Trim();
			return $"{codec}|{pixelFormat}|{stream.Width}x{stream.Height}";
		}

		static bool ShouldBypassD3D11GrayByteForFamily(FileEntry videoFile, out string familyKey) {
			familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile) ?? string.Empty;
			if (familyKey.Length == 0 || IsEnvFlagEnabled(DisableNativeGrayByteD3D11AdaptiveEnvVar))
				return false;
			lock (D3D11GrayByteAdaptiveStateLock) {
				return D3D11GrayByteAdaptiveStatsByFamily.TryGetValue(familyKey, out D3D11GrayByteAdaptiveStats? stats) && stats.Bypass;
			}
		}

		static bool ShouldProbeD3D11GrayByteFamilyWithCpu(FileEntry videoFile, out string familyKey) {
			familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile) ?? string.Empty;
			if (familyKey.Length == 0 || IsEnvFlagEnabled(DisableNativeGrayByteD3D11AdaptiveEnvVar))
				return false;
			lock (D3D11GrayByteAdaptiveStateLock) {
				if (!D3D11GrayByteAdaptiveStatsByFamily.TryGetValue(familyKey, out D3D11GrayByteAdaptiveStats? stats) || !stats.CpuProbePending || stats.CpuProbeCompleted || stats.Bypass)
					return false;
				stats.CpuProbePending = false;
				stats.CpuProbeCompleted = true;
				return true;
			}
		}

		static void CompleteD3D11GrayByteCpuProbe(FileEntry videoFile, string familyKey, long d3d11TotalMs, long cpuTotalMs) {
			if (familyKey.Length == 0)
				return;
			lock (D3D11GrayByteAdaptiveStateLock) {
				if (!D3D11GrayByteAdaptiveStatsByFamily.TryGetValue(familyKey, out D3D11GrayByteAdaptiveStats? stats))
					return;
				if (cpuTotalMs < d3d11TotalMs) {
					stats.Bypass = true;
					Logger.Instance.Info($"Native FFmpeg D3D11 graybyte adaptive policy will use native CPU decode for family '{familyKey}' after CPU probe won: d3d11={d3d11TotalMs}ms, cpu={cpuTotalMs}ms. Set {DisableNativeGrayByteD3D11AdaptiveEnvVar}=1 to disable this scan-local policy.");
				}
				else {
					Logger.Instance.Info($"Native FFmpeg D3D11 graybyte adaptive policy will keep D3D11 for family '{familyKey}' after CPU probe lost: d3d11={d3d11TotalMs}ms, cpu={cpuTotalMs}ms.");
				}
			}
		}

		static void ObserveD3D11GrayByteFamily(FileEntry videoFile, NativeGrayByteTiming timing, long totalMs) {
			if (timing.TinyDownloads <= 0 || timing.SampledFrames <= 0)
				return;
			string? familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);
			if (familyKey == null || IsEnvFlagEnabled(DisableNativeGrayByteD3D11AdaptiveEnvVar))
				return;
			long perSampleMs = totalMs / Math.Max(1, timing.SampledFrames);
			lock (D3D11GrayByteAdaptiveStateLock) {
				if (!D3D11GrayByteAdaptiveStatsByFamily.TryGetValue(familyKey, out D3D11GrayByteAdaptiveStats? stats)) {
					stats = new D3D11GrayByteAdaptiveStats();
					D3D11GrayByteAdaptiveStatsByFamily.Add(familyKey, stats);
				}
				stats.Observations++;
				stats.TotalMs += totalMs;
				stats.Samples += timing.SampledFrames;
				if (perSampleMs >= D3D11GrayByteAdaptiveSlowPerSampleMs)
					stats.SlowObservations++;
				long averagePerSampleMs = stats.TotalMs / Math.Max(1, stats.Samples);
				if (!stats.Bypass
					&& !stats.CpuProbePending
					&& !stats.CpuProbeCompleted
					&& stats.Observations >= D3D11GrayByteAdaptiveMinimumObservations
					&& stats.SlowObservations >= D3D11GrayByteAdaptiveMinimumObservations
					&& averagePerSampleMs >= D3D11GrayByteAdaptiveSlowPerSampleMs) {
					stats.CpuProbePending = true;
					Logger.Instance.Info($"Native FFmpeg D3D11 graybyte adaptive policy will probe native CPU decode for family '{familyKey}' after {stats.Observations} D3D11 observation(s): avgPerSample={averagePerSampleMs}ms, slowThreshold={D3D11GrayByteAdaptiveSlowPerSampleMs}ms.");
				}
			}
		}

		static bool IsD3D11GrayByteGpuScaleFailure(Exception exception) {
			string message = exception.Message;
			return message.Contains("d3d11 graybytes", StringComparison.OrdinalIgnoreCase)
				|| message.Contains("D3D11 graybyte scaler", StringComparison.OrdinalIgnoreCase)
				|| message.Contains("D3D11 video processor graybyte scaler", StringComparison.OrdinalIgnoreCase);
		}

		static bool IsD3D11GrayByteGpuScaleGraphFailure(Exception exception) =>
			exception.Message.Contains("avfilter_graph_config(d3d11 graybytes)", StringComparison.OrdinalIgnoreCase);

		static string GetHardwarePolicy(AVHWDeviceType deviceType, bool enableHardwareAcceleration) {
			if (!enableHardwareAcceleration)
				return "disabled-for-call";
			return deviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_NONE ? "configured-off" : "requested";
		}

		static AVHWDeviceType GetConfiguredHardwareDeviceType(bool enableHardwareAcceleration = true) {
			if (!enableHardwareAcceleration)
				return AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;

			return HardwareAccelerationMode switch {
				FFHardwareAccelerationMode.vdpau => AVHWDeviceType.AV_HWDEVICE_TYPE_VDPAU,
				FFHardwareAccelerationMode.dxva2 => AVHWDeviceType.AV_HWDEVICE_TYPE_DXVA2,
				FFHardwareAccelerationMode.vaapi => AVHWDeviceType.AV_HWDEVICE_TYPE_VAAPI,
				FFHardwareAccelerationMode.qsv => AVHWDeviceType.AV_HWDEVICE_TYPE_QSV,
				FFHardwareAccelerationMode.cuda => AVHWDeviceType.AV_HWDEVICE_TYPE_CUDA,
				FFHardwareAccelerationMode.videotoolbox => AVHWDeviceType.AV_HWDEVICE_TYPE_VIDEOTOOLBOX,
				FFHardwareAccelerationMode.d3d11va => AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA,
				FFHardwareAccelerationMode.drm => AVHWDeviceType.AV_HWDEVICE_TYPE_DRM,
				FFHardwareAccelerationMode.mediacodec => AVHWDeviceType.AV_HWDEVICE_TYPE_MEDIACODEC,
				FFHardwareAccelerationMode.vulkan => AVHWDeviceType.AV_HWDEVICE_TYPE_VULKAN,
				_ => AVHWDeviceType.AV_HWDEVICE_TYPE_NONE
			};
		}

		static AVHWDeviceType GetConfiguredGrayByteHardwareDeviceType(out string hardwarePolicy) {
			AVHWDeviceType configuredDeviceType = GetConfiguredHardwareDeviceType();
			if (configuredDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_NONE) {
				hardwarePolicy = "configured-off";
				return configuredDeviceType;
			}

			if (IsEnvFlagEnabled(ForceNativeGrayByteCpuEnvVar)) {
				hardwarePolicy = $"forced-cpu-by-{ForceNativeGrayByteCpuEnvVar}";
				return AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			}

			hardwarePolicy = "requested";
			return configuredDeviceType;
		}

		static bool ShouldUseD3D11GrayByteGpuScale(AVHWDeviceType deviceType, ref string hardwarePolicy, out string unavailableReason) {
			unavailableReason = string.Empty;
			if (deviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA)
				return false;

			if (IsEnvFlagEnabled(DisableNativeGrayByteGpuScaleEnvVar)) {
				hardwarePolicy = $"requested-gpu-scale-disabled-by-{DisableNativeGrayByteGpuScaleEnvVar}";
				return false;
			}

			if (IsD3D11GrayByteGpuScaleDisabled()) {
				hardwarePolicy = D3D11GrayByteGpuScaleDisabledPolicy;
				return false;
			}

			hardwarePolicy = "d3d11-video-processor-gray32";
			return true;
		}

		static unsafe byte[] ExtractGray32FromFrame(AVFrame convertedFrame) {
			const int N = 32;
			int width = convertedFrame.width;
			int height = convertedFrame.height;
			if (width != N || height != N) throw new Exception($"Unexpected size {width}x{height}, expected {N}.");
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

		static List<GrayByteRequest> GetMissingGrayByteRequests(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds) {
			List<GrayByteRequest> requests = new();
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (!videoFile.grayBytes.ContainsKey(position))
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

		static unsafe byte[] ExtractGrayBytesFromFrame(VideoStreamDecoder vsd, AVFrame srcFrame, ref VideoFrameConverter? converter, ref Size converterSourceSize, ref AVPixelFormat converterSourcePixelFormat, out long convertMs, out long copyMs) {
			Size sourceSize = new(srcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width, srcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);
			if (sourceSize.Width <= 0 || sourceSize.Height <= 0)
				throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");

			AVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);
			if (!IsValidPixelFormat(srcPixFmt))
				throw new Exception($"Invalid source pixel format {srcPixFmt}");

			if (converter == null || sourceSize != converterSourceSize || srcPixFmt != converterSourcePixelFormat) {
				converter?.Dispose();
				converter = new VideoFrameConverter(sourceSize, srcPixFmt, new Size(32, 32), AVPixelFormat.AV_PIX_FMT_GRAY8, VideoFrameConverter.ScaleQuality.FastBilinear, false);
				converterSourceSize = sourceSize;
				converterSourcePixelFormat = srcPixFmt;
			}

			var phaseSw = Stopwatch.StartNew();
			phaseSw.Restart();
			AVFrame convertedFrame = converter.Convert(srcFrame);
			convertMs = phaseSw.ElapsedMilliseconds;

			phaseSw.Restart();
			byte[] outBuf = ExtractGray32FromFrame(convertedFrame);
			copyMs = phaseSw.ElapsedMilliseconds;

			return outBuf;
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

		static unsafe bool TryGetGrayBytesFromVideoNativeBatch(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging, List<GrayByteResult> results, bool allowD3D11GpuScale = true, bool forceCpuDecode = false, string? forcedCpuPolicy = null) {
			int requestedSamples = 0;
			string hardwarePolicy = "unresolved";
			try {
				List<GrayByteRequest> requests = GetMissingGrayByteRequests(videoFile, positions, maxSamplingDurationSeconds);
				requestedSamples = requests.Count;
				if (requests.Count == 0)
					return true;

				var batchSw = Stopwatch.StartNew();
				var openSw = Stopwatch.StartNew();
				AVHWDeviceType hardwareDeviceType;
				if (forceCpuDecode) {
					hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
					hardwarePolicy = forcedCpuPolicy ?? "d3d11-software-frames-cpu-retry";
				}
				else {
					hardwareDeviceType = GetConfiguredGrayByteHardwareDeviceType(out hardwarePolicy);
					if (hardwareDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA && ShouldBypassD3D11GrayByteForFamily(videoFile, out _)) {
						hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
						hardwarePolicy = "d3d11-adaptive-cpu-family-bypass";
					}
				}
				using var vsd = new VideoStreamDecoder(videoFile.Path, hardwareDeviceType);
				NativeGrayByteTiming nativeTiming = new() { OpenMs = openSw.ElapsedMilliseconds };
				VideoFrameConverter? converter = null;
				D3D11VideoProcessorGrayByteScaler? d3d11Scaler = null;
				List<PendingD3D11GrayByteResult> pendingD3D11Downloads = new();
				Size converterSourceSize = default;
				AVPixelFormat converterSourcePixelFormat = AVPixelFormat.AV_PIX_FMT_NONE;
				bool useD3D11GpuScale = allowD3D11GpuScale && ShouldUseD3D11GrayByteGpuScale(hardwareDeviceType, ref hardwarePolicy, out _);
				if (!allowD3D11GpuScale && hardwareDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA)
					hardwarePolicy = "d3d11-full-frame-after-gpu-scale-failure";
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
				if (extendedLogging)
					LogNativeBatchTiming(videoFile.Path, vsd.IsHardwareDecode, GetEffectiveGrayByteHardwarePolicy(hardwarePolicy, useD3D11GpuScale, nativeTiming), batchMode, requests.Count, nativeTiming, batchSw.ElapsedMilliseconds);
				if (useD3D11GpuScale && vsd.IsHardwareDecode) {
					long d3d11TotalMs = batchSw.ElapsedMilliseconds;
					ObserveD3D11GrayByteFamily(videoFile, nativeTiming, d3d11TotalMs);
					if (!forceCpuDecode && ShouldProbeD3D11GrayByteFamilyWithCpu(videoFile, out string probeFamilyKey)) {
						List<GrayByteResult> d3d11Results = new(results);
						results.Clear();
						var cpuProbeSw = Stopwatch.StartNew();
						bool cpuProbeSucceeded = TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, allowD3D11GpuScale: false, forceCpuDecode: true, forcedCpuPolicy: "d3d11-adaptive-cpu-probe");
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
				return true;
			}
			catch (Exception e) {
				if (e is D3D11SoftwareFrameFallbackException && !forceCpuDecode) {
					Logger.Instance.Info($"Native FFmpeg graybyte extraction detected software frames under D3D11 on '{videoFile.Path}', retrying native batch with CPU decode. Staged {results.Count} of {requestedSamples} sample(s). Reason: {NormalizeLogReason(e.Message, 240)}");
					results.Clear();
					return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, allowD3D11GpuScale: false, forceCpuDecode: true);
				}
				if (allowD3D11GpuScale && IsD3D11GrayByteGpuScaleFailure(e)) {
					if (IsD3D11GrayByteGpuScaleGraphFailure(e))
						DisableD3D11GrayByteGpuScale(e.Message);
					Logger.Instance.Info($"Native FFmpeg D3D11 GPU-scale graybyte extraction failed on '{videoFile.Path}', retrying native batch with full-frame hardware transfer. Staged {results.Count} of {requestedSamples} sample(s). Reason: {NormalizeLogReason(e.Message, 360)}");
					results.Clear();
					return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, false);
				}
				Logger.Instance.Info($"Native FFmpeg batched graybyte extraction failed on '{videoFile.Path}', falling back to per-sample path for missing samples. hwPolicy={hardwarePolicy}. Staged {results.Count} of {requestedSamples} sample(s). Reason: {e.Message}");
				return false;
			}
		}

		public static unsafe byte[]? GetThumbnail(FfmpegSettings settings, bool extendedLogging) {
			const int N = 32;
			const int ExpectedBytes = N * N;
			bool isGrayByte = settings.GrayScale == 1;
			bool enableHardwareAcceleration = true;
			string hardwarePolicy = "unresolved";

			try {
				if (UseNativeBinding) {
					var totalSw = Stopwatch.StartNew();
					long openMs = 0, seekMs = 0, decodeMs = 0, transferMs = 0, convertMs = 0, copyMs = 0;
					int hardwareTransfers = 0;
					var phaseSw = Stopwatch.StartNew();
					AVHWDeviceType hardwareDeviceType = isGrayByte
						? GetConfiguredGrayByteHardwareDeviceType(out hardwarePolicy)
						: GetConfiguredHardwareDeviceType(enableHardwareAcceleration);
					if (!isGrayByte)
						hardwarePolicy = GetHardwarePolicy(hardwareDeviceType, enableHardwareAcceleration);
					using var vsd = new VideoStreamDecoder(settings.File, hardwareDeviceType);
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

					Size destinationSize = isGrayByte
						? new Size(N, N)
						: settings.Fullsize == 1
							? sourceSize
							: new Size(100, Convert.ToInt32(sourceSize.Height * (100 / (double)sourceSize.Width)));

					AVPixelFormat destinationPixelFrmt = isGrayByte ? AVPixelFormat.AV_PIX_FMT_GRAY8 : AVPixelFormat.AV_PIX_FMT_BGRA;

					phaseSw.Restart();
					using var vfc = new VideoFrameConverter(sourceSize, srcPixFmt, destinationSize, destinationPixelFrmt, isGrayByte ? VideoFrameConverter.ScaleQuality.FastBilinear : VideoFrameConverter.ScaleQuality.Bicubic, false);
					AVFrame convertedFrame = vfc.Convert(srcFrame);
					convertMs = phaseSw.ElapsedMilliseconds;

					phaseSw.Restart();
					if (isGrayByte) {
						byte[] outBuf = ExtractGray32FromFrame(convertedFrame);
						copyMs = phaseSw.ElapsedMilliseconds;
						if (extendedLogging)
							LogNativeTiming(settings.File, settings.Position, true, vsd.IsHardwareDecode, hardwarePolicy, openMs, seekMs, decodeMs, transferMs, hardwareTransfers, convertMs, copyMs, totalSw.ElapsedMilliseconds);
						return outBuf;
					}
					else {
						int width = convertedFrame.width;
						int height = convertedFrame.height;
						if (convertedFrame.data[0] == null) throw new Exception("Converted frame has no data[0] (null).");
						if (width <= 0 || height <= 0) throw new Exception($"Invalid converted frame dimensions {width}x{height}.");
						long totalBytesLong = (long)width * height * 4;
						if (totalBytesLong > 200_000_000) throw new Exception($"Frame too large: {width}x{height} ({totalBytesLong} bytes).");
						var totalBytes = (int)totalBytesLong;
						var rgbaBytes = new byte[totalBytes];
						int stride = convertedFrame.linesize[0];
						if (stride < width * 4) throw new Exception($"Invalid stride ({stride}) for width {width}.");
						fixed (byte* destPtr = rgbaBytes) {
							byte* sourcePtr = convertedFrame.data[0];
							if (stride == width * 4) Buffer.MemoryCopy(sourcePtr, destPtr, totalBytes, totalBytes);
							else {
								int byteWidth = width * 4;
								for (int y = 0; y < height; y++)
									Buffer.MemoryCopy(sourcePtr + (y * stride), destPtr + (y * byteWidth), byteWidth, byteWidth);
							}
						}
						copyMs = phaseSw.ElapsedMilliseconds;
						if (extendedLogging)
							LogNativeTiming(settings.File, settings.Position, false, vsd.IsHardwareDecode, hardwarePolicy, openMs, seekMs, decodeMs, transferMs, hardwareTransfers, convertMs, copyMs, totalSw.ElapsedMilliseconds);
						var image = Image.LoadPixelData<SixLabors.ImageSharp.PixelFormats.Bgra32>(rgbaBytes, width, height);
						using MemoryStream stream = new();
						image.Save(stream, jpegEncoder);
						return stream.ToArray();
					}
				}
			}
			catch (Exception e) {
				Logger.Instance.Info($"Failed using native FFmpeg binding on '{settings.File}', try switching to process mode. hwPolicy={hardwarePolicy}. Reason: {e.Message}");
			}

			var psi = new ProcessStartInfo {
				FileName = FFmpegPath,
				CreateNoWindow = true,
				RedirectStandardInput = false,
				RedirectStandardOutput = true,
				WorkingDirectory = Path.GetDirectoryName(FFmpegPath)!,
				RedirectStandardError = extendedLogging,
				WindowStyle = ProcessWindowStyle.Hidden
			};

			psi.ArgumentList.Add("-hide_banner");
			psi.ArgumentList.Add("-loglevel"); psi.ArgumentList.Add(extendedLogging ? "error" : "quiet");
			psi.ArgumentList.Add("-nostdin");

			if (enableHardwareAcceleration && HardwareAccelerationMode != FFHardwareAccelerationMode.none) {
				psi.ArgumentList.Add("-hwaccel");
				psi.ArgumentList.Add(HardwareAccelerationMode.ToString());
			}

			psi.ArgumentList.Add("-ss"); psi.ArgumentList.Add(settings.Position.ToString(null, CultureInfo.InvariantCulture));
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

			if (isGrayByte) {
				string vfChain = $"scale={N}:{N}:flags=bicubic,format=gray";
				if (userVfFilter != null) vfChain = $"{userVfFilter},{vfChain}";
				psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(vfChain);
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("rawvideo");
				psi.ArgumentList.Add("-pix_fmt"); psi.ArgumentList.Add("gray");
			}
			else {
				if (settings.Fullsize != 1) {
					string vfChain = "scale=100:-1";
					if (userVfFilter != null) vfChain = $"{vfChain},{userVfFilter}";
					psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(vfChain);
				}
				else if (userVfFilter != null) {
					psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(userVfFilter);
				}
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("mjpeg");
			}

			psi.ArgumentList.Add("-frames:v"); psi.ArgumentList.Add("1");
			foreach (var item in remainingCustomArgs) psi.ArgumentList.Add(item);
			psi.ArgumentList.Add("pipe:1");

			using var process = new Process { StartInfo = psi };
			string errOut = string.Empty;
			byte[]? bytes = null;
			try {
				process.EnableRaisingEvents = true;
				process.Start();
				if (extendedLogging) {
					process.ErrorDataReceived += (sender, e) => {
						if (e.Data?.Length > 0) errOut += Environment.NewLine + e.Data;
					};
					process.BeginErrorReadLine();
				}
				using var ms = new MemoryStream();
				process.StandardOutput.BaseStream.CopyTo(ms);

				if (!process.WaitForExit(TimeoutDuration)) throw new TimeoutException($"FFmpeg timed out on file: {settings.File}");
				else if (extendedLogging) process.WaitForExit();

				if (process.ExitCode != 0) throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}");

				bytes = ms.ToArray();
				if (bytes.Length == 0) bytes = null;
				else if (isGrayByte && bytes.Length != ExpectedBytes) {
					errOut += $"{Environment.NewLine}graybytes length != {ExpectedBytes} (got {bytes.Length})";
					bytes = null;
				}
			}
			catch (Exception e) {
				errOut += $"{Environment.NewLine}{e.Message}";
				try {
					if (!process.HasExited) process.Kill();
				}
				catch { }
				bytes = null;
			}
			if (bytes == null || errOut.Length > 0) {
				string message = $"{(bytes == null ? "ERROR: Failed to retrieve" : "WARNING: Problems while retrieving")} {(isGrayByte ? "graybytes" : "thumbnail")} from: {settings.File}";
				if (extendedLogging) {
					var args = string.Join(" ", psi.ArgumentList);
					message += $":{Environment.NewLine}{FFmpegPath} {args}";
				}
				Logger.Instance.Info($"{message}{errOut}");
			}
			return bytes;
		}

		internal static bool GetGrayBytesFromVideo(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging) {
			List<GrayByteRequest> requests = GetMissingGrayByteRequests(videoFile, positions, maxSamplingDurationSeconds);
			int missingPositions = requests.Count;
			if (missingPositions == 0) return true;

			int tooDarkCounter = 0;
			List<GrayByteResult> stagedResults = new(missingPositions);
			if (UseNativeBinding && TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, stagedResults)) {
				CommitGrayByteResults(videoFile, stagedResults, ref tooDarkCounter);
				if (tooDarkCounter == missingPositions) {
					videoFile.Flags.Set(EntryFlags.TooDark);
					Logger.Instance.Info($"ERROR: Graybytes too dark of: {videoFile.Path}");
					return false;
				}
				return true;
			}

			tooDarkCounter = 0;
			HashSet<double> stagedIndexes = stagedResults.Select(result => result.Index).ToHashSet();
			foreach (GrayByteRequest request in requests) {
				if (stagedIndexes.Contains(request.Index)) continue;

				var data = GetThumbnail(new FfmpegSettings {
					File = videoFile.Path,
					Position = request.Position,
					GrayScale = 1,
				}, extendedLogging);

				if (data == null) {
					videoFile.Flags.Set(EntryFlags.ThumbnailError);
					return false;
				}
				stagedResults.Add(CreateGrayByteResult(request, data));
				stagedIndexes.Add(request.Index);
			}
			if (stagedResults.Count != missingPositions) {
				videoFile.Flags.Set(EntryFlags.ThumbnailError);
				return false;
			}

			CommitGrayByteResults(videoFile, stagedResults, ref tooDarkCounter);
			if (tooDarkCounter == missingPositions) {
				videoFile.Flags.Set(EntryFlags.TooDark);
				Logger.Instance.Info($"ERROR: Graybytes too dark of: {videoFile.Path}");
				return false;
			}
			return true;
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

		public static byte[]? ExtractThumbnailJpeg(string filePath, TimeSpan position, int maxWidth = 0, bool extendedLogging = false) {
			var settings = new FfmpegSettings {
				File = filePath,
				Position = position,
				GrayScale = 0,
				Fullsize = (byte)(maxWidth == 0 ? 1 : 0),
			};
			var raw = GetThumbnail(maxWidth == 0 ? settings : settings with { Fullsize = 1 }, extendedLogging);
			if (raw == null || raw.Length == 0) return null;

			if (maxWidth > 0) {
				using var ms = new MemoryStream(raw);
				using var image = Image.Load(ms);
				if (image.Width > maxWidth) {
					int h = (int)(image.Height * ((double)maxWidth / image.Width));
					image.Mutate(x => x.Resize(maxWidth, h));
				}
				using var outMs = new MemoryStream();
				image.Save(outMs, new SixLabors.ImageSharp.Formats.Jpeg.JpegEncoder { Quality = 90 });
				return outMs.ToArray();
			}
			return raw;
		}
	}

	internal struct FfmpegSettings {
		public byte GrayScale;
		public byte Fullsize;
		public string File;
		public TimeSpan Position;
	}
}
