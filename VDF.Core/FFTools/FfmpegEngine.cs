using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static class FfmpegEngine {
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
		const int HardwareDecodeCodecBypassMinimumFailures = 3;
		const int D3D11SoftwareFrameCodecBypassMinimumFailures = 3;
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
		const int NativeFailureThreshold = 5;
		static int NativeConsecutiveFailures;
		static int NativeDisabledForSession;
		static int VulkanNativeWarningLogged;

		static bool ShouldUseNativeBinding =>
			UseNativeBinding
			&& !IsNativeBindingDisabledForSessionForTests
			&& FFmpegHelper.CanLoadNativeLibraries;

		internal static bool ShouldAttemptNativeBinding => ShouldUseNativeBinding;

		internal static bool IsNativeBindingDisabledForSessionForTests =>
			Volatile.Read(ref NativeDisabledForSession) != 0;

		internal static void ResetNativeBindingHealthForTests() =>
			ResetNativeBindingHealth();

		static void ResetNativeBindingHealth() {
			Volatile.Write(ref NativeConsecutiveFailures, 0);
			Volatile.Write(ref NativeDisabledForSession, 0);
			Volatile.Write(ref VulkanNativeWarningLogged, 0);
		}

		static void RecordNativeSuccess() =>
			Volatile.Write(ref NativeConsecutiveFailures, 0);

		internal static bool IsNativeBindingLoadFailure(Exception e) =>
			e is NotSupportedException
			&& (e.Message.Contains("Specified method is not supported", StringComparison.OrdinalIgnoreCase)
				|| (e.StackTrace?.Contains("FFmpeg.AutoGen.DynamicallyLoadedBindings", StringComparison.Ordinal) ?? false));

		static bool IsNativeBindingInfrastructureFailure(Exception e) =>
			e is DllNotFoundException or EntryPointNotFoundException or BadImageFormatException;

		static void DisableNativeBindingForSession(string file, Exception e, string prefix) {
			if (Interlocked.Exchange(ref NativeDisabledForSession, 1) != 0)
				return;
			Logger.Instance.Info($"{prefix}; using process mode for the rest of this session. Last error on '{file}': {e.GetType().Name}: {e.Message}.{BuildNativeFailureDetail(e)} If this persists, disable 'Use native FFmpeg binding' or install matching shared FFmpeg libraries.");
		}

		internal static void RecordNativeFailure(string file, Exception e) {
			if (IsNativeBindingLoadFailure(e)) {
				DisableNativeBindingForSession(file, e, "Native FFmpeg binding could not call the loaded FFmpeg libraries");
				return;
			}
			if (!IsNativeBindingInfrastructureFailure(e))
				return;

			int failures = Interlocked.Increment(ref NativeConsecutiveFailures);
			if (failures >= NativeFailureThreshold) {
				DisableNativeBindingForSession(file, e, $"Native FFmpeg binding failed on {failures} consecutive files");
				return;
			}

			Logger.Instance.Info($"Native FFmpeg binding failure observed on '{file}' ({failures}/{NativeFailureThreshold}); keeping native enabled until repeated failures. Reason: {e.GetType().Name}: {e.Message}{BuildNativeFailureDetail(e)}");
		}


		/// <summary>
		/// Builds the extra diagnostic suffix for a native failure: FFmpeg log lines
		/// captured on this thread plus a plain-language hint about the likely cause.
		/// </summary>
		static string BuildNativeFailureDetail(Exception e) {
			string diagnostics = FfmpegLogCapture.GetRecent();
			string? hint = FfmpegErrorClassifier.Classify(
				diagnostics.Length > 0 ? $"{diagnostics} {e.Message}" : e.Message);
			string detail = string.Empty;
			if (diagnostics.Length > 0)
				detail += $" FFmpeg log: {diagnostics}.";
			if (hint != null)
				detail += $" Hint: {hint}";
			return detail;
		}

		static void LogNativeTiming(string file, TimeSpan position, bool isGrayByte, bool hwDecode, string hardwarePolicy, long openMs, long seekMs, long decodeMs, long transferMs, int hardwareTransfers, long convertMs, long copyMs, long totalMs) {
			Logger.Instance.Info($"Native FFmpeg timing on '{file}' @ {position}: mode={(isGrayByte ? "gray32" : "thumb")}, hw={(hwDecode ? "requested" : "off")}, hwPolicy={hardwarePolicy}, hwTransfers={hardwareTransfers}/1, open={openMs}ms, seek={seekMs}ms, decode={decodeMs}ms, transfer={transferMs}ms, convert={convertMs}ms, copy={copyMs}ms, total={totalMs}ms");
		}

		static void LogNativeBatchTiming(string file, string? familyKey, bool hwDecode, string hardwarePolicy, string batchMode, int samples, NativeGrayByteTiming timing, long totalMs) {
			string family = string.IsNullOrWhiteSpace(familyKey) ? "unknown" : familyKey;
			Logger.Instance.Info($"Native FFmpeg batched graybyte extraction completed for '{file}': mode={batchMode}, family={family}, hw={(hwDecode ? "requested" : "off")}, hwPolicy={hardwarePolicy}, hwTransfers={timing.HardwareTransfers}/{samples}, fullFrameTransfers={timing.FullFrameTransfers}/{samples}, tinyDownloads={timing.TinyDownloads}/{samples}, samples={samples}, queue={timing.QueueMs}ms, open={timing.OpenMs}ms, seek={timing.SeekMs}ms, decode={timing.DecodeMs}ms, transfer={timing.TransferMs}ms, filter={timing.FilterMs}ms, convert={timing.ConvertMs}ms, tinyConvert={timing.TinyConvertMs}ms, map={timing.MapMs}ms, copy={timing.CopyMs}ms, total={totalMs}ms");
		}

		static string FormatLogValue(string? value, string fallback) =>
			string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();

		internal static string FormatNativeGrayByteBatchSkippedLog(string file, string? familyKey, string nativeState, int samples) {
			string family = FormatLogValue(familyKey, "unknown");
			return $"Native FFmpeg batched graybyte extraction skipped for '{file}': native={nativeState}, family={family}, samples={samples}; using FFmpeg process per-sample path";
		}

		static void LogNativeGrayByteBatchSkipped(string file, string? familyKey, string nativeState, int samples) =>
			Logger.Instance.Info(FormatNativeGrayByteBatchSkippedLog(file, familyKey, nativeState, samples));

		internal static string FormatProcessGrayByteBatchTimingLog(string file, string? familyKey, string? codecName, string nativeState, string hardwarePolicy, int processSamples, int totalSamples, int stagedNativeSamples, long totalMs) {
			string family = FormatLogValue(familyKey, "unknown");
			string codec = FormatLogValue(codecName, "unknown");
			return $"FFmpeg process graybyte extraction completed for '{file}': mode=process-per-sample, family={family}, codec={codec}, native={nativeState}, hwPolicy={hardwarePolicy}, processSamples={processSamples}/{totalSamples}, stagedNativeSamples={stagedNativeSamples}/{totalSamples}, samples={totalSamples}, total={totalMs}ms";
		}

		static void LogProcessGrayByteBatchTiming(string file, string? familyKey, string? codecName, string nativeState, string hardwarePolicy, int processSamples, int totalSamples, int stagedNativeSamples, long totalMs) =>
			Logger.Instance.Info(FormatProcessGrayByteBatchTimingLog(file, familyKey, codecName, nativeState, hardwarePolicy, processSamples, totalSamples, stagedNativeSamples, totalMs));

		internal static string FormatCachedGrayByteScanLog(string file, string? familyKey, string? codecName, int cachedSamples, int totalSamples) {
			string family = FormatLogValue(familyKey, "unknown");
			string codec = FormatLogValue(codecName, "unknown");
			return $"FFmpeg graybyte extraction skipped for '{file}': mode=cached, family={family}, codec={codec}, cachedSamples={cachedSamples}/{totalSamples}, samples={totalSamples}";
		}

		static void LogCachedGrayByteScan(string file, string? familyKey, string? codecName, int cachedSamples, int totalSamples) =>
			Logger.Instance.Info(FormatCachedGrayByteScanLog(file, familyKey, codecName, cachedSamples, totalSamples));

		internal static string FormatProcessTimingLog(string file, TimeSpan position, bool isGrayByte, bool hardwareRequested, string hardwarePolicy, int bytes, long totalMs) =>
			$"FFmpeg process timing on '{file}' @ {position}: mode={(isGrayByte ? "gray32" : "thumb")}, hw={(hardwareRequested ? "requested" : "off")}, hwPolicy={hardwarePolicy}, bytes={bytes}, total={totalMs}ms";

		static void LogProcessTiming(string file, TimeSpan position, bool isGrayByte, bool hardwareRequested, string hardwarePolicy, int bytes, long totalMs) =>
			Logger.Instance.Info(FormatProcessTimingLog(file, position, isGrayByte, hardwareRequested, hardwarePolicy, bytes, totalMs));

		internal static bool ShouldAttemptNativeSingleFrameExtraction(FfmpegSettings settings) =>
			ShouldUseNativeBinding;

		internal static bool ShouldLogNativeSuccessTiming(bool extendedLogging) {
			_ = extendedLogging;
			return true;
		}

		internal static bool ShouldLogGrayByteScanTelemetry(bool extendedLogging) {
			_ = extendedLogging;
			return true;
		}

		internal static string DescribeNativeGrayBytePathState() {
			if (!UseNativeBinding)
				return "disabled";
			if (IsNativeBindingDisabledForSessionForTests)
				return "session-disabled";
			if (!FFmpegHelper.CanLoadNativeLibraries)
				return "libraries-unavailable";
			return "available";
		}

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
			public int ConsecutiveFailures;
			public int ConsecutiveSoftwareFrameFallbacks;
			public int HardwareSuccesses;
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

		static bool TryGetNativeGrayByteD3D11ManualMaxConcurrency(out int concurrency) {
			concurrency = 0;
			string? value = Environment.GetEnvironmentVariable(NativeGrayByteD3D11MaxConcurrencyEnvVar);
			if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
				return false;
			concurrency = Math.Clamp(parsed, 1, 16);
			return true;
		}

		static int GetEffectiveScanMaxDegreeOfParallelism() =>
			ScanMaxDegreeOfParallelism > 0 ? ScanMaxDegreeOfParallelism : Environment.ProcessorCount;

		static int GetNativeGrayByteD3D11AutoMaxConcurrency() =>
			Math.Clamp(Math.Min(GetEffectiveScanMaxDegreeOfParallelism(), NativeGrayByteD3D11AutoMaxConcurrency), 1, 16);

		static int GetNativeGrayByteD3D11InitialConcurrency() {
			if (TryGetNativeGrayByteD3D11ManualMaxConcurrency(out int manualConcurrency))
				return manualConcurrency;
			return Math.Clamp(Math.Min(GetEffectiveScanMaxDegreeOfParallelism(), NativeGrayByteD3D11AutoInitialConcurrency), 1, GetNativeGrayByteD3D11AutoMaxConcurrency());
		}

		internal static void ConfigureNativeGrayByteD3D11Concurrency() {
			lock (D3D11GrayByteConcurrencyLock) {
				D3D11GrayByteCurrentConcurrencyLimit = GetNativeGrayByteD3D11InitialConcurrency();
				D3D11GrayByteTuningObservations = 0;
				D3D11GrayByteTuningQueueMs = 0;
				D3D11GrayByteTuningDecodeMs = 0;
				D3D11GrayByteTuningDecodeSpikeObservations = 0;
				Monitor.PulseAll(D3D11GrayByteConcurrencyLock);
			}
		}

		static IDisposable? EnterD3D11GrayByteConcurrencyLimiter(AVHWDeviceType deviceType) {
			if (deviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA)
				return null;
			lock (D3D11GrayByteConcurrencyLock) {
				while (D3D11GrayByteActiveConcurrency >= D3D11GrayByteCurrentConcurrencyLimit)
					Monitor.Wait(D3D11GrayByteConcurrencyLock);
				D3D11GrayByteActiveConcurrency++;
			}
			return new SemaphoreLease(ExitD3D11GrayByteConcurrencyLimiter);
		}

		static void ExitD3D11GrayByteConcurrencyLimiter() {
			lock (D3D11GrayByteConcurrencyLock) {
				D3D11GrayByteActiveConcurrency = Math.Max(0, D3D11GrayByteActiveConcurrency - 1);
				Monitor.PulseAll(D3D11GrayByteConcurrencyLock);
			}
		}

		static int CalculateNativeGrayByteD3D11AutoConcurrency(int oldLimit, int maxLimit, long averageQueueMs, long averageDecodeMs, int decodeSpikes, int observations) {
			int sustainedDecodeSpikeThreshold = Math.Max(2, (observations + 1) / 2);
			bool decodePressure = averageDecodeMs >= NativeGrayByteD3D11AutoDecodeHighMs || decodeSpikes >= sustainedDecodeSpikeThreshold;
			bool queuePressure = averageQueueMs >= NativeGrayByteD3D11AutoQueueHighMs
				&& averageDecodeMs < NativeGrayByteD3D11AutoDecodeHighMs / 2
				&& decodeSpikes == 0;

			int newLimit = oldLimit;
			if (decodePressure && oldLimit > 1)
				newLimit = oldLimit - 1;
			else if (queuePressure && oldLimit < maxLimit)
				newLimit = oldLimit + 1;

			return Math.Clamp(newLimit, 1, maxLimit);
		}

		internal static int CalculateNativeGrayByteD3D11AutoConcurrencyForTests(int oldLimit, int maxLimit, long averageQueueMs, long averageDecodeMs, int decodeSpikes, int observations) =>
			CalculateNativeGrayByteD3D11AutoConcurrency(oldLimit, maxLimit, averageQueueMs, averageDecodeMs, decodeSpikes, observations);

		static void ObserveD3D11GrayByteConcurrency(NativeGrayByteTiming timing) {
			if (timing.TinyDownloads <= 0 || timing.SampledFrames <= 0)
				return;
			if (TryGetNativeGrayByteD3D11ManualMaxConcurrency(out _))
				return;
			lock (D3D11GrayByteConcurrencyLock) {
				D3D11GrayByteTuningObservations++;
				D3D11GrayByteTuningQueueMs += timing.QueueMs;
				D3D11GrayByteTuningDecodeMs += timing.DecodeMs;
				if (timing.DecodeMs >= NativeGrayByteD3D11AutoDecodeHighMs)
					D3D11GrayByteTuningDecodeSpikeObservations++;
				if (D3D11GrayByteTuningObservations < NativeGrayByteD3D11AutoTuneObservationWindow)
					return;

				int observations = D3D11GrayByteTuningObservations;
				long averageQueueMs = D3D11GrayByteTuningQueueMs / Math.Max(1, observations);
				long averageDecodeMs = D3D11GrayByteTuningDecodeMs / Math.Max(1, observations);
				int decodeSpikes = D3D11GrayByteTuningDecodeSpikeObservations;
				int oldLimit = D3D11GrayByteCurrentConcurrencyLimit;
				int maxLimit = GetNativeGrayByteD3D11AutoMaxConcurrency();
				int newLimit = CalculateNativeGrayByteD3D11AutoConcurrency(oldLimit, maxLimit, averageQueueMs, averageDecodeMs, decodeSpikes, observations);

				D3D11GrayByteTuningObservations = 0;
				D3D11GrayByteTuningQueueMs = 0;
				D3D11GrayByteTuningDecodeMs = 0;
				D3D11GrayByteTuningDecodeSpikeObservations = 0;

				if (newLimit == oldLimit)
					return;

				D3D11GrayByteCurrentConcurrencyLimit = Math.Clamp(newLimit, 1, maxLimit);
				Monitor.PulseAll(D3D11GrayByteConcurrencyLock);
				Logger.Instance.Info($"Native FFmpeg D3D11 graybyte auto concurrency changed from {oldLimit} to {D3D11GrayByteCurrentConcurrencyLimit}: avgQueue={averageQueueMs}ms, avgDecode={averageDecodeMs}ms, decodeSpikes={decodeSpikes}/{observations}, scanMax={GetEffectiveScanMaxDegreeOfParallelism()}, autoMax={maxLimit}. Set {NativeGrayByteD3D11MaxConcurrencyEnvVar}=N to override.");
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
		static bool IsD3D11GrayByteCpuProbeEnabled() =>
			IsEnvFlagEnabled(EnableNativeGrayByteD3D11CpuProbeEnvVar)
			&& !IsEnvFlagEnabled(DisableNativeGrayByteD3D11AdaptiveEnvVar);

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

		static string? GetPrimaryVideoCodecName(FileEntry videoFile) {
			MediaInfo.StreamInfo? stream = GetPrimaryVideoStream(videoFile);
			return string.IsNullOrWhiteSpace(stream?.CodecName) ? null : stream.CodecName.Trim();
		}

		static string? NormalizeHardwareCodecName(string? codecName) =>
			string.IsNullOrWhiteSpace(codecName) ? null : codecName.Trim().ToLowerInvariant();

		static string? GetHardwareDecodeCodecKey(string? codecName) {
			string? codec = NormalizeHardwareCodecName(codecName);
			if (codec == null || HardwareAccelerationMode == FFHardwareAccelerationMode.none)
				return null;
			return $"{HardwareAccelerationMode}|{codec}";
		}

		internal static bool ShouldBypassHardwareDecodeForCodec(string? codecName, out string reason) {
			reason = string.Empty;
			string? key = GetHardwareDecodeCodecKey(codecName);
			if (key == null)
				return false;
			lock (HardwareDecodeCodecStateLock) {
				if (!HardwareDecodeCodecStatsByModeAndCodec.TryGetValue(key, out HardwareDecodeCodecStats? stats) || !stats.Bypass)
					return false;
				reason = stats.Reason;
				return true;
			}
		}

		internal static void RecordHardwareDecodeSuccessForCodec(string? codecName) {
			string? key = GetHardwareDecodeCodecKey(codecName);
			if (key == null)
				return;
			lock (HardwareDecodeCodecStateLock) {
				if (!HardwareDecodeCodecStatsByModeAndCodec.TryGetValue(key, out HardwareDecodeCodecStats? stats)) {
					stats = new HardwareDecodeCodecStats();
					HardwareDecodeCodecStatsByModeAndCodec.Add(key, stats);
				}
				if (!stats.Bypass) {
					stats.ConsecutiveFailures = 0;
					stats.ConsecutiveSoftwareFrameFallbacks = 0;
					stats.HardwareSuccesses++;
				}
			}
		}

		internal static void RecordD3D11SoftwareFrameFallbackForCodec(string? codecName, string reason) {
			string? codec = NormalizeHardwareCodecName(codecName);
			string? key = GetHardwareDecodeCodecKey(codec);
			if (key == null)
				return;

			lock (HardwareDecodeCodecStateLock) {
				if (!HardwareDecodeCodecStatsByModeAndCodec.TryGetValue(key, out HardwareDecodeCodecStats? stats)) {
					stats = new HardwareDecodeCodecStats();
					HardwareDecodeCodecStatsByModeAndCodec.Add(key, stats);
				}
				if (stats.Bypass || stats.HardwareSuccesses > 0)
					return;

				stats.ConsecutiveSoftwareFrameFallbacks++;
				string normalizedReason = NormalizeLogReason(reason, 240);
				if (stats.ConsecutiveSoftwareFrameFallbacks < D3D11SoftwareFrameCodecBypassMinimumFailures) {
					Logger.Instance.Info($"FFmpeg D3D11 software-frame fallback observed for codec '{codec}' on {HardwareAccelerationMode} ({stats.ConsecutiveSoftwareFrameFallbacks}/{D3D11SoftwareFrameCodecBypassMinimumFailures}); keeping hardware enabled for this codec until repeated software-frame fallbacks and no hardware success. Reason: {normalizedReason}");
					return;
				}

				stats.Bypass = true;
				stats.Reason = $"codec '{codec}' on {HardwareAccelerationMode}: repeated D3D11 software-frame fallbacks with no hardware decode success: {normalizedReason}";
				Logger.Instance.Info($"FFmpeg hardware decode will use CPU decode for codec '{codec}' on {HardwareAccelerationMode} for the rest of this session after {stats.ConsecutiveSoftwareFrameFallbacks} repeated D3D11 software-frame fallback(s) and no hardware decode success: {normalizedReason}");
			}
		}

		internal static void RecordHardwareDecodeFailureForCodec(string? codecName, string reason) {
			if (!IsPersistentHardwareCodecFailure(reason))
				return;

			string? codec = NormalizeHardwareCodecName(codecName);
			string? key = GetHardwareDecodeCodecKey(codec);
			if (key == null)
				return;
			lock (HardwareDecodeCodecStateLock) {
				if (!HardwareDecodeCodecStatsByModeAndCodec.TryGetValue(key, out HardwareDecodeCodecStats? stats)) {
					stats = new HardwareDecodeCodecStats();
					HardwareDecodeCodecStatsByModeAndCodec.Add(key, stats);
				}
				if (stats.Bypass)
					return;

				stats.ConsecutiveFailures++;
				string normalizedReason = NormalizeLogReason(reason, 240);
				if (stats.ConsecutiveFailures < HardwareDecodeCodecBypassMinimumFailures) {
					Logger.Instance.Info($"FFmpeg hardware codec support failure observed for codec '{codec}' on {HardwareAccelerationMode} ({stats.ConsecutiveFailures}/{HardwareDecodeCodecBypassMinimumFailures}); keeping hardware enabled for this codec until repeated support failures. Reason: {normalizedReason}");
					return;
				}

				stats.Bypass = true;
				stats.Reason = $"codec '{codec}' on {HardwareAccelerationMode}: {normalizedReason}";
				Logger.Instance.Info($"FFmpeg hardware decode will use CPU decode for codec '{codec}' on {HardwareAccelerationMode} for the rest of this session after {stats.ConsecutiveFailures} repeated codec support failure(s): {normalizedReason}");
			}
		}

		static bool ShouldBypassGrayByteHardwareForFamily(string? familyKey) {
			if (string.IsNullOrWhiteSpace(familyKey) || IsEnvFlagEnabled(DisableNativeGrayByteD3D11AdaptiveEnvVar))
				return false;
			lock (D3D11GrayByteAdaptiveStateLock) {
				return D3D11GrayByteAdaptiveStatsByFamily.TryGetValue(familyKey, out D3D11GrayByteAdaptiveStats? stats) && stats.Bypass;
			}
		}

		internal static bool IsConfiguredHardwareDecodeBypassed(out string reason) {
			// Hardware decode failures stay file-scoped; a bad file must not turn off
			// the configured accelerator for the rest of the scan.
			reason = string.Empty;
			return false;
		}

		internal static void ResetConfiguredHardwareDecodeAdaptiveStateForTests() {
			lock (HardwareDecodeCodecStateLock) {
				HardwareDecodeCodecStatsByModeAndCodec.Clear();
			}
		}

		internal static void MarkConfiguredHardwareDecodeFailure(string reason) {
			_ = reason;
		}

		static bool ShouldBypassD3D11GrayByteForFamily(FileEntry videoFile, out string familyKey) {
			familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile) ?? string.Empty;
			return ShouldBypassGrayByteHardwareForFamily(familyKey);
		}

		static bool ShouldProbeD3D11GrayByteFamilyWithCpu(FileEntry videoFile, out string familyKey) {
			familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile) ?? string.Empty;
			if (familyKey.Length == 0 || !IsD3D11GrayByteCpuProbeEnabled())
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
					Logger.Instance.Info($"Native FFmpeg D3D11 graybyte adaptive policy will use native CPU decode for family '{familyKey}' after CPU probe won: d3d11={d3d11TotalMs}ms, cpu={cpuTotalMs}ms. This opt-in policy is controlled by {EnableNativeGrayByteD3D11CpuProbeEnvVar}=1; set {DisableNativeGrayByteD3D11AdaptiveEnvVar}=1 to disable all D3D11 adaptive family caching.");
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
			if (familyKey == null || !IsD3D11GrayByteCpuProbeEnabled())
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

		static string GetHardwarePolicy(AVHWDeviceType deviceType, bool enableHardwareAcceleration) {
			if (!enableHardwareAcceleration)
				return "disabled-for-call";
			return deviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_NONE ? "configured-off" : "requested";
		}

		static AVHWDeviceType GetConfiguredHardwareDeviceType(bool enableHardwareAcceleration = true) {
			if (!enableHardwareAcceleration)
				return AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;

			// Vulkan through the in-process native binding can segfault the app on
			// some drivers (#799). Keep Vulkan available to the isolated FFmpeg
			// process path, but force software decode for native calls.
			if (HardwareAccelerationMode == FFHardwareAccelerationMode.vulkan) {
				if (Interlocked.Exchange(ref VulkanNativeWarningLogged, 1) == 0) {
					Logger.Instance.Info(
						"Vulkan hardware acceleration is not supported with the native FFmpeg binding " +
						"(it crashes the process on some drivers, #799); decoding in software instead. " +
						"Disable 'Use native FFmpeg binding' to run Vulkan via the CLI, or pick another " +
						"hardware acceleration mode such as 'cuda'.");
				}
				return AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			}

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

		static bool ShouldUseProcessHardwareAccelerationForGrayBytes(out string hardwarePolicy) {
			hardwarePolicy = "requested";
			if (HardwareAccelerationMode == FFHardwareAccelerationMode.none) {
				hardwarePolicy = "configured-off";
				return false;
			}

			if (IsEnvFlagEnabled(ForceNativeGrayByteCpuEnvVar)) {
				hardwarePolicy = $"forced-cpu-by-{ForceNativeGrayByteCpuEnvVar}";
				return false;
			}

			return true;
		}

		static string DescribeProcessGrayByteHardwarePolicy(string? familyKey, string? codecName) {
			if (ShouldBypassHardwareDecodeForCodec(codecName, out _))
				return "hardware-decode-codec-bypass";
			if (ShouldBypassGrayByteHardwareForFamily(familyKey))
				return "hardware-decode-failure-cpu-family-bypass";
			ShouldUseProcessHardwareAccelerationForGrayBytes(out string hardwarePolicy);
			return hardwarePolicy;
		}

		internal static bool IsHardwareDecodeFailure(string? text) {
			if (string.IsNullOrWhiteSpace(text))
				return false;

			string value = text.ToLowerInvariant();
			bool hasHardwareContext =
				value.Contains("hwaccel", StringComparison.Ordinal)
				|| value.Contains("hardware", StringComparison.Ordinal)
				|| value.Contains("hwdevice", StringComparison.Ordinal)
				|| value.Contains("hw device", StringComparison.Ordinal)
				|| value.Contains("d3d11", StringComparison.Ordinal)
				|| value.Contains("dxva", StringComparison.Ordinal)
				|| value.Contains("vaapi", StringComparison.Ordinal)
				|| value.Contains("cuda", StringComparison.Ordinal)
				|| value.Contains("qsv", StringComparison.Ordinal)
				|| value.Contains("vdpau", StringComparison.Ordinal)
				|| value.Contains("videotoolbox", StringComparison.Ordinal)
				|| value.Contains("vulkan", StringComparison.Ordinal)
				|| value.Contains("av_hwdevice_ctx_create", StringComparison.Ordinal)
				|| value.Contains("av_hwframe_transfer_data", StringComparison.Ordinal);
			if (!hasHardwareContext)
				return false;

			return value.Contains("not supported", StringComparison.Ordinal)
				|| value.Contains("unsupported", StringComparison.Ordinal)
				|| value.Contains("failed", StringComparison.Ordinal)
				|| value.Contains("failure", StringComparison.Ordinal)
				|| value.Contains("could not", StringComparison.Ordinal)
				|| value.Contains("cannot", StringComparison.Ordinal)
				|| value.Contains("no device", StringComparison.Ordinal)
				|| value.Contains("device setup failed", StringComparison.Ordinal)
				|| value.Contains("function not implemented", StringComparison.Ordinal)
				|| value.Contains("not implemented", StringComparison.Ordinal)
				|| value.Contains("error", StringComparison.Ordinal);
		}

		internal static bool IsPersistentHardwareCodecFailure(string? text) {
			if (!IsHardwareDecodeFailure(text))
				return false;

			string value = text!.ToLowerInvariant();
			return value.Contains("doesn't support hardware", StringComparison.Ordinal)
				|| value.Contains("does not support hardware", StringComparison.Ordinal)
				|| value.Contains("not supported", StringComparison.Ordinal)
				|| value.Contains("unsupported", StringComparison.Ordinal)
				|| value.Contains("failed setup for format", StringComparison.Ordinal)
				|| value.Contains("hwaccel initialisation", StringComparison.Ordinal)
				|| value.Contains("hwaccel initialization", StringComparison.Ordinal)
				|| value.Contains("no device available for decoder", StringComparison.Ordinal)
				|| value.Contains("device setup failed for decoder", StringComparison.Ordinal)
				|| value.Contains("av_hwdevice_ctx_create", StringComparison.Ordinal)
				|| value.Contains("failed to create", StringComparison.Ordinal)
				|| value.Contains("function not implemented", StringComparison.Ordinal)
				|| value.Contains("not implemented", StringComparison.Ordinal);
		}

		static bool ShouldUseD3D11GrayByteGpuScale(AVHWDeviceType deviceType, ref string hardwarePolicy, out string unavailableReason) {
			unavailableReason = string.Empty;
			if (deviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA)
				return false;

			if (IsEnvFlagEnabled(DisableNativeGrayByteGpuScaleEnvVar)) {
				unavailableReason = $"disabled by {DisableNativeGrayByteGpuScaleEnvVar}";
				hardwarePolicy = $"requested-gpu-scale-disabled-by-{DisableNativeGrayByteGpuScaleEnvVar}";
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

		static unsafe bool TryGetGrayBytesFromVideoNativeBatch(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging, List<GrayByteResult> results, bool allowD3D11GpuScale = true, bool forceCpuDecode = false, string? forcedCpuPolicy = null) {
			int requestedSamples = 0;
			string hardwarePolicy = "unresolved";
			string? familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);
			string? codecName = GetPrimaryVideoCodecName(videoFile);
			AVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
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
					if (hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && ShouldBypassHardwareDecodeForCodec(codecName, out _)) {
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
					RecordHardwareDecodeSuccessForCodec(codecName);
				if (useD3D11GpuScale && vsd.IsHardwareDecode) {
					long d3d11TotalMs = batchSw.ElapsedMilliseconds;
					ObserveD3D11GrayByteConcurrency(nativeTiming);
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
				RecordNativeSuccess();
				return true;
			}
			catch (Exception e) {
				if (IsNativeBindingLoadFailure(e)) {
					RecordNativeFailure(videoFile.Path, e);
					return false;
				}
				if (e is D3D11SoftwareFrameFallbackException && !forceCpuDecode) {
					RecordD3D11SoftwareFrameFallbackForCodec(codecName, e.Message);
					Logger.Instance.Info($"Native FFmpeg graybyte extraction detected software frames under D3D11 on '{videoFile.Path}', retrying native batch with CPU decode. Staged {results.Count} of {requestedSamples} sample(s). Reason: {NormalizeLogReason(e.Message, 240)}");
					results.Clear();
					return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, allowD3D11GpuScale: false, forceCpuDecode: true);
				}
				string failureText = $"{hardwarePolicy} {hardwareDeviceType} {e}";
				if (!forceCpuDecode && hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && IsHardwareDecodeFailure(failureText)) {
					MarkConfiguredHardwareDecodeFailure(failureText);
					RecordHardwareDecodeFailureForCodec(codecName, failureText);
					Logger.Instance.Info($"Native FFmpeg graybyte extraction hit a hardware decode failure on '{videoFile.Path}', retrying native batch with CPU decode. hwPolicy={hardwarePolicy}. Staged {results.Count} of {requestedSamples} sample(s). Reason: {NormalizeLogReason(e.Message, 240)}");
					results.Clear();
					return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, results, allowD3D11GpuScale: false, forceCpuDecode: true, forcedCpuPolicy: "hardware-decode-failure-cpu-retry");
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
				AVHWDeviceType hardwareDeviceType = ShouldBypassHardwareDecodeForCodec(hardwareCodecName, out _)
					? AVHWDeviceType.AV_HWDEVICE_TYPE_NONE
					: GetConfiguredHardwareDeviceType();
				try {
					FfmpegLogCapture.Reset();
					using var vsd = new VideoStreamDecoder(filePath, hardwareDeviceType);
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
							AVPixelFormat srcPixFmt = vsd.IsHardwareDecode ? (AVPixelFormat)srcFrame.format : vsd.PixelFormat;
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
					if (IsNativeBindingLoadFailure(e))
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
					HardwareCodecName = hardwareCodecName
				}, extendedLogging);
			}
			return frames;
		}

		public static unsafe byte[]? GetThumbnail(FfmpegSettings settings, bool extendedLogging) {
			const int N = 32;
			const int ExpectedBytes = N * N;
			bool isGrayByte = settings.GrayScale == 1;
			string hardwarePolicy = "unresolved";
			bool bypassHardwareForFamily = isGrayByte && ShouldBypassGrayByteHardwareForFamily(settings.HardwareFamilyKey);
			bool bypassHardwareForCodec = ShouldBypassHardwareDecodeForCodec(settings.HardwareCodecName, out _);
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
				if (ShouldAttemptNativeSingleFrameExtraction(settings)) {
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

					AVRational sampleAspectRatio = srcFrame.sample_aspect_ratio;
					if (sampleAspectRatio.num <= 0 || sampleAspectRatio.den <= 0)
						sampleAspectRatio = vsd.StreamSampleAspectRatio;
					Size displaySize = isGrayByte
						? sourceSize
						: GetDisplaySizeForSampleAspectRatio(
							sourceSize, sampleAspectRatio.num, sampleAspectRatio.den);
					Size destinationSize = isGrayByte
						? new Size(N, N)
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
						byte[] outBuf = ExtractGray32FromFrame(convertedFrame);
						copyMs = phaseSw.ElapsedMilliseconds;
						if (ShouldLogNativeSuccessTiming(extendedLogging))
							LogNativeTiming(settings.File, settings.Position, true, vsd.IsHardwareDecode, hardwarePolicy, openMs, seekMs, decodeMs, transferMs, hardwareTransfers, convertMs, copyMs, totalSw.ElapsedMilliseconds);
						if (nativeHardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && vsd.IsHardwareDecode)
							RecordHardwareDecodeSuccessForCodec(settings.HardwareCodecName);
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
							RecordHardwareDecodeSuccessForCodec(settings.HardwareCodecName);
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
					RecordHardwareDecodeFailureForCodec(settings.HardwareCodecName, failureText);
					Logger.Instance.Info($"Native FFmpeg extraction hit a hardware decode failure on '{settings.File}', retrying with CPU decode. hwPolicy={hardwarePolicy}. Reason: {NormalizeLogReason(e.Message, 240)}");
					return GetThumbnail(settings with { ForceCpuDecode = true }, extendedLogging);
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

			if (isGrayByte) {
				string vfChain = $"scale={N}:{N}:flags=bicubic,format=gray";
				if (userVfFilter != null) vfChain = $"{userVfFilter},{vfChain}";
				psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(vfChain);
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("rawvideo");
				psi.ArgumentList.Add("-pix_fmt"); psi.ArgumentList.Add("gray");
			}
			else {
				string? vfChain = null;
				if (!FileUtils.IsImageFile(settings.File)) {
					// Convert the coded raster to square pixels before encoding the JPEG.
					// This is the equivalent of a media player's display-size correction.
					vfChain = "scale=trunc(iw*if(eq(sar\\,0)\\,1\\,sar)):ih,setsar=1";
				}

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
			foreach (var item in remainingCustomArgs) psi.ArgumentList.Add(item);
			psi.ArgumentList.Add("pipe:1");

			var processSw = Stopwatch.StartNew();
			using var process = new Process { StartInfo = psi };
			var errOut = new FfmpegErrorAccumulator();
			byte[]? bytes = null;
			try {
				process.EnableRaisingEvents = true;
				process.Start();
				process.ErrorDataReceived += new DataReceivedEventHandler((sender, e) => {
					errOut.AppendLine(e.Data);
				});
				process.BeginErrorReadLine();
				using var ms = new MemoryStream();
				process.StandardOutput.BaseStream.CopyTo(ms);

				if (!process.WaitForExit(TimeoutDuration)) {
					throw new TimeoutException($"FFmpeg timed out on file: {settings.File}");
				}
				else
					process.WaitForExit(); // Because of asynchronous event handlers, see: https://github.com/dotnet/runtime/issues/18789

				if (process.ExitCode != 0) throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}");

				bytes = ms.ToArray();
				if (bytes.Length == 0) bytes = null;
				else if (isGrayByte && bytes.Length != ExpectedBytes) {
					errOut.AppendLine($"graybytes length != {ExpectedBytes} (got {bytes.Length})");
					bytes = null;
				}
				if (bytes != null && processAttemptedHardware)
					RecordHardwareDecodeSuccessForCodec(settings.HardwareCodecName);
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
					RecordHardwareDecodeFailureForCodec(settings.HardwareCodecName, ffmpegError);
				}
				if (!settings.ForceCpuDecode && processHardwareFailure && bytes == null) {
					Logger.Instance.Info($"FFmpeg process extraction hit a hardware decode failure on '{settings.File}', retrying with CPU decode. hwPolicy={hardwarePolicy}. Reason: {NormalizeLogReason(ffmpegError, 240)}");
					return GetThumbnail(settings with { ForceCpuDecode = true }, extendedLogging);
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
			if (nativeGrayByteState == "available" && TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, extendedLogging, stagedResults)) {
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
		/// <summary>
		/// Extracts 32x32 grayscale frames for arbitrary timestamps. Native mode keeps
		/// one decoder open for the batch; process mode fills any remaining positions.
		/// </summary>
		public static List<byte[]?> ExtractGrayFrames(
			string filePath,
			IReadOnlyList<TimeSpan> positions,
			bool extendedLogging = false,
			string? hardwareCodecName = null) {
			var frames = new byte[]?[positions.Count];
			if (positions.Count == 0)
				return frames.ToList();

			bool forceCpuForRemaining = false;
			if (ShouldUseNativeBinding) {
				TryExtractGrayFramesNative(
					filePath,
					positions,
					extendedLogging,
					hardwareCodecName,
					frames,
					ref forceCpuForRemaining);
			}

			for (int i = 0; i < positions.Count; i++) {
				frames[i] ??= GetThumbnail(new FfmpegSettings {
					File = filePath,
					Position = positions[i],
					GrayScale = 1,
					HardwareCodecName = hardwareCodecName,
					ForceCpuDecode = forceCpuForRemaining,
				}, extendedLogging);
			}

			return frames.ToList();
		}

		static unsafe bool TryExtractGrayFramesNative(
			string filePath,
			IReadOnlyList<TimeSpan> positions,
			bool extendedLogging,
			string? hardwareCodecName,
			byte[]?[] frames,
			ref bool forceCpuForRemaining,
			bool forceCpuDecode = false) {
			string hardwarePolicy =
				forceCpuDecode
					? "hardware-decode-failure-cpu-retry"
					: "unresolved";
			AVHWDeviceType hardwareDeviceType =
				AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;

			try {
				bool bypassHardwareForCodec =
					ShouldBypassHardwareDecodeForCodec(
						hardwareCodecName,
						out _);
				bool enableHardwareAcceleration =
					!forceCpuDecode && !bypassHardwareForCodec;

				hardwareDeviceType =
					GetConfiguredHardwareDeviceType(
						enableHardwareAcceleration);
				hardwarePolicy = forceCpuDecode
					? "hardware-decode-failure-cpu-retry"
					: bypassHardwareForCodec
						? "hardware-decode-codec-bypass"
						: GetHardwarePolicy(
							hardwareDeviceType,
							enableHardwareAcceleration);

				var openSw = Stopwatch.StartNew();
				FfmpegLogCapture.Reset();
				using var decoder =
					new VideoStreamDecoder(filePath, hardwareDeviceType);
				long openMs = openSw.ElapsedMilliseconds;

				VideoFrameConverter? converter = null;
				Size converterSourceSize = default;
				AVPixelFormat converterSourcePixelFormat =
					AVPixelFormat.AV_PIX_FMT_NONE;
				bool anySuccess = false;

				try {
					for (int i = 0; i < positions.Count; i++) {
						var totalSw = Stopwatch.StartNew();
						long openForSampleMs = i == 0 ? openMs : 0;

						if (!decoder.TryDecodeFrame(
							out var sourceFrame,
							positions[i],
							out DecodedFrameTiming frameTiming)) {
							if (!forceCpuDecode &&
								hardwareDeviceType !=
									AVHWDeviceType.AV_HWDEVICE_TYPE_NONE) {
								throw new Exception(
									$"TryDecodeFrame failed at pos={positions[i]} " +
									$"for '{filePath}'. " +
									$"size={decoder.FrameSize.Width}x" +
									$"{decoder.FrameSize.Height}");
							}
							continue;
						}

						byte[] gray = ExtractGrayBytesFromFrame(
							decoder,
							sourceFrame,
							ref converter,
							ref converterSourceSize,
							ref converterSourcePixelFormat,
							out long convertMs,
							out long copyMs);

						frames[i] = gray;
						anySuccess = true;

						if (ShouldLogNativeSuccessTiming(extendedLogging)) {
							LogNativeTiming(
								filePath,
								positions[i],
								true,
								decoder.IsHardwareDecode,
								hardwarePolicy,
								openForSampleMs,
								frameTiming.SeekMs,
								frameTiming.DecodeMs,
								frameTiming.TransferMs,
								frameTiming.HardwareTransfers,
								convertMs,
								copyMs,
								totalSw.ElapsedMilliseconds + openForSampleMs);
						}
					}
				}
				finally {
					converter?.Dispose();
				}

				if (hardwareDeviceType !=
						AVHWDeviceType.AV_HWDEVICE_TYPE_NONE &&
					decoder.IsHardwareDecode) {
					RecordHardwareDecodeSuccessForCodec(
						hardwareCodecName);
				}

				if (anySuccess)
					RecordNativeSuccess();

				return anySuccess;
			}
			catch (Exception e) {
				string failureText =
					$"{hardwarePolicy} {hardwareDeviceType} {e}";

				if (IsNativeBindingLoadFailure(e)) {
					RecordNativeFailure(filePath, e);
					return false;
				}

				if (!forceCpuDecode &&
					hardwareDeviceType !=
						AVHWDeviceType.AV_HWDEVICE_TYPE_NONE &&
					IsHardwareDecodeFailure(failureText)) {
					forceCpuForRemaining = true;
					MarkConfiguredHardwareDecodeFailure(failureText);
					RecordHardwareDecodeFailureForCodec(
						hardwareCodecName,
						failureText);

					Logger.Instance.Info(
						$"Native FFmpeg batched alignment extraction hit " +
						$"a hardware decode failure on '{filePath}', " +
						$"retrying with CPU decode. " +
						$"hwPolicy={hardwarePolicy}. Reason: " +
						$"{NormalizeLogReason(e.Message, 240)}");

					return TryExtractGrayFramesNative(
						filePath,
						positions,
						extendedLogging,
						hardwareCodecName,
						frames,
						ref forceCpuForRemaining,
						forceCpuDecode: true);
				}

				Logger.Instance.Info(
					$"Native FFmpeg batched alignment extraction failed " +
					$"on '{filePath}', falling back to process mode. " +
					$"hwPolicy={hardwarePolicy}. Reason: " +
					$"{NormalizeLogReason(e.Message, 240)}");
				return false;
			}
		}
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
			if (!double.IsFinite(exactWidth) || exactWidth <= 0 || exactWidth >= int.MaxValue)
				return source;

			return new Size(Math.Max(1, (int)Math.Floor(exactWidth)), source.Height);
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
	}
}
