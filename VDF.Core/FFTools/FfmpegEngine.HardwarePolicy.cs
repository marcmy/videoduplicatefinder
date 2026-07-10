using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using FFmpeg.AutoGen;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
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

		static string? NormalizeHardwareCodecName(
			string? codecName) =>
				string.IsNullOrWhiteSpace(codecName)
					? null
					: codecName.Trim().ToLowerInvariant();

		static string? NormalizeHardwareFamilyKey(
			string? familyKey) =>
				string.IsNullOrWhiteSpace(familyKey)
					? null
					: familyKey.Trim().ToLowerInvariant();

		static string? GetHardwareDecodeCapabilityKey(
			string? codecName,
			string? familyKey,
			out string description) {
			description = string.Empty;
			if (HardwareAccelerationMode ==
				FFHardwareAccelerationMode.none) {
				return null;
			}

			string? family =
				NormalizeHardwareFamilyKey(familyKey);
			if (family != null) {
				description = $"family '{family}'";
				return
					$"{HardwareAccelerationMode}|family|{family}";
			}

			string? codec =
				NormalizeHardwareCodecName(codecName);
			if (codec == null)
				return null;

			description = $"codec '{codec}'";
			return
				$"{HardwareAccelerationMode}|codec|{codec}";
		}

		static bool TryGetHardwareDecodeBypass(
			string? key,
			out string reason) {
			reason = string.Empty;
			if (key == null)
				return false;

			lock (HardwareDecodeCodecStateLock) {
				if (!HardwareDecodeCodecStatsByModeAndCodec
					.TryGetValue(
						key,
						out HardwareDecodeCodecStats? stats) ||
					!stats.Bypass) {
					return false;
				}

				reason = stats.Reason;
				return true;
			}
		}

		internal static bool ShouldBypassHardwareDecodeForCodec(
			string? codecName,
			out string reason,
			string? familyKey = null) {
			string? exactKey =
				GetHardwareDecodeCapabilityKey(
					codecName,
					familyKey,
					out _);

			if (TryGetHardwareDecodeBypass(
				exactKey,
				out reason)) {
				return true;
			}

			// A codec-only explicit capability failure applies to all of
			// that codec's families. A family-specific failure does not.
			if (!string.IsNullOrWhiteSpace(familyKey)) {
				string? codecKey =
					GetHardwareDecodeCapabilityKey(
						codecName,
						null,
						out _);

				if (TryGetHardwareDecodeBypass(
					codecKey,
					out reason)) {
					return true;
				}
			}

			reason = string.Empty;
			return false;
		}

		internal static void RecordHardwareDecodeSuccessForCodec(
			string? codecName,
			string? familyKey = null) {
			string? exactKey =
				GetHardwareDecodeCapabilityKey(
					codecName,
					familyKey,
					out _);
			string? codecKey =
				GetHardwareDecodeCapabilityKey(
					codecName,
					null,
					out _);

			lock (HardwareDecodeCodecStateLock) {
				bool cleared = false;

				if (exactKey != null)
					cleared |=
						HardwareDecodeCodecStatsByModeAndCodec
							.Remove(exactKey);

				if (codecKey != null &&
					!string.Equals(
						codecKey,
						exactKey,
						StringComparison.OrdinalIgnoreCase)) {
					cleared |=
						HardwareDecodeCodecStatsByModeAndCodec
							.Remove(codecKey);
				}

				if (cleared) {
					Logger.Instance.Info(
						$"FFmpeg hardware decode succeeded for " +
						$"codec '{NormalizeHardwareCodecName(codecName)}' " +
						$"on {HardwareAccelerationMode}; cleared a stale " +
						$"capability bypass.");
				}
			}
		}

		internal static void RecordD3D11SoftwareFrameFallbackForCodec(
			string? codecName,
			string reason,
			string? familyKey = null) {
			_ = codecName;
			_ = reason;
			_ = familyKey;

			// This is intentionally not cached or logged as a codec/family
			// capability. The caller logs the affected file and retries only
			// that operation with native CPU decode.
		}

		internal static void RecordHardwareDecodeFailureForCodec(
			string? codecName,
			string reason,
			string? familyKey = null) {
			if (!IsPersistentHardwareCodecFailure(reason))
				return;

			string? key =
				GetHardwareDecodeCapabilityKey(
					codecName,
					familyKey,
					out string description);
			if (key == null)
				return;

			string normalizedReason =
				NormalizeLogReason(reason, 240);

			lock (HardwareDecodeCodecStateLock) {
				if (HardwareDecodeCodecStatsByModeAndCodec
					.TryGetValue(
						key,
						out HardwareDecodeCodecStats? existing) &&
					existing.Bypass) {
					return;
				}

				HardwareDecodeCodecStatsByModeAndCodec[key] =
					new HardwareDecodeCodecStats {
						Bypass = true,
						Reason =
							$"{description} on " +
							$"{HardwareAccelerationMode}: " +
							normalizedReason,
					};
			}

			Logger.Instance.Info(
				$"FFmpeg hardware decode will use CPU decode for " +
				$"{description} on {HardwareAccelerationMode} for the " +
				$"rest of this session because FFmpeg explicitly " +
				$"reported an unsupported capability: " +
				$"{normalizedReason}");
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

		internal static AVHWDeviceType GetConfiguredHardwareDeviceType(bool enableHardwareAcceleration = true) {
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
			if (ShouldBypassHardwareDecodeForCodec(
				codecName,
				out _,
				familyKey))
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

			return value.Contains("doesn't support", StringComparison.Ordinal)
				|| value.Contains("does not support", StringComparison.Ordinal)
				|| value.Contains("not supported", StringComparison.Ordinal)
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

		internal static bool IsPersistentHardwareCodecFailure(
			string? text) {
			if (!IsHardwareDecodeFailure(text))
				return false;

			string value = text!.ToLowerInvariant();

			return
				value.Contains(
					"doesn't support hardware accelerated",
					StringComparison.Ordinal) ||
				value.Contains(
					"does not support hardware accelerated",
					StringComparison.Ordinal) ||
				value.Contains(
					"unsupported hardware codec",
					StringComparison.Ordinal) ||
				value.Contains(
					"unsupported codec",
					StringComparison.Ordinal) ||
				value.Contains(
					"codec is not supported",
					StringComparison.Ordinal) ||
				value.Contains(
					"codec not supported",
					StringComparison.Ordinal) ||
				value.Contains(
					"does not support this codec",
					StringComparison.Ordinal) ||
				value.Contains(
					"doesn't support this codec",
					StringComparison.Ordinal) ||
				value.Contains(
					"does not support the codec",
					StringComparison.Ordinal) ||
				value.Contains(
					"doesn't support the codec",
					StringComparison.Ordinal) ||
				value.Contains(
					"unsupported profile",
					StringComparison.Ordinal) ||
				value.Contains(
					"profile is not supported",
					StringComparison.Ordinal) ||
				value.Contains(
					"profile not supported",
					StringComparison.Ordinal) ||
				value.Contains(
					"unsupported pixel format",
					StringComparison.Ordinal) ||
				value.Contains(
					"pixel format is not supported",
					StringComparison.Ordinal) ||
				value.Contains(
					"pixel format not supported",
					StringComparison.Ordinal) ||
				value.Contains(
					"not supported by this device",
					StringComparison.Ordinal) ||
				value.Contains(
					"not supported by the device",
					StringComparison.Ordinal);
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
	}
}
