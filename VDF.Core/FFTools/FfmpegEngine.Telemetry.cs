using System;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
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

		internal static bool ShouldLogNativeSuccessTiming(bool extendedLogging) =>
			extendedLogging;

		internal static bool ShouldLogGrayByteScanTelemetry(bool extendedLogging) =>
			extendedLogging;

		internal static string DescribeNativeGrayBytePathState() {
			if (!UseNativeBinding)
				return "disabled";
			if (IsNativeBindingDisabledForSessionForTests)
				return "session-disabled";
			if (!FFmpegHelper.CanLoadNativeLibraries)
				return "libraries-unavailable";
			return "available";
		}
	}
}
