using FFmpeg.AutoGen;

namespace VDF.Core.FFTools {
	// Compatibility surface for upstream tests and safety helpers that are merged into the
	// performance branch while its specialized FfmpegEngine implementation is retained.
	internal static partial class FfmpegEngine {
		const int NativeFailureFullDetailLimit = 20;
		const int NativeFailureCompactLimit = 200;
		const int NativeFailureSummaryEvery = 100;

		internal enum NativeFailureLogMode { Full, Compact, Summary, Suppressed }

		internal static NativeFailureLogMode GetNativeFailureLogMode(int totalFailures) {
			if (totalFailures <= NativeFailureFullDetailLimit)
				return NativeFailureLogMode.Full;
			if (totalFailures <= NativeFailureCompactLimit)
				return NativeFailureLogMode.Compact;
			return totalFailures % NativeFailureSummaryEvery == 0
				? NativeFailureLogMode.Summary
				: NativeFailureLogMode.Suppressed;
		}

		internal static AVPixelFormat ResolveSourcePixelFormat(int frameFormat, AVPixelFormat openTimeFormat) =>
			frameFormat >= 0 ? (AVPixelFormat)frameFormat : openTimeFormat;

		// A corrupt/truncated software decode will fail identically through the process fallback,
		// so avoid paying another timeout. Hardware failures may be driver-specific and still
		// deserve the out-of-process retry.
		internal static bool ShouldSkipProcessRetryForCorruptFile(
			FfmpegErrorCategory category,
			AVHWDeviceType hardwareDeviceType) =>
			category == FfmpegErrorCategory.CorruptOrTruncated
			&& hardwareDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
	}
}
