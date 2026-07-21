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
	}
}
