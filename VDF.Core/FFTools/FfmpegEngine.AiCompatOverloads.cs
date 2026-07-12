using System.Collections.Generic;
using System.Threading;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
		// Preserve upstream's shorter call shape for callers that only provide an
		// embedding sink. The perf implementation keeps the progress callback as a
		// separate optional concern while remaining source-compatible with upstream.
		internal static bool GetGrayBytesFromVideo(
			FileEntry videoFile,
			List<float> positions,
			double maxSamplingDurationSeconds,
			bool extendedLogging,
			global::VDF.Core.AI.IEmbeddingFrameSink? embeddingSink) =>
			GetGrayBytesFromVideo(
				videoFile,
				positions,
				maxSamplingDurationSeconds,
				extendedLogging,
				onSampleComplete: null,
				embeddingSink);

		// Preserve upstream's default-cancellation call shape used by integration
		// tests and non-cancellable callers.
		internal static byte[][]? GetDenseAiFrames(
			string file,
			double intervalSeconds,
			int maxFrames,
			bool extendedLogging) =>
			GetDenseAiFrames(
				file,
				intervalSeconds,
				maxFrames,
				extendedLogging,
				CancellationToken.None);
	}
}
