// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Numerics;
using System.Linq;

namespace VDF.Core.FFTools {
	internal readonly record struct FrameAlignmentCandidate(
		int Offset,
		byte[]? GrayBytes);

	internal readonly record struct FrameAlignmentResult(
		int Offset,
		int HashDistance,
		double MeanAbsoluteDifference);

	internal static class FrameAlignment {
		const double MeanDifferenceTieTolerance = 0.0005d;

		internal static FrameAlignmentResult? FindBest(
			byte[] reference,
			IEnumerable<FrameAlignmentCandidate> candidates) {
			ArgumentNullException.ThrowIfNull(reference);
			if (reference.Length != 32 * 32) {
				throw new ArgumentException(
					"Reference frame must contain exactly 32x32 grayscale bytes.",
					nameof(reference));
			}

			ulong referenceHash =
				pHash.PerceptualHash.ComputePHashFromGray32x32(reference);
			FrameAlignmentResult? best = null;

			foreach (FrameAlignmentCandidate candidate in candidates) {
				byte[]? gray = candidate.GrayBytes;
				if (gray == null || gray.Length != reference.Length)
					continue;

				ulong candidateHash =
					pHash.PerceptualHash.ComputePHashFromGray32x32(gray);
				int hashDistance =
					BitOperations.PopCount(referenceHash ^ candidateHash);

				long absoluteDifference = 0;
				for (int i = 0; i < reference.Length; i++)
					absoluteDifference += Math.Abs(reference[i] - gray[i]);

				double meanDifference =
					absoluteDifference / (255d * reference.Length);

				var result = new FrameAlignmentResult(
					candidate.Offset,
					hashDistance,
					meanDifference);

				if (best == null || IsBetter(result, best.Value))
					best = result;
			}

			return best;
		}

		static bool IsBetter(
			FrameAlignmentResult candidate,
			FrameAlignmentResult current) {
			if (candidate.HashDistance != current.HashDistance)
				return candidate.HashDistance < current.HashDistance;

			double improvement =
				current.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			if (improvement > MeanDifferenceTieTolerance)
				return true;
			if (improvement < -MeanDifferenceTieTolerance)
				return false;

			int candidateDistance = Math.Abs(candidate.Offset);
			int currentDistance = Math.Abs(current.Offset);
			if (candidateDistance != currentDistance)
				return candidateDistance < currentDistance;

			return candidate.Offset < current.Offset;
		}

		internal static int CalculateCoarseStep(int radius) =>
			Math.Max(1, Math.Clamp(radius, 1, 300) / 15);

		internal static IReadOnlyList<int> BuildCoarseOffsets(int radius) {
			radius = Math.Clamp(radius, 1, 300);
			int step = CalculateCoarseStep(radius);
			var offsets = new SortedSet<int> { -radius, 0, radius };

			for (int offset = -radius; offset <= radius; offset += step)
				offsets.Add(offset);

			return offsets.ToArray();
		}

		internal static IReadOnlyList<int> BuildFineOffsets(
			int center,
			int coarseStep,
			int radius,
			ISet<int> excludedOffsets) {
			if (coarseStep <= 1)
				return Array.Empty<int>();

			radius = Math.Clamp(radius, 1, 300);
			int start = Math.Max(-radius, center - coarseStep + 1);
			int end = Math.Min(radius, center + coarseStep - 1);
			var offsets = new List<int>();

			for (int offset = start; offset <= end; offset++) {
				if (!excludedOffsets.Contains(offset))
					offsets.Add(offset);
			}

			return offsets;
		}
		const int ConfidentHashDistance = 3;
		const double ConfidentMeanDifference = 0.06d;

		internal static bool IsConfident(FrameAlignmentResult result) =>
			result.HashDistance <= ConfidentHashDistance &&
			result.MeanAbsoluteDifference <= ConfidentMeanDifference;
		const int MinimumNonZeroHashImprovement = 2;
		const double MinimumNonZeroMeanImprovement = 0.006d;
		const int NonZeroDistancePenaltyEveryFrames = 8;

		internal static bool IsClearImprovementOverZero(
			FrameAlignmentResult zero,
			FrameAlignmentResult candidate) {
			if (candidate.Offset == 0)
				return true;
			if (!IsConfident(candidate))
				return false;

			int distance = Math.Abs(candidate.Offset);
			int requiredHashImprovement =
				MinimumNonZeroHashImprovement +
				(distance / NonZeroDistancePenaltyEveryFrames);
			double requiredMeanImprovement =
				MinimumNonZeroMeanImprovement +
				(Math.Min(distance, 30) * 0.0004d);

			int hashImprovement =
				zero.HashDistance - candidate.HashDistance;
			double meanImprovement =
				zero.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			bool hashLedImprovement =
				hashImprovement >= requiredHashImprovement &&
				meanImprovement >= requiredMeanImprovement / 2d;
			bool pixelLedImprovement =
				hashImprovement >=
					Math.Max(1, requiredHashImprovement - 2) &&
				meanImprovement >= requiredMeanImprovement;

			return hashLedImprovement || pixelLedImprovement;
		}

		internal static IReadOnlyList<IReadOnlyList<int>>
			BuildProgressiveOffsetBatches(int radius) {
			radius = Math.Clamp(radius, 1, 300);
			int[] preferredSteps = { 1, 2, 4, 8, 16, radius };
			var seen = new HashSet<int>();
			var batches = new List<IReadOnlyList<int>>();

			foreach (int step in preferredSteps) {
				if (step > radius || !seen.Add(step))
					continue;

				batches.Add(new[] { -step, step });
			}

			return batches;
		}

		internal static IReadOnlyList<int> BuildRefinementOffsets(
			int center,
			ISet<int> testedOffsets,
			int radius) {
			radius = Math.Clamp(radius, 1, 300);
			int? lower = null;
			int? upper = null;

			foreach (int offset in testedOffsets) {
				if (offset < center &&
					(!lower.HasValue || offset > lower.Value)) {
					lower = offset;
				}
				else if (offset > center &&
					(!upper.HasValue || offset < upper.Value)) {
					upper = offset;
				}
			}

			var result = new SortedSet<int>();

			if (lower.HasValue && center - lower.Value > 1)
				result.Add(lower.Value + ((center - lower.Value) / 2));
			if (upper.HasValue && upper.Value - center > 1)
				result.Add(center + ((upper.Value - center) / 2));

			result.RemoveWhere(offset =>
				offset < -radius ||
				offset > radius ||
				testedOffsets.Contains(offset));

			return result.ToArray();
		}
	}
}
