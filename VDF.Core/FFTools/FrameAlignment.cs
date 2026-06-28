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

		const int NearlyExactHashDistance = 1;
		const double NearlyExactMeanDifference = 0.018d;
		const int MaxUsableCandidateHashDistance = 12;
		const double MaxUsableCandidateMeanDifference = 0.14d;

		internal static bool IsNearlyExact(
			FrameAlignmentResult result) =>
				result.HashDistance <= NearlyExactHashDistance &&
				result.MeanAbsoluteDifference <=
					NearlyExactMeanDifference;

		internal static bool IsClearImprovementOverZero(
			FrameAlignmentResult zero,
			FrameAlignmentResult candidate) {
			if (candidate.Offset == 0)
				return true;

			// Reject objectively poor candidates, but judge usable candidates
			// primarily by how much they improve over the zero-offset baseline.
			if (candidate.HashDistance >
					MaxUsableCandidateHashDistance ||
				candidate.MeanAbsoluteDifference >
					MaxUsableCandidateMeanDifference) {
				return false;
			}

			int distance = Math.Abs(candidate.Offset);
			int requiredHashImprovement;
			double requiredMeanImprovement;

			if (distance <= 4) {
				requiredHashImprovement = 1;
				requiredMeanImprovement = 0.003d;
			}
			else if (distance <= 12) {
				requiredHashImprovement = 2;
				requiredMeanImprovement = 0.007d;
			}
			else {
				requiredHashImprovement =
					4 + ((distance - 13) / 8);
				requiredMeanImprovement =
					0.014d +
					((distance - 13) * 0.0005d);
			}

			int hashImprovement =
				zero.HashDistance - candidate.HashDistance;
			double meanImprovement =
				zero.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			// Both independent measures must improve. Nearby corrections need
			// only a modest advantage; large jumps need progressively stronger
			// evidence, which blocks repeated-scene false matches.
			return
				hashImprovement >= requiredHashImprovement &&
				meanImprovement >= requiredMeanImprovement;
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
