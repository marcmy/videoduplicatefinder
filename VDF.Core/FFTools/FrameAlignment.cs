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
			return FindBestResult(MeasureCandidates(reference, candidates));
		}

		internal static IReadOnlyList<FrameAlignmentResult> MeasureCandidates(
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
			var results = new List<FrameAlignmentResult>();

			foreach (FrameAlignmentCandidate candidate in candidates) {
				byte[]? gray = candidate.GrayBytes;
				if (gray == null || gray.Length != reference.Length)
					continue;

				ulong candidateHash =
					pHash.PerceptualHash.ComputePHashFromGray32x32(gray);
				int hashDistance =
					BitOperations.PopCount(referenceHash ^ candidateHash);

				double meanDifference =
					CalculateMeanAbsoluteDifference(reference, gray);

				var result = new FrameAlignmentResult(
					candidate.Offset,
					hashDistance,
					meanDifference);
				results.Add(result);
			}

			return results;
		}

		internal static FrameAlignmentResult? FindBestResult(
			IEnumerable<FrameAlignmentResult> results) {
			FrameAlignmentResult? best = null;

			foreach (FrameAlignmentResult result in results) {
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

		internal static double CalculateMeanAbsoluteDifference(
			byte[] reference,
			byte[] candidate) {
			ArgumentNullException.ThrowIfNull(reference);
			ArgumentNullException.ThrowIfNull(candidate);
			if (reference.Length != candidate.Length) {
				throw new ArgumentException(
					"Candidate frame must have the same length as the reference.",
					nameof(candidate));
			}

			long absoluteDifference = 0;
			for (int i = 0; i < reference.Length; i++)
				absoluteDifference += Math.Abs(reference[i] - candidate[i]);

			return absoluteDifference / (255d * reference.Length);
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
		const int ConfidentZeroHashImprovementBonus = 1;
		const double ConfidentZeroMeanImprovementBonus = 0.006d;

		const int NearlyExactHashDistance = 1;
		const double NearlyExactMeanDifference = 0.018d;
		const int MaxUsableCandidateHashDistance = 12;
		const double MaxUsableCandidateMeanDifference = 0.14d;
		const double NearbyNudgeMeanRegressionTolerance = 0.001d;
		const double NearbyNudgeDetailImprovement = 0.00035d;
		const double NearbyNudgeDetailTieTolerance = 0.0001d;

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

			(int requiredHashImprovement, double requiredMeanImprovement) =
				GetRequiredImprovement(zero, candidate);

			int hashImprovement =
				zero.HashDistance - candidate.HashDistance;
			double meanImprovement =
				zero.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			// Both independent measures must improve. Nearby corrections need
			// clear evidence, and a zero frame that is already confident adds
			// hysteresis so noise does not nudge an aligned pair off zero.
			return
				hashImprovement >= requiredHashImprovement &&
				meanImprovement >= requiredMeanImprovement;
		}

		static (int Hash, double Mean) GetRequiredImprovement(
			FrameAlignmentResult zero,
			FrameAlignmentResult candidate) {
			int distance = Math.Abs(candidate.Offset);
			int requiredHashImprovement =
				MinimumNonZeroHashImprovement +
				(distance / NonZeroDistancePenaltyEveryFrames);
			double requiredMeanImprovement =
				MinimumNonZeroMeanImprovement +
				(Math.Min(distance, 30) * 0.0004d);

			if (IsConfident(zero)) {
				requiredHashImprovement +=
					ConfidentZeroHashImprovementBonus;
				requiredMeanImprovement +=
					ConfidentZeroMeanImprovementBonus;
			}

			return (requiredHashImprovement, requiredMeanImprovement);
		}

		internal static FrameAlignmentResult? FindBestClearImprovementOverZero(
			FrameAlignmentResult zero,
			IEnumerable<FrameAlignmentResult> results) {
			FrameAlignmentResult? best = null;

			foreach (FrameAlignmentResult result in results) {
				if (result.Offset == 0 ||
					!IsClearImprovementOverZero(zero, result)) {
					continue;
				}

				if (best == null || IsBetter(result, best.Value))
					best = result;
			}

			return best;
		}

		internal static FrameAlignmentResult? FindBestNearbyNudge(
			FrameAlignmentResult zero,
			IEnumerable<FrameAlignmentResult> results,
			IReadOnlyDictionary<int, double> detailMeanDifferences) {
			if (!detailMeanDifferences.TryGetValue(0, out double zeroDetail))
				return null;

			FrameAlignmentResult? best = null;
			double bestDetail = double.MaxValue;

			foreach (FrameAlignmentResult result in results) {
				if (!IsPotentialNearbyNudge(zero, result))
					continue;
				if (!detailMeanDifferences.TryGetValue(
					result.Offset,
					out double detailMeanDifference))
					continue;
				if (zeroDetail - detailMeanDifference <
					NearbyNudgeDetailImprovement) {
					continue;
				}

				if (!best.HasValue ||
					detailMeanDifference <
						bestDetail - NearbyNudgeDetailTieTolerance ||
					(Math.Abs(detailMeanDifference - bestDetail) <=
						NearbyNudgeDetailTieTolerance &&
						IsBetter(result, best.Value))) {
					best = result;
					bestDetail = detailMeanDifference;
				}
			}

			return best;
		}

		internal static bool HasNearbyNudgeCandidate(
			FrameAlignmentResult zero,
			IEnumerable<FrameAlignmentResult> results) =>
			results.Any(result => IsPotentialNearbyNudge(zero, result));

		static bool IsPotentialNearbyNudge(
			FrameAlignmentResult zero,
			FrameAlignmentResult result) {
			if (Math.Abs(result.Offset) != 1)
				return false;
			if (result.HashDistance > zero.HashDistance)
				return false;
			if (result.HashDistance > MaxUsableCandidateHashDistance ||
				result.MeanAbsoluteDifference >
					MaxUsableCandidateMeanDifference) {
				return false;
			}

			return result.MeanAbsoluteDifference -
				zero.MeanAbsoluteDifference <=
				NearbyNudgeMeanRegressionTolerance;
		}

		internal static int SelectConsensusOffset(
			int primaryOffset,
			IReadOnlyList<int> offsets) {
			if (offsets.Count <= 1)
				return primaryOffset;
			if (primaryOffset == 0)
				return 0;
			if (Math.Abs(primaryOffset) <= 1)
				return primaryOffset;

			int matchingVotes = offsets.Count(offset => offset == primaryOffset);
			return matchingVotes >= 2 ? primaryOffset : 0;
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
