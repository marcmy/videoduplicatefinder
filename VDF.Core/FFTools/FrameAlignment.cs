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
		const int ConfidenceNearOffsetFrames = 2;
		const int ConfidenceLocalBasinFrames = 8;
		const int ConfidenceStrongHashGain = 8;
		const double ConfidenceStrongMeanGain = 0.015d;

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

		internal static FrameAlignmentResult? FindBestConfidentResult(
			IEnumerable<FrameAlignmentResult> results,
			int baselineOffset = 0,
			int preferredOffset = 0) {
			FrameAlignmentResult[] measured = results.ToArray();
			FrameAlignmentResult? baseline = null;
			foreach (FrameAlignmentResult result in measured) {
				if (result.Offset == baselineOffset) {
					baseline = result;
					break;
				}
			}

			var remaining = measured.ToList();
			while (remaining.Count > 0) {
				FrameAlignmentResult? best = FindBestResult(remaining);
				if (!best.HasValue)
					break;
				if (best.Value.Offset == baselineOffset)
					return best;
				if (!baseline.HasValue)
					return best;

				if (IsConfidentAgainstBaseline(
					best.Value,
					baseline.Value) &&
					IsConfidentAgainstPreferredDirection(
						best.Value,
						baseline.Value,
						baselineOffset,
						preferredOffset) &&
					IsConfidentAgainstAlternatives(
						best.Value,
						measured,
						baselineOffset)) {
					return best;
				}

				remaining.Remove(best.Value);
			}

			return baseline;
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

		static bool IsConfidentAgainstBaseline(
			FrameAlignmentResult candidate,
			FrameAlignmentResult baseline) {
			int offsetDistance =
				Math.Abs(candidate.Offset - baseline.Offset);
			int hashGain =
				baseline.HashDistance - candidate.HashDistance;
			double meanGain =
				baseline.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			if (offsetDistance <= ConfidenceNearOffsetFrames) {
				return
					(hashGain > 0 &&
						meanGain >= -MeanDifferenceTieTolerance) ||
					(hashGain >= 0 &&
						meanGain > MeanDifferenceTieTolerance);
			}

			if (hashGain >= ConfidenceStrongHashGain &&
				meanGain >= -MeanDifferenceTieTolerance) {
				return true;
			}

			if (meanGain >= ConfidenceStrongMeanGain &&
				hashGain >= -1) {
				return true;
			}

			int requiredHashGain =
				offsetDistance <= 16 ? 1 :
				offsetDistance <= 60 ? 2 :
				3;
			double requiredMeanGain =
				offsetDistance <= 16 ? 0.002d :
				offsetDistance <= 60 ? 0.004d :
				0.006d;

			return
				hashGain >= requiredHashGain &&
				meanGain >= requiredMeanGain;
		}

		static bool IsConfidentAgainstPreferredDirection(
			FrameAlignmentResult candidate,
			FrameAlignmentResult baseline,
			int baselineOffset,
			int preferredOffset) {
			int preferredDirection = Math.Sign(preferredOffset);
			int candidateDirection =
				Math.Sign(candidate.Offset - baselineOffset);

			if (preferredDirection == 0 ||
				candidateDirection == 0 ||
				candidateDirection == preferredDirection ||
				Math.Abs(candidate.Offset - baselineOffset) <=
					ConfidenceNearOffsetFrames) {
				return true;
			}

			int hashGain =
				baseline.HashDistance - candidate.HashDistance;
			double meanGain =
				baseline.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			return
				hashGain >= ConfidenceStrongHashGain &&
				meanGain >= ConfidenceStrongMeanGain;
		}

		static bool IsConfidentAgainstAlternatives(
			FrameAlignmentResult candidate,
			IEnumerable<FrameAlignmentResult> results,
			int baselineOffset) {
			if (Math.Abs(candidate.Offset - baselineOffset) <=
				ConfidenceNearOffsetFrames) {
				return true;
			}

			FrameAlignmentResult? competitor = FindBestResult(
				results.Where(result =>
					result.Offset != baselineOffset &&
					Math.Abs(result.Offset - candidate.Offset) >
						ConfidenceLocalBasinFrames));

			if (!competitor.HasValue)
				return true;

			int hashMargin =
				competitor.Value.HashDistance -
				candidate.HashDistance;
			double meanMargin =
				competitor.Value.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			return
				hashMargin >= 2 ||
				(hashMargin >= 0 && meanMargin >= 0.002d);
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

		internal static IReadOnlyList<IReadOnlyList<int>>
			BuildProgressiveOffsetBatches(int radius) {
			radius = Math.Clamp(radius, 1, 300);
			int[] preferredSteps = { 1, 2, 4, 8, 16, 30, 60, radius };
			var seen = new HashSet<int>();
			var batches = new List<IReadOnlyList<int>>();

			foreach (int step in preferredSteps) {
				if (step > radius || !seen.Add(step))
					continue;

				batches.Add(new[] { -step, step });
			}

			return batches;
		}

		internal static IReadOnlyList<IReadOnlyList<int>>
			BuildDurationHintOffsetBatches(
				int hintOffset,
				int radius) {
			radius = Math.Clamp(radius, 1, 300);
			hintOffset = Math.Clamp(hintOffset, -radius, radius);

			if (hintOffset == 0)
				return Array.Empty<IReadOnlyList<int>>();

			var seen = new HashSet<int> { 0 };
			var batches = new List<IReadOnlyList<int>>();

			void AddBatch(IEnumerable<int> offsets) {
				int[] batch = offsets
					.Where(offset =>
						Math.Abs(offset) <= radius &&
						seen.Add(offset))
					.ToArray();

				if (batch.Length > 0)
					batches.Add(batch);
			}

			int oppositeHintOffset = -hintOffset;
			AddBatch(new[] { hintOffset, oppositeHintOffset });

			int[] spreads = { 1, 2, 4, 8, 16 };
			foreach (int spread in spreads) {
				AddBatch(new[] {
					hintOffset - spread,
					hintOffset + spread,
					oppositeHintOffset - spread,
					oppositeHintOffset + spread,
				});
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
