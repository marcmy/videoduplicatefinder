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
		const double PreferredMeanGain = 0.003d;
		const double PreferredMeanTieTolerance = 0.004d;
		const double SmoothPathHashPenalty = 0.55d;
		const double SmoothPathMeanPenalty = 120d;
		const double SmoothPathPreferredFramePenalty = 0.015d;
		const double SmoothPathWeakNonZeroPenalty = 0.75d;
		const double SmoothPathTransitionFramePenalty = 0.22d;

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
					return ChoosePreferredResultWhenNeeded(
						best.Value,
						baseline.Value,
						measured,
						baselineOffset,
						preferredOffset);
				}

				remaining.Remove(best.Value);
			}

			if (baseline.HasValue) {
				return ChoosePreferredResultWhenNeeded(
					baseline.Value,
					baseline.Value,
					measured,
					baselineOffset,
					preferredOffset);
			}

			return baseline;
		}

		internal static IReadOnlyList<FrameAlignmentResult?> FindBestSmoothPath(
			IReadOnlyList<IReadOnlyList<FrameAlignmentResult>> resultsByIndex,
			IReadOnlyList<int> preferredOffsets) {
			ArgumentNullException.ThrowIfNull(resultsByIndex);
			ArgumentNullException.ThrowIfNull(preferredOffsets);
			if (resultsByIndex.Count != preferredOffsets.Count) {
				throw new ArgumentException(
					"Preferred offsets must match the number of result groups.",
					nameof(preferredOffsets));
			}

			var selections = new FrameAlignmentResult?[resultsByIndex.Count];
			if (resultsByIndex.Count == 0)
				return selections;

			List<FrameAlignmentResult>[] candidatesByIndex =
				resultsByIndex
					.Select(BuildSmoothPathCandidates)
					.ToArray();
			int[] activeIndexes = Enumerable.Range(0, resultsByIndex.Count)
				.Where(index => candidatesByIndex[index].Count > 0)
				.ToArray();

			if (activeIndexes.Length == 0)
				return selections;

			var costs = new double[activeIndexes.Length][];
			var parents = new int[activeIndexes.Length][];

			for (int activeIndex = 0;
				activeIndex < activeIndexes.Length;
				activeIndex++) {
				int sourceIndex = activeIndexes[activeIndex];
				List<FrameAlignmentResult> candidates =
					candidatesByIndex[sourceIndex];
				costs[activeIndex] = new double[candidates.Count];
				parents[activeIndex] = new int[candidates.Count];

				for (int candidateIndex = 0;
					candidateIndex < candidates.Count;
					candidateIndex++) {
					FrameAlignmentResult candidate =
						candidates[candidateIndex];
					double localCost = CalculateSmoothPathLocalCost(
						candidate,
						resultsByIndex[sourceIndex],
						preferredOffsets[sourceIndex]);

					if (activeIndex == 0) {
						costs[activeIndex][candidateIndex] = localCost;
						parents[activeIndex][candidateIndex] = -1;
						continue;
					}

					int previousSourceIndex = activeIndexes[activeIndex - 1];
					List<FrameAlignmentResult> previousCandidates =
						candidatesByIndex[previousSourceIndex];
					double bestCost = double.PositiveInfinity;
					int bestParent = -1;

					for (int previousCandidateIndex = 0;
						previousCandidateIndex < previousCandidates.Count;
						previousCandidateIndex++) {
						FrameAlignmentResult previousCandidate =
							previousCandidates[previousCandidateIndex];
						double transitionCost =
							CalculateSmoothPathTransitionCost(
								previousCandidate,
								candidate,
								preferredOffsets[previousSourceIndex],
								preferredOffsets[sourceIndex]);
						double totalCost =
							costs[activeIndex - 1][previousCandidateIndex] +
							localCost +
							transitionCost;

						if (totalCost < bestCost) {
							bestCost = totalCost;
							bestParent = previousCandidateIndex;
						}
					}

					costs[activeIndex][candidateIndex] = bestCost;
					parents[activeIndex][candidateIndex] = bestParent;
				}
			}

			int selectedCandidateIndex = 0;
			for (int candidateIndex = 1;
				candidateIndex < costs[^1].Length;
				candidateIndex++) {
				if (costs[^1][candidateIndex] <
					costs[^1][selectedCandidateIndex]) {
					selectedCandidateIndex = candidateIndex;
				}
			}

			for (int activeIndex = activeIndexes.Length - 1;
				activeIndex >= 0;
				activeIndex--) {
				int sourceIndex = activeIndexes[activeIndex];
				selections[sourceIndex] =
					candidatesByIndex[sourceIndex][selectedCandidateIndex];
				selectedCandidateIndex =
					parents[activeIndex][selectedCandidateIndex];
				if (selectedCandidateIndex < 0)
					break;
			}

			return selections;
		}

		static List<FrameAlignmentResult> BuildSmoothPathCandidates(
			IReadOnlyList<FrameAlignmentResult> results) {
			var candidates = new List<FrameAlignmentResult>();
			foreach (var group in results.GroupBy(result => result.Offset)) {
				FrameAlignmentResult? best = FindBestResult(group);
				if (best.HasValue)
					candidates.Add(best.Value);
			}

			return candidates;
		}

		static double CalculateSmoothPathLocalCost(
			FrameAlignmentResult candidate,
			IReadOnlyList<FrameAlignmentResult> results,
			int preferredOffset) {
			FrameAlignmentResult best = FindBestResult(results)!.Value;
			FrameAlignmentResult? baseline = null;
			foreach (FrameAlignmentResult result in results) {
				if (result.Offset == 0) {
					baseline = result;
					break;
				}
			}

			double cost =
				Math.Max(0, candidate.HashDistance - best.HashDistance) *
					SmoothPathHashPenalty +
				Math.Max(
					0d,
					candidate.MeanAbsoluteDifference -
						best.MeanAbsoluteDifference) *
					SmoothPathMeanPenalty;

			if (preferredOffset != 0) {
				cost +=
					Math.Abs(candidate.Offset - preferredOffset) *
					SmoothPathPreferredFramePenalty;

				if (candidate.Offset != 0 &&
					Math.Sign(candidate.Offset) != Math.Sign(preferredOffset)) {
					cost += 2d;
				}
			}
			else {
				cost += Math.Abs(candidate.Offset) * 0.01d;
			}

			if (baseline.HasValue &&
				candidate.Offset != 0 &&
				!IsConfidentAgainstBaseline(candidate, baseline.Value)) {
				cost += SmoothPathWeakNonZeroPenalty;
			}

			return cost;
		}

		static double CalculateSmoothPathTransitionCost(
			FrameAlignmentResult previous,
			FrameAlignmentResult current,
			int previousPreferredOffset,
			int currentPreferredOffset) {
			int expectedDelta =
				currentPreferredOffset - previousPreferredOffset;
			int actualDelta = current.Offset - previous.Offset;
			double residual = Math.Abs(actualDelta - expectedDelta);
			double slack = Math.Abs(expectedDelta) <= 2 ? 2d : 4d;
			double cost =
				Math.Max(0d, residual - slack) *
				SmoothPathTransitionFramePenalty;

			if (Math.Abs(previous.Offset) > ConfidenceNearOffsetFrames &&
				Math.Abs(current.Offset) > ConfidenceNearOffsetFrames &&
				Math.Sign(previous.Offset) != Math.Sign(current.Offset)) {
				cost += 4d;
			}

			return cost;
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

		static FrameAlignmentResult ChoosePreferredResultWhenNeeded(
			FrameAlignmentResult selected,
			FrameAlignmentResult baseline,
			IEnumerable<FrameAlignmentResult> results,
			int baselineOffset,
			int preferredOffset) {
			FrameAlignmentResult? preferred =
				FindBestPreferredDirectionResult(
					results,
					baseline,
					baselineOffset,
					preferredOffset);

			if (!preferred.HasValue)
				return selected;
			if (!ShouldPreferDirectionResult(
				selected,
				baselineOffset,
				preferredOffset))
				return selected;

			return preferred.Value;
		}

		static FrameAlignmentResult? FindBestPreferredDirectionResult(
			IEnumerable<FrameAlignmentResult> results,
			FrameAlignmentResult baseline,
			int baselineOffset,
			int preferredOffset) {
			int preferredDirection = Math.Sign(preferredOffset);
			if (preferredDirection == 0)
				return null;

			FrameAlignmentResult? best = null;

			foreach (FrameAlignmentResult result in results) {
				if (!IsPreferredDirectionCandidate(
					result,
					baseline,
					baselineOffset,
					preferredOffset)) {
					continue;
				}

				if (!best.HasValue ||
					IsBetterPreferredDirectionCandidate(
						result,
						best.Value,
						baselineOffset,
						preferredOffset)) {
					best = result;
				}
			}

			return best;
		}

		static bool IsPreferredDirectionCandidate(
			FrameAlignmentResult candidate,
			FrameAlignmentResult baseline,
			int baselineOffset,
			int preferredOffset) {
			int candidateOffset =
				candidate.Offset - baselineOffset;
			int preferredDirection = Math.Sign(preferredOffset);

			if (Math.Sign(candidateOffset) != preferredDirection ||
				Math.Abs(candidateOffset) <=
					ConfidenceNearOffsetFrames ||
				!IsInsidePreferredWindow(
					candidate.Offset,
					baselineOffset,
					preferredOffset)) {
				return false;
			}

			int hashGain =
				baseline.HashDistance - candidate.HashDistance;
			double meanGain =
				baseline.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;

			return
				(hashGain >= 0 &&
					meanGain >= -MeanDifferenceTieTolerance) ||
				meanGain >= PreferredMeanGain;
		}

		static bool ShouldPreferDirectionResult(
			FrameAlignmentResult selected,
			int baselineOffset,
			int preferredOffset) {
			if (preferredOffset == 0)
				return false;

			int selectedOffset =
				selected.Offset - baselineOffset;
			int preferredDirection = Math.Sign(preferredOffset);

			return
				selected.Offset == baselineOffset ||
				Math.Abs(selectedOffset) <=
					ConfidenceNearOffsetFrames ||
				Math.Sign(selectedOffset) != preferredDirection ||
				!IsInsidePreferredWindow(
					selected.Offset,
					baselineOffset,
					preferredOffset);
		}

		static bool IsInsidePreferredWindow(
			int offset,
			int baselineOffset,
			int preferredOffset) {
			double window =
				Math.Clamp(Math.Abs(preferredOffset) * 0.25d, 4d, 20d);
			int relativeOffset = offset - baselineOffset;

			return
				Math.Abs(relativeOffset - preferredOffset) <= window;
		}

		static bool IsBetterPreferredDirectionCandidate(
			FrameAlignmentResult candidate,
			FrameAlignmentResult current,
			int baselineOffset,
			int preferredOffset) {
			int hashDelta =
				candidate.HashDistance - current.HashDistance;
			if (Math.Abs(hashDelta) >= 2)
				return hashDelta < 0;

			double meanImprovement =
				current.MeanAbsoluteDifference -
				candidate.MeanAbsoluteDifference;
			if (meanImprovement > PreferredMeanTieTolerance)
				return true;
			if (meanImprovement < -PreferredMeanTieTolerance)
				return false;

			double candidatePreferredDistance =
				Math.Abs(
					(candidate.Offset - baselineOffset) -
					preferredOffset);
			double currentPreferredDistance =
				Math.Abs(
					(current.Offset - baselineOffset) -
					preferredOffset);
			if (candidatePreferredDistance !=
				currentPreferredDistance) {
				return candidatePreferredDistance <
					currentPreferredDistance;
			}

			int candidateBaselineDistance =
				Math.Abs(candidate.Offset - baselineOffset);
			int currentBaselineDistance =
				Math.Abs(current.Offset - baselineOffset);
			if (candidateBaselineDistance !=
				currentBaselineDistance) {
				return candidateBaselineDistance <
					currentBaselineDistance;
			}

			return candidate.Offset < current.Offset;
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
