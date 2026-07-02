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
		double MeanAbsoluteDifference,
		double GradientDifference = -1d);

	internal static class FrameAlignment {
		const double MeanDifferenceTieTolerance = 0.0005d;
		const int ModelCandidatesPerAnchor = 8;
		const int ModelCandidateSeparationFrames = 2;
		const int ModelSupportRadiusFrames = 5;
		const int ModelRefinementRadiusFrames = 4;
		const double ModelDistancePenalty = 0.035d;
		const double MinimumModelImprovement = 0.035d;
		const double MinimumModelSeparation = 0.012d;

		readonly record struct RankedAlignmentCandidate(
			FrameAlignmentResult Result,
			double LocalCost);

		readonly record struct AlignmentAnchor(
			int Index,
			IReadOnlyList<RankedAlignmentCandidate> Candidates,
			double Weight);

		readonly record struct AffineAlignmentModel(
			double Intercept,
			double Slope,
			double Score,
			int Support) {
			internal double Predict(int index) => Intercept + (Slope * index);
		}

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
				double gradientDifference =
					CalculateGradientDifference(reference, gray, 32);

				results.Add(new FrameAlignmentResult(
					candidate.Offset,
					hashDistance,
					meanDifference,
					gradientDifference));
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

		// The multi-anchor model is the confidence mechanism. When only one
		// anchor is available, preserve the original bounded best-match behavior
		// instead of adding another set of example-specific thresholds.
		internal static FrameAlignmentResult? FindBestConfidentResult(
			IEnumerable<FrameAlignmentResult> results,
			int baselineOffset = 0,
			int preferredOffset = 0) {
			_ = baselineOffset;
			_ = preferredOffset;
			return FindBestResult(results);
		}

		// Fits one pair-level timeline relationship:
		//
		//     B frame offset ~= intercept + slope * thumbnail index
		//
		// Candidate quality is ranked independently inside each thumbnail anchor,
		// flat/ambiguous anchors are down-weighted, and the final score is robust to
		// isolated bad local minima. The duration-derived offsets are deliberately
		// not used here; they remain useful only for deciding where to probe.
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

			_ = preferredOffsets;
			var selections = new FrameAlignmentResult?[resultsByIndex.Count];
			List<AlignmentAnchor> anchors = BuildAlignmentAnchors(resultsByIndex);
			if (anchors.Count == 0)
				return selections;

			AffineAlignmentModel zeroModel = EvaluateModel(0d, 0d, anchors);
			List<AffineAlignmentModel> models = BuildCandidateModels(anchors);
			if (models.Count == 0)
				models.Add(zeroModel);

			AffineAlignmentModel best = models
				.OrderBy(model => model.Score)
				.ThenByDescending(model => model.Support)
				.ThenBy(model => ModelMagnitude(model, anchors))
				.First();

			best = RefitModel(best, anchors);
			AffineAlignmentModel second = models
				.Where(model => IsDistinctModel(model, best, anchors))
				.OrderBy(model => model.Score)
				.ThenByDescending(model => model.Support)
				.FirstOrDefault(zeroModel);

			int informativeAnchors = anchors.Count(anchor => anchor.Weight >= 0.25d);
			bool nearZeroModel =
				Math.Abs(best.Predict(anchors[0].Index)) <= 2d &&
				Math.Abs(best.Predict(anchors[^1].Index)) <= 2d;
			double zeroImprovement = zeroModel.Score - best.Score;
			double secondMargin = second.Score - best.Score;
			int requiredSupport = informativeAnchors <= 3
				? 2
				: Math.Max(3, (informativeAnchors + 1) / 2);
			bool useBestModel =
				nearZeroModel ||
				(informativeAnchors >= 3 &&
					best.Support >= requiredSupport &&
					zeroImprovement >= MinimumModelImprovement &&
					secondMargin >= MinimumModelSeparation);

			AffineAlignmentModel selectedModel =
				useBestModel ? best : zeroModel;

			foreach (AlignmentAnchor anchor in anchors) {
				double predictedOffset = selectedModel.Predict(anchor.Index);
				selections[anchor.Index] = SelectAlongModel(
					anchor,
					predictedOffset,
					useBestModel || nearZeroModel);
			}

			return selections;
		}

		static List<AlignmentAnchor> BuildAlignmentAnchors(
			IReadOnlyList<IReadOnlyList<FrameAlignmentResult>> resultsByIndex) {
			var anchors = new List<AlignmentAnchor>();

			for (int index = 0; index < resultsByIndex.Count; index++) {
				FrameAlignmentResult[] unique = resultsByIndex[index]
					.GroupBy(result => result.Offset)
					.Select(group => FindBestResult(group)!.Value)
					.OrderBy(result => result.Offset)
					.ToArray();
				if (unique.Length == 0)
					continue;

				int minimumHash = unique.Min(result => result.HashDistance);
				int maximumHash = unique.Max(result => result.HashDistance);
				double minimumMean =
					unique.Min(result => result.MeanAbsoluteDifference);
				double maximumMean =
					unique.Max(result => result.MeanAbsoluteDifference);
				FrameAlignmentResult[] gradientResults = unique
					.Where(result => result.GradientDifference >= 0d)
					.ToArray();
				double minimumGradient = gradientResults.Length > 0
					? gradientResults.Min(result => result.GradientDifference)
					: 0d;
				double maximumGradient = gradientResults.Length > 0
					? gradientResults.Max(result => result.GradientDifference)
					: 0d;
				double hashInformation =
					Math.Clamp((maximumHash - minimumHash) / 8d, 0d, 1d);
				double meanInformation =
					Math.Clamp((maximumMean - minimumMean) / 0.03d, 0d, 1d);
				double gradientInformation = gradientResults.Length > 1
					? Math.Clamp(
						(maximumGradient - minimumGradient) / 0.03d,
						0d,
						1d)
					: 0d;
				double metricWeight =
					hashInformation + meanInformation + gradientInformation;
				double anchorWeight = Math.Clamp(
					Math.Max(
						hashInformation,
						Math.Max(meanInformation, gradientInformation)),
					0.08d,
					1d);

				var ranked = new List<RankedAlignmentCandidate>(unique.Length);
				foreach (FrameAlignmentResult result in unique) {
					double hashRank = StrictRank(
						unique,
						candidate => candidate.HashDistance,
						result.HashDistance);
					double meanRank = StrictRank(
						unique,
						candidate => candidate.MeanAbsoluteDifference,
						result.MeanAbsoluteDifference);
					double gradientRank = result.GradientDifference >= 0d
						? StrictRank(
							gradientResults,
							candidate => candidate.GradientDifference,
							result.GradientDifference)
						: 0d;
					double localCost = metricWeight > 0.0001d
						? ((hashRank * hashInformation) +
							(meanRank * meanInformation) +
							(gradientRank * gradientInformation)) /
							metricWeight
						: 0.5d;
					ranked.Add(new RankedAlignmentCandidate(result, localCost));
				}

				anchors.Add(new AlignmentAnchor(index, ranked, anchorWeight));
			}

			return anchors;
		}

		static double StrictRank<T>(
			IReadOnlyList<FrameAlignmentResult> values,
			Func<FrameAlignmentResult, T> selector,
			T value)
			where T : IComparable<T> {
			if (values.Count <= 1)
				return 0d;

			int lower = values.Count(candidate =>
				selector(candidate).CompareTo(value) < 0);
			return lower / (double)(values.Count - 1);
		}

		static List<AffineAlignmentModel> BuildCandidateModels(
			IReadOnlyList<AlignmentAnchor> anchors) {
			var models = new List<AffineAlignmentModel>();
			var seen = new HashSet<(int Intercept, int Slope)>();

			void AddModel(double intercept, double slope) {
				if (!double.IsFinite(intercept) || !double.IsFinite(slope))
					return;

				var key = (
					(int)Math.Round(intercept * 2d),
					(int)Math.Round(slope * 4d));
				if (!seen.Add(key))
					return;

				models.Add(EvaluateModel(intercept, slope, anchors));
			}

			AddModel(0d, 0d);
			var modelCandidates = anchors.ToDictionary(
				anchor => anchor.Index,
				SelectModelCandidates);

			foreach (AlignmentAnchor anchor in anchors) {
				foreach (RankedAlignmentCandidate candidate in
					modelCandidates[anchor.Index]) {
					AddModel(candidate.Result.Offset, 0d);
				}
			}

			for (int firstIndex = 0;
				firstIndex < anchors.Count;
				firstIndex++) {
				AlignmentAnchor first = anchors[firstIndex];
				for (int secondIndex = firstIndex + 1;
					secondIndex < anchors.Count;
					secondIndex++) {
					AlignmentAnchor second = anchors[secondIndex];
					int indexDistance = second.Index - first.Index;
					if (indexDistance < 2)
						continue;

					foreach (RankedAlignmentCandidate firstCandidate in
						modelCandidates[first.Index]) {
						foreach (RankedAlignmentCandidate secondCandidate in
							modelCandidates[second.Index]) {
							double slope =
								(secondCandidate.Result.Offset -
									firstCandidate.Result.Offset) /
								(double)indexDistance;
							double intercept =
								firstCandidate.Result.Offset -
								(slope * first.Index);
							AddModel(intercept, slope);
						}
					}
				}
			}

			return models;
		}

		static IReadOnlyList<RankedAlignmentCandidate> SelectModelCandidates(
			AlignmentAnchor anchor) {
			var selected = new List<RankedAlignmentCandidate>();
			foreach (RankedAlignmentCandidate candidate in anchor.Candidates
				.OrderBy(candidate => candidate.LocalCost)
				.ThenBy(candidate => Math.Abs(candidate.Result.Offset))) {
				if (selected.Any(existing =>
					Math.Abs(existing.Result.Offset - candidate.Result.Offset) <
						ModelCandidateSeparationFrames)) {
					continue;
				}

				selected.Add(candidate);
				if (selected.Count >= ModelCandidatesPerAnchor)
					break;
			}

			RankedAlignmentCandidate[] zeroCandidates = anchor.Candidates
				.Where(candidate => candidate.Result.Offset == 0)
				.ToArray();
			if (zeroCandidates.Length > 0 && !selected.Any(candidate =>
				candidate.Result.Offset == 0)) {
				selected.Add(zeroCandidates[0]);
			}

			return selected;
		}

		static AffineAlignmentModel EvaluateModel(
			double intercept,
			double slope,
			IReadOnlyList<AlignmentAnchor> anchors) {
			var weightedCosts = new List<(double Cost, double Weight)>();
			int support = 0;

			foreach (AlignmentAnchor anchor in anchors) {
				double predicted = intercept + (slope * anchor.Index);
				RankedAlignmentCandidate nearest = anchor.Candidates
					.OrderBy(candidate =>
						candidate.LocalCost +
						(Math.Min(
							Math.Abs(candidate.Result.Offset - predicted),
							20d) * ModelDistancePenalty))
					.ThenBy(candidate =>
						Math.Abs(candidate.Result.Offset - predicted))
					.First();
				double residual =
					Math.Abs(nearest.Result.Offset - predicted);
				double cost = nearest.LocalCost +
					(Math.Min(residual, 20d) * ModelDistancePenalty);
				weightedCosts.Add((cost, anchor.Weight));

				if (residual <= ModelSupportRadiusFrames &&
					nearest.LocalCost <= 0.55d &&
					anchor.Weight >= 0.2d) {
					support++;
				}
			}

			double mean = WeightedMean(weightedCosts);
			double median = WeightedMedian(weightedCosts);
			double score = (median * 0.65d) + (mean * 0.35d);
			return new AffineAlignmentModel(intercept, slope, score, support);
		}

		static AffineAlignmentModel RefitModel(
			AffineAlignmentModel model,
			IReadOnlyList<AlignmentAnchor> anchors) {
			for (int iteration = 0; iteration < 2; iteration++) {
				double sumWeight = 0d;
				double sumX = 0d;
				double sumY = 0d;
				double sumXX = 0d;
				double sumXY = 0d;

				foreach (AlignmentAnchor anchor in anchors) {
					double predicted = model.Predict(anchor.Index);
					RankedAlignmentCandidate selected = anchor.Candidates
						.OrderBy(candidate =>
							candidate.LocalCost +
							(Math.Abs(candidate.Result.Offset - predicted) *
								ModelDistancePenalty))
						.First();
					double residual =
						Math.Abs(selected.Result.Offset - predicted);
					double robustWeight =
						1d / Math.Max(1d, residual / 4d);
					double weight = anchor.Weight * robustWeight *
						Math.Max(0.1d, 1d - selected.LocalCost);
					double x = anchor.Index;
					double y = selected.Result.Offset;

					sumWeight += weight;
					sumX += weight * x;
					sumY += weight * y;
					sumXX += weight * x * x;
					sumXY += weight * x * y;
				}

				double denominator =
					(sumWeight * sumXX) - (sumX * sumX);
				if (sumWeight <= 0d || Math.Abs(denominator) < 0.0001d)
					break;

				double slope =
					((sumWeight * sumXY) - (sumX * sumY)) /
					denominator;
				double intercept = (sumY - (slope * sumX)) / sumWeight;
				model = EvaluateModel(intercept, slope, anchors);
			}

			return model;
		}

		static FrameAlignmentResult SelectAlongModel(
			AlignmentAnchor anchor,
			double predictedOffset,
			bool allowLocalRefinement) {
			int roundedPrediction = (int)Math.Round(predictedOffset);
			RankedAlignmentCandidate nearest = anchor.Candidates
				.OrderBy(candidate =>
					Math.Abs(candidate.Result.Offset - predictedOffset))
				.ThenBy(candidate => candidate.LocalCost)
				.First();

			if (!allowLocalRefinement || anchor.Weight < 0.18d) {
				return nearest.Result with { Offset = roundedPrediction };
			}

			RankedAlignmentCandidate[] nearby = anchor.Candidates
				.Where(candidate =>
					Math.Abs(candidate.Result.Offset - predictedOffset) <=
						ModelRefinementRadiusFrames)
				.ToArray();
			if (nearby.Length == 0)
				return nearest.Result with { Offset = roundedPrediction };

			RankedAlignmentCandidate refined = nearby
				.OrderBy(candidate =>
					candidate.LocalCost +
					(Math.Abs(candidate.Result.Offset - predictedOffset) * 0.04d))
				.ThenBy(candidate =>
					Math.Abs(candidate.Result.Offset - predictedOffset))
				.First();

			return refined.Result;
		}

		static bool IsDistinctModel(
			AffineAlignmentModel candidate,
			AffineAlignmentModel selected,
			IReadOnlyList<AlignmentAnchor> anchors) {
			return
				Math.Abs(candidate.Predict(anchors[0].Index) -
					selected.Predict(anchors[0].Index)) > 4d ||
				Math.Abs(candidate.Predict(anchors[^1].Index) -
					selected.Predict(anchors[^1].Index)) > 4d;
		}

		static double ModelMagnitude(
			AffineAlignmentModel model,
			IReadOnlyList<AlignmentAnchor> anchors) {
			return
				Math.Abs(model.Predict(anchors[0].Index)) +
				Math.Abs(model.Predict(anchors[^1].Index));
		}

		static double WeightedMean(
			IReadOnlyList<(double Cost, double Weight)> values) {
			double totalWeight = values.Sum(value => value.Weight);
			if (totalWeight <= 0d)
				return double.PositiveInfinity;

			return values.Sum(value => value.Cost * value.Weight) /
				totalWeight;
		}

		static double WeightedMedian(
			IReadOnlyList<(double Cost, double Weight)> values) {
			double totalWeight = values.Sum(value => value.Weight);
			if (totalWeight <= 0d)
				return double.PositiveInfinity;

			double halfWeight = totalWeight / 2d;
			double accumulated = 0d;
			foreach ((double cost, double weight) in values
				.OrderBy(value => value.Cost)) {
				accumulated += weight;
				if (accumulated >= halfWeight)
					return cost;
			}

			return values.Max(value => value.Cost);
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

			if (candidate.GradientDifference >= 0d &&
				current.GradientDifference >= 0d) {
				double gradientImprovement =
					current.GradientDifference - candidate.GradientDifference;
				if (gradientImprovement > MeanDifferenceTieTolerance)
					return true;
				if (gradientImprovement < -MeanDifferenceTieTolerance)
					return false;
			}

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

		internal static double CalculateGradientDifference(
			byte[] reference,
			byte[] candidate,
			int sideLength) {
			ArgumentNullException.ThrowIfNull(reference);
			ArgumentNullException.ThrowIfNull(candidate);
			if (sideLength < 3 ||
				reference.Length != sideLength * sideLength ||
				candidate.Length != reference.Length) {
				throw new ArgumentException(
					"Gradient frames must be matching square grayscale buffers.",
					nameof(candidate));
			}

			long difference = 0;
			int samples = 0;
			for (int y = 1; y < sideLength - 1; y++) {
				int row = y * sideLength;
				for (int x = 1; x < sideLength - 1; x++) {
					int index = row + x;
					int referenceX =
						reference[index + 1] - reference[index - 1];
					int referenceY =
						reference[index + sideLength] -
						reference[index - sideLength];
					int candidateX =
						candidate[index + 1] - candidate[index - 1];
					int candidateY =
						candidate[index + sideLength] -
						candidate[index - sideLength];
					difference +=
						Math.Abs(referenceX - candidateX) +
						Math.Abs(referenceY - candidateY);
					samples++;
				}
			}

			return difference / (1020d * samples);
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
