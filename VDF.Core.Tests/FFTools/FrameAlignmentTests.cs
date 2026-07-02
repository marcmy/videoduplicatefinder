// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using VDF.Core.FFTools;
using System.Linq;

namespace VDF.Core.Tests.FFTools;

public class FrameAlignmentTests {
	static byte[] Gradient() {
		var result = new byte[32 * 32];
		for (int i = 0; i < result.Length; i++)
			result[i] = (byte)(i % 251);
		return result;
	}

	static FrameAlignmentResult[] Results(
		params (int Offset, int Hash, double Mean)[] values) =>
			values
				.Select(value => new FrameAlignmentResult(
					value.Offset,
					value.Hash,
					value.Mean))
				.ToArray();

	[Fact]
	public void FindBest_PicksMostSimilarCandidate() {
		byte[] reference = Gradient();
		byte[] imperfect = (byte[])reference.Clone();
		for (int i = 0; i < 100; i++)
			imperfect[i] = (byte)Math.Min(255, imperfect[i] + 20);

		FrameAlignmentResult? result = FrameAlignment.FindBest(
			reference,
			new[] {
				new FrameAlignmentCandidate(0, imperfect),
				new FrameAlignmentCandidate(6, (byte[])reference.Clone()),
			});

		Assert.NotNull(result);
		Assert.Equal(6, result.Value.Offset);
	}

	[Fact]
	public void FindBest_TiePrefersOffsetClosestToZero() {
		byte[] reference = Gradient();

		FrameAlignmentResult? result = FrameAlignment.FindBest(
			reference,
			new[] {
				new FrameAlignmentCandidate(-7, (byte[])reference.Clone()),
				new FrameAlignmentCandidate(2, (byte[])reference.Clone()),
			});

		Assert.NotNull(result);
		Assert.Equal(2, result.Value.Offset);
	}

	[Fact]
	public void FindBestConfidentResult_PreservesBoundedBestMatch() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(
				Results(
					(0, 10, 0.0500d),
					(80, 9, 0.0498d)));

		Assert.NotNull(result);
		Assert.Equal(80, result.Value.Offset);
	}

	[Fact]
	public void FindBestSmoothPath_FitsPositiveTaperAndRejectsNegativeOutlier() {
		IReadOnlyList<FrameAlignmentResult?> path =
			FrameAlignment.FindBestSmoothPath(
				new[] {
					Results(
						(0, 16, 0.1164d),
						(75, 2, 0.0361d),
						(-75, 8, 0.0600d)),
					Results(
						(0, 12, 0.0875d),
						(65, 16, 0.0710d),
						(-77, 8, 0.0350d)),
					Results(
						(0, 14, 0.0850d),
						(56, 4, 0.0376d),
						(-60, 20, 0.0900d)),
					Results(
						(0, 12, 0.0800d),
						(45, 4, 0.0350d),
						(-45, 18, 0.0800d)),
					Results(
						(0, 10, 0.0700d),
						(35, 2, 0.0300d),
						(-35, 16, 0.0750d)),
				},
				new[] { 90, 80, 70, 60, 50 });

		Assert.Equal(new[] { 75, 65, 56, 45, 35 },
			path.Select(result => result!.Value.Offset));
	}

	[Fact]
	public void FindBestSmoothPath_FitsConstantLargeOffset() {
		IReadOnlyList<FrameAlignmentResult?> path =
			FrameAlignment.FindBestSmoothPath(
				Enumerable.Range(0, 5)
					.Select(_ => (IReadOnlyList<FrameAlignmentResult>)Results(
						(0, 14, 0.0900d),
						(70, 2, 0.0300d),
						(-70, 16, 0.1000d)))
					.ToArray(),
				new[] { 0, 0, 0, 0, 0 });

		Assert.All(path, result => Assert.Equal(70, result?.Offset));
	}

	[Fact]
	public void FindBestSmoothPath_RejectsUncorroboratedLargeJump() {
		IReadOnlyList<FrameAlignmentResult?> path =
			FrameAlignment.FindBestSmoothPath(
				new[] {
					Results((0, 2, 0.020d), (80, 14, 0.090d)),
					Results((0, 2, 0.021d), (80, 14, 0.089d)),
					Results((0, 12, 0.080d), (80, 0, 0.020d)),
					Results((0, 2, 0.019d), (80, 16, 0.100d)),
					Results((0, 2, 0.020d), (80, 14, 0.095d)),
				},
				new[] { 80, 80, 80, 80, 80 });

		Assert.All(path, result => Assert.Equal(0, result?.Offset));
	}

	[Fact]
	public void FindBestSmoothPath_KeepsZeroForFlatAmbiguousEvidence() {
		IReadOnlyList<FrameAlignmentResult?> path =
			FrameAlignment.FindBestSmoothPath(
				new[] {
					Results(
						(-30, 0, 0.0301d),
						(-1, 0, 0.0300d),
						(0, 0, 0.0300d),
						(1, 0, 0.0300d),
						(30, 0, 0.0301d)),
					Results(
						(-30, 0, 0.0272d),
						(-1, 0, 0.0265d),
						(0, 0, 0.0265d),
						(1, 0, 0.0265d),
						(30, 0, 0.0272d)),
					Results(
						(-30, 0, 0.0300d),
						(-1, 0, 0.0300d),
						(0, 0, 0.0300d),
						(1, 0, 0.0300d),
						(30, 0, 0.0300d)),
				},
				new[] { 90, 80, 70 });

		Assert.All(path, result => Assert.Equal(0, result?.Offset));
	}

	[Fact]
	public void FindBestSmoothPath_AllowsSmallLocalNudgesAroundModel() {
		IReadOnlyList<FrameAlignmentResult?> path =
			FrameAlignment.FindBestSmoothPath(
				new[] {
					Results(
						(-1, 4, 0.0400d),
						(0, 2, 0.0300d),
						(1, 0, 0.0200d)),
					Results(
						(-1, 0, 0.0200d),
						(0, 2, 0.0300d),
						(1, 4, 0.0400d)),
					Results(
						(-1, 4, 0.0400d),
						(0, 0, 0.0200d),
						(1, 4, 0.0400d)),
				},
				new[] { 0, 0, 0 });

		Assert.Equal(new[] { 1, -1, 0 },
			path.Select(result => result!.Value.Offset));
	}

	[Fact]
	public void GradientDifference_IgnoresUniformBrightnessShift() {
		var reference = new byte[32 * 32];
		var brighter = new byte[32 * 32];
		for (int y = 0; y < 32; y++) {
			for (int x = 0; x < 32; x++) {
				int index = (y * 32) + x;
				reference[index] = (byte)(50 + x + y);
				brighter[index] = (byte)(70 + x + y);
			}
		}

		Assert.True(
			FrameAlignment.CalculateMeanAbsoluteDifference(
				reference,
				brighter) > 0.07d);
		Assert.Equal(
			0d,
			FrameAlignment.CalculateGradientDifference(
				reference,
				brighter,
				32),
			10);
	}

	[Fact]
	public void CoarseAndFineOffsets_CoverFramesBetweenCoarseSamples() {
		const int radius = 30;
		IReadOnlyList<int> coarse =
			FrameAlignment.BuildCoarseOffsets(radius);
		int step = FrameAlignment.CalculateCoarseStep(radius);
		IReadOnlyList<int> fine = FrameAlignment.BuildFineOffsets(
			center: 14,
			coarseStep: step,
			radius: radius,
			excludedOffsets: coarse.ToHashSet());

		Assert.Contains(-radius, coarse);
		Assert.Contains(0, coarse);
		Assert.Contains(radius, coarse);
		Assert.Contains(15, fine);
		Assert.True(coarse.Count < radius * 2 + 1);
	}

	[Fact]
	public void ProgressiveOffsets_StartNearAndKeepMidRangeAnchors() {
		IReadOnlyList<IReadOnlyList<int>> batches =
			FrameAlignment.BuildProgressiveOffsetBatches(90);

		Assert.Equal(new[] { -1, 1 }, batches[0]);
		Assert.Contains(batches, batch =>
			batch.SequenceEqual(new[] { -30, 30 }));
		Assert.Contains(batches, batch =>
			batch.SequenceEqual(new[] { -60, 60 }));
		Assert.Equal(new[] { -90, 90 }, batches[^1]);
	}

	[Fact]
	public void DurationHintOffsets_ProbeHintAndOppositeSign() {
		IReadOnlyList<IReadOnlyList<int>> batches =
			FrameAlignment.BuildDurationHintOffsetBatches(75, 90);

		Assert.Equal(new[] { 75, -75 }, batches[0]);
		Assert.Contains(batches, batch =>
			batch.SequenceEqual(new[] { 59, 91, -91, -59 }
				.Where(offset => Math.Abs(offset) <= 90)));
	}

	[Fact]
	public void DurationHintOffsets_ClampToRadius() {
		IReadOnlyList<IReadOnlyList<int>> batches =
			FrameAlignment.BuildDurationHintOffsetBatches(140, 90);

		Assert.Equal(new[] { 90, -90 }, batches[0]);
	}

	[Fact]
	public void RefinementOffsets_BisectNearestTestedNeighbors() {
		var tested = new HashSet<int> {
			-30, -16, -8, 0, 8, 16, 30
		};

		IReadOnlyList<int> offsets =
			FrameAlignment.BuildRefinementOffsets(
				center: -16,
				testedOffsets: tested,
				radius: 30);

		Assert.Equal(new[] { -23, -12 }, offsets);
	}
}
