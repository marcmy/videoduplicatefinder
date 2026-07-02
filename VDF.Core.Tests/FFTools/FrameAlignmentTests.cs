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
	public void FindBestConfidentResult_KeepsBaselineOnTinyFarImprovement() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(new[] {
				new FrameAlignmentResult(0, 10, 0.0500d),
				new FrameAlignmentResult(80, 9, 0.0498d),
			});

		Assert.NotNull(result);
		Assert.Equal(0, result.Value.Offset);
	}

	[Fact]
	public void FindBestConfidentResult_AcceptsClearFarImprovement() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(new[] {
				new FrameAlignmentResult(0, 20, 0.0800d),
				new FrameAlignmentResult(80, 12, 0.0500d),
			});

		Assert.NotNull(result);
		Assert.Equal(80, result.Value.Offset);
	}

	[Fact]
	public void FindBestConfidentResult_AcceptsNearSmallImprovement() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(new[] {
				new FrameAlignmentResult(0, 5, 0.0300d),
				new FrameAlignmentResult(1, 5, 0.0293d),
			});

		Assert.NotNull(result);
		Assert.Equal(1, result.Value.Offset);
	}

	[Fact]
	public void FindBestConfidentResult_SkipsWeakBestForConfidentNearOffset() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(new[] {
				new FrameAlignmentResult(0, 10, 0.0500d),
				new FrameAlignmentResult(80, 9, 0.0498d),
				new FrameAlignmentResult(1, 10, 0.0493d),
			});

		Assert.NotNull(result);
		Assert.Equal(1, result.Value.Offset);
	}

	[Fact]
	public void FindBestConfidentResult_RejectsAmbiguousFarAlternative() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(new[] {
				new FrameAlignmentResult(0, 20, 0.0800d),
				new FrameAlignmentResult(80, 12, 0.0500d),
				new FrameAlignmentResult(-70, 12, 0.0505d),
			});

		Assert.NotNull(result);
		Assert.Equal(0, result.Value.Offset);
	}

	[Fact]
	public void FindBestConfidentResult_RejectsWeakOppositeDurationHintDirection() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(
				new[] {
					new FrameAlignmentResult(0, 12, 0.0875d),
					new FrameAlignmentResult(-77, 10, 0.0568d),
					new FrameAlignmentResult(2, 12, 0.0843d),
				},
				preferredOffset: 90);

		Assert.NotNull(result);
		Assert.Equal(2, result.Value.Offset);
	}

	[Fact]
	public void FindBestConfidentResult_AcceptsStrongOppositeDurationHintDirection() {
		FrameAlignmentResult? result =
			FrameAlignment.FindBestConfidentResult(
				new[] {
					new FrameAlignmentResult(0, 20, 0.0900d),
					new FrameAlignmentResult(-77, 4, 0.0300d),
					new FrameAlignmentResult(2, 20, 0.0870d),
				},
				preferredOffset: 90);

		Assert.NotNull(result);
		Assert.Equal(-77, result.Value.Offset);
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
	public void ProgressiveOffsets_StartNearAndReachRadius() {
		IReadOnlyList<IReadOnlyList<int>> batches =
			FrameAlignment.BuildProgressiveOffsetBatches(30);

		Assert.Equal(new[] { -1, 1 }, batches[0]);
		Assert.Equal(new[] { -2, 2 }, batches[1]);
		Assert.Equal(new[] { -30, 30 }, batches[^1]);
		Assert.Equal(6, batches.Count);
	}

	[Fact]
	public void ProgressiveOffsets_ExtendedRadiusKeepsMidRangeAnchors() {
		IReadOnlyList<IReadOnlyList<int>> batches =
			FrameAlignment.BuildProgressiveOffsetBatches(90);

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
