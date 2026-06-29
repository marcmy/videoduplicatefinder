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

	[Theory]
	[InlineData(3, 0.06, true)]
	[InlineData(4, 0.01, false)]
	[InlineData(1, 0.07, false)]
	public void Confidence_RequiresHashAndPixelAgreement(
		int hashDistance,
		double meanDifference,
		bool expected) {
		var result = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: hashDistance,
			MeanAbsoluteDifference: meanDifference);

		Assert.Equal(expected, FrameAlignment.IsConfident(result));
	}
	[Fact]
	public void NonZeroCandidate_MustClearlyBeatZeroBaseline() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 5,
			MeanAbsoluteDifference: 0.070d);
		var suspiciousFarMatch = new FrameAlignmentResult(
			Offset: 29,
			HashDistance: 3,
			MeanAbsoluteDifference: 0.058d);

		Assert.False(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				suspiciousFarMatch));
	}

	[Fact]
	public void NearbyCandidate_CanBeatZeroWithClearImprovement() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 6,
			MeanAbsoluteDifference: 0.080d);
		var aligned = new FrameAlignmentResult(
			Offset: -1,
			HashDistance: 2,
			MeanAbsoluteDifference: 0.050d);

		Assert.True(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				aligned));
	}

	[Fact]
	public void LargeOffset_RequiresStrongEvidence() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 12,
			MeanAbsoluteDifference: 0.120d);
		var aligned = new FrameAlignmentResult(
			Offset: 29,
			HashDistance: 1,
			MeanAbsoluteDifference: 0.035d);

		Assert.True(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				aligned));
	}

	[Fact]
	public void RelativeImprovementCanOverrideAbsoluteConfidence() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 10,
			MeanAbsoluteDifference: 0.110d);
		var candidate = new FrameAlignmentResult(
			Offset: 2,
			HashDistance: 4,
			MeanAbsoluteDifference: 0.040d);

		Assert.True(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				candidate));
	}
	[Theory]
	[InlineData(1, 0.018, true)]
	[InlineData(2, 0.010, false)]
	[InlineData(1, 0.025, false)]
	public void NearlyExact_RequiresVeryCloseAgreement(
		int hashDistance,
		double meanDifference,
		bool expected) {
		var result = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: hashDistance,
			MeanAbsoluteDifference: meanDifference);

		Assert.Equal(
			expected,
			FrameAlignment.IsNearlyExact(result));
	}

	[Fact]
	public void NearbyTinyImprovement_DoesNotOverrideZero() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 5,
			MeanAbsoluteDifference: 0.075d);
		var candidate = new FrameAlignmentResult(
			Offset: 1,
			HashDistance: 4,
			MeanAbsoluteDifference: 0.068d);

		Assert.False(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				candidate));
	}

	[Fact]
	public void StrongImprovementCanOverrideConfidentZero() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 3,
			MeanAbsoluteDifference: 0.050d);
		var candidate = new FrameAlignmentResult(
			Offset: 2,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.025d);

		Assert.True(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				candidate));
	}

	[Fact]
	public void WeakImprovementDoesNotOverrideConfidentZero() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 3,
			MeanAbsoluteDifference: 0.050d);
		var candidate = new FrameAlignmentResult(
			Offset: -1,
			HashDistance: 1,
			MeanAbsoluteDifference: 0.044d);

		Assert.False(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				candidate));
	}

	[Fact]
	public void BestImprovementIgnoresHashBestCandidateThatFailsPixelGate() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 7,
			MeanAbsoluteDifference: 0.080d);
		var hashBest = new FrameAlignmentResult(
			Offset: -1,
			HashDistance: 2,
			MeanAbsoluteDifference: 0.079d);
		var aligned = new FrameAlignmentResult(
			Offset: 2,
			HashDistance: 4,
			MeanAbsoluteDifference: 0.065d);

		FrameAlignmentResult? result =
			FrameAlignment.FindBestClearImprovementOverZero(
				zero,
				new[] { zero, hashBest, aligned });

		Assert.NotNull(result);
		Assert.Equal(2, result.Value.Offset);
	}

	[Fact]
	public void BestImprovementReturnsNullWhenOnlyTinyNearbyShiftImproves() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 5,
			MeanAbsoluteDifference: 0.075d);
		var candidate = new FrameAlignmentResult(
			Offset: 1,
			HashDistance: 4,
			MeanAbsoluteDifference: 0.068d);

		FrameAlignmentResult? result =
			FrameAlignment.FindBestClearImprovementOverZero(
				zero,
				new[] { zero, candidate });

		Assert.Null(result);
	}

	[Fact]
	public void NearbyNudgeAllowsOneFrameDetailImprovement() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0300d);
		var nudge = new FrameAlignmentResult(
			Offset: 1,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0302d);

		FrameAlignmentResult? result = FrameAlignment.FindBestNearbyNudge(
			zero,
			new[] { zero, nudge },
			new Dictionary<int, double> {
				[0] = 0.0500d,
				[-1] = 0.0484d,
				[1] = 0.0475d,
			});

		Assert.NotNull(result);
		Assert.Equal(1, result.Value.Offset);
	}

	[Fact]
	public void NearbyNudgeAllowsSubtleUniqueDetailWinner() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0300d);
		var opposite = new FrameAlignmentResult(
			Offset: -1,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0301d);
		var nudge = new FrameAlignmentResult(
			Offset: 1,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0300d);

		FrameAlignmentResult? result = FrameAlignment.FindBestNearbyNudge(
			zero,
			new[] { opposite, zero, nudge },
			new Dictionary<int, double> {
				[-1] = 0.0040d,
				[0] = 0.0037d,
				[1] = 0.0035d,
			});

		Assert.NotNull(result);
		Assert.Equal(1, result.Value.Offset);
	}

	[Fact]
	public void NearbyNudgeRejectsOppositeSideTie() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0411d);
		var candidate = new FrameAlignmentResult(
			Offset: -1,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0405d);
		var opposite = new FrameAlignmentResult(
			Offset: 1,
			HashDistance: 2,
			MeanAbsoluteDifference: 0.0401d);

		FrameAlignmentResult? result = FrameAlignment.FindBestNearbyNudge(
			zero,
			new[] { candidate, zero, opposite },
			new Dictionary<int, double> {
				[-1] = 0.0142d,
				[0] = 0.0146d,
				[1] = 0.0142d,
			});

		Assert.Null(result);
	}

	[Fact]
	public void NearbyNudgeRejectsHashRegression() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 2,
			MeanAbsoluteDifference: 0.045d);
		var misleading = new FrameAlignmentResult(
			Offset: 1,
			HashDistance: 6,
			MeanAbsoluteDifference: 0.028d);

		FrameAlignmentResult? result = FrameAlignment.FindBestNearbyNudge(
			zero,
			new[] { zero, misleading },
			new Dictionary<int, double> {
				[0] = 0.080d,
				[1] = 0.040d,
			});

		Assert.Null(result);
	}

	[Fact]
	public void NearbyNudgeRejectsFlatDetailSignal() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0300d);
		var candidate = new FrameAlignmentResult(
			Offset: 1,
			HashDistance: 0,
			MeanAbsoluteDifference: 0.0299d);

		FrameAlignmentResult? result = FrameAlignment.FindBestNearbyNudge(
			zero,
			new[] { zero, candidate },
			new Dictionary<int, double> {
				[0] = 0.0500d,
				[1] = 0.0499d,
			});

		Assert.Null(result);
	}

	[Fact]
	public void ConsensusOffsetKeepsSingleAnchorDecision() {
		Assert.Equal(
			3,
			FrameAlignment.SelectConsensusOffset(
				3,
				new[] { 3 }));
	}

	[Fact]
	public void ConsensusOffsetRequiresNeighborAgreement() {
		Assert.Equal(
			0,
			FrameAlignment.SelectConsensusOffset(
				3,
				new[] { 3, 0, -3 }));
	}

	[Fact]
	public void ConsensusOffsetAcceptsMatchingNeighborVote() {
		Assert.Equal(
			3,
			FrameAlignment.SelectConsensusOffset(
				3,
				new[] { 3, 3, 0 }));
	}

	[Fact]
	public void ConsensusOffsetKeepsOneFrameLocalNudge() {
		Assert.Equal(
			1,
			FrameAlignment.SelectConsensusOffset(
				1,
				new[] { 1, 0, -1 }));
	}

	[Fact]
	public void ConsensusOffsetDoesNotLetNeighborsOverridePrimaryZero() {
		Assert.Equal(
			0,
			FrameAlignment.SelectConsensusOffset(
				0,
				new[] { 0, 3, 3 }));
	}

	[Fact]
	public void ImprovementInOnlyOneMetric_DoesNotOverrideZero() {
		var zero = new FrameAlignmentResult(
			Offset: 0,
			HashDistance: 7,
			MeanAbsoluteDifference: 0.080d);
		var candidate = new FrameAlignmentResult(
			Offset: 2,
			HashDistance: 3,
			MeanAbsoluteDifference: 0.079d);

		Assert.False(
			FrameAlignment.IsClearImprovementOverZero(
				zero,
				candidate));
	}
}
