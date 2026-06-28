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
}
