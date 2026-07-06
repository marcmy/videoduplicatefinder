// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using VDF.Core;

namespace VDF.Core.Tests;

public class MatchingConcurrencyTests {
	[Theory]
	[InlineData(0, 1, 1)]
	[InlineData(0, 2, 1)]
	[InlineData(0, 4, 3)]
	[InlineData(0, 8, 6)]
	[InlineData(0, 16, 13)]
	[InlineData(0, 32, 26)]
	[InlineData(-1, 8, 6)]
	public void CalculateMatchingMaxDegreeOfParallelism_AutoLeavesCpuHeadroom(
		int configured,
		int processorCount,
		int expected) {
		Assert.Equal(expected,
			ScanEngine.CalculateMatchingMaxDegreeOfParallelism(configured, processorCount));
	}

	[Theory]
	[InlineData(1, 16, 1)]
	[InlineData(6, 16, 6)]
	[InlineData(99, 16, 16)]
	public void CalculateMatchingMaxDegreeOfParallelism_ExplicitValueIsHonoredAndClamped(
		int configured,
		int processorCount,
		int expected) {
		Assert.Equal(expected,
			ScanEngine.CalculateMatchingMaxDegreeOfParallelism(configured, processorCount));
	}
}
