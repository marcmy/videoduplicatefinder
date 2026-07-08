// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using VDF.GUI.ViewModels;

namespace VDF.GUI.Tests;

public class ThumbnailComparerModeTests {
	[Theory]
	[InlineData(CompareMode.Single, false, false, false)]
	[InlineData(CompareMode.Single, true, true, false)]
	[InlineData(CompareMode.Swipe, true, true, false)]
	[InlineData(CompareMode.SideBySide, true, true, false)]
	[InlineData(CompareMode.Stacked, true, true, false)]
	[InlineData(CompareMode.Swipe, true, false, true)]
	[InlineData(CompareMode.SideBySide, false, true, true)]
	public void DualModesDependOnSelections_NotBitmapLoadTiming(
		CompareMode mode, bool hasA, bool hasB, bool expected) =>
		Assert.Equal(expected, ThumbnailComparerVM.ShouldForceSingleView(mode, hasA, hasB));
}
