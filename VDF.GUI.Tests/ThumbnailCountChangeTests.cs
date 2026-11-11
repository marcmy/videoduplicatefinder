// /*
//     Copyright (C) 2026 0x90d
//     This file is part of VideoDuplicateFinder
//     VideoDuplicateFinder is free software: you can redistribute it and/or modify
//     it under the terms of the GNU Affero General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
//     VideoDuplicateFinder is distributed in the hope that it will be useful,
//     but WITHOUT ANY WARRANTY without even the implied warranty of
//     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//     GNU Affero General Public License for more details.
//     You should have received a copy of the GNU Affero General Public License
//     along with VideoDuplicateFinder.  If not, see <http://www.gnu.org/licenses/>.
// */
//

using VDF.GUI.ViewModels;

namespace VDF.GUI.Tests {
	/// <summary>
	/// Thumbnail-count stepper vs. quick-rescan readiness. Regression: the warning
	/// dialog spawned once per arrow click, stacking boxes until one OK was clicked,
	/// because the readiness flag was only reset after the dialog await. The decision
	/// is evaluated up front; the flag change is applied before the dialog shows.
	/// </summary>
	public class ThumbnailCountChangeTests {

		[Fact]
		public void RaisingTheCount_WarnsOnce_AndDropsReadiness() {
			var (ready, warn) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: true, gatheredThumbnailCount: 2, nestedLayout: true, newValue: 3, wasReadyToCompare: true);
			Assert.False(ready);
			Assert.True(warn);
		}

		[Fact]
		public void FurtherClicksWhileNotReady_DoNotWarnAgain() {
			// The regression: click 2 .. n each spawned another message box. Once the
			// flag is down, subsequent changes must stay silent.
			var (ready, warn) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: true, gatheredThumbnailCount: 2, nestedLayout: true, newValue: 4, wasReadyToCompare: false);
			Assert.False(ready);
			Assert.False(warn);
		}

		[Fact]
		public void ChangingBackToTheScannedCount_RestoresReadinessSilently() {
			var (ready, warn) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: true, gatheredThumbnailCount: 2, nestedLayout: true, newValue: 2, wasReadyToCompare: false);
			Assert.True(ready);
			Assert.False(warn);
		}

		[Fact]
		public void WithoutAGatheredScan_NeverReady() {
			var (ready, warn) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: false, gatheredThumbnailCount: 2, nestedLayout: true, newValue: 2, wasReadyToCompare: false);
			Assert.False(ready);
			Assert.False(warn);
		}

		[Fact]
		public void LoweringTheCount_StaysReady_WithoutWarning() {
			// Sample positions are nested: 3 frames are a subset of the 7 already sampled.
			var (ready, warn) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: true, gatheredThumbnailCount: 7, nestedLayout: true, newValue: 3, wasReadyToCompare: true);
			Assert.True(ready);
			Assert.False(warn);
		}

		[Fact]
		public void ZeroFrames_IsNeverReady() {
			var (ready, _) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: true, gatheredThumbnailCount: 7, nestedLayout: true, newValue: 0, wasReadyToCompare: false);
			Assert.False(ready);
		}

		[Fact]
		public void LegacyLayout_LoweringTheCount_NeedsAFullScan() {
			// k/(n+1) positions differ for every count: 3 frames are not a subset of 6.
			var (ready, warn) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: true, gatheredThumbnailCount: 6, nestedLayout: false, newValue: 3, wasReadyToCompare: true);
			Assert.False(ready);
			Assert.True(warn);
		}

		[Fact]
		public void LegacyLayout_TheScannedCount_StaysReady() {
			var (ready, _) = MainWindowVM.EvaluateThumbnailCountChange(
				isGathered: true, gatheredThumbnailCount: 6, nestedLayout: false, newValue: 6, wasReadyToCompare: false);
			Assert.True(ready);
		}
	}
}
