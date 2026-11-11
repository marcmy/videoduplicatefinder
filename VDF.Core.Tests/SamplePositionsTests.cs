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

namespace VDF.Core.Tests;

/// <summary>
/// The relative positions frames are sampled at. The scan, the thumbnail reload and the
/// Web UI's frame stepping all read them from <see cref="ScanEngine.BuildSamplePositions(int, SamplePositionLayout)"/>;
/// the values are part of the database contract (gray bytes are keyed by them), so a
/// change here re-extracts every library. The layout is nested so that changing the
/// frame count reuses every frame the old and new counts share.
/// </summary>
public class SamplePositionsTests {
	[Fact]
	public void OneFrame_IsTheMidpoint() =>
		Assert.Equal(new[] { 0.5f }, ScanEngine.BuildSamplePositions(1, SamplePositionLayout.Nested));

	[Fact]
	public void ThreeFrames_AreTheQuarters() =>
		Assert.Equal(new[] { 0.25f, 0.5f, 0.75f }, ScanEngine.BuildSamplePositions(3, SamplePositionLayout.Nested));

	[Theory]
	[InlineData(0)]
	[InlineData(-1)]
	public void NoFrames_IsEmptyInsteadOfThrowing(int count) =>
		Assert.Empty(ScanEngine.BuildSamplePositions(count, SamplePositionLayout.Nested));

	[Fact]
	public void SevenFrames_AreTheEighths() =>
		Assert.Equal(new[] { 0.125f, 0.25f, 0.375f, 0.5f, 0.625f, 0.75f, 0.875f }, ScanEngine.BuildSamplePositions(7, SamplePositionLayout.Nested));

	static List<float> LegacyPositions(int count) {
		// The layout before nested positions: k / (n + 1), float-accumulated.
		var positions = new List<float>();
		float counter = 0f;
		for (int i = 0; i < count; i++) {
			counter += 1.0F / (count + 1);
			positions.Add(counter);
		}
		return positions;
	}

	[Theory]
	[InlineData(1)]
	[InlineData(3)]
	[InlineData(7)]
	[InlineData(15)]
	[InlineData(31)]
	public void CountsOfTwoPowerMinusOne_AreTheSameInBothLayouts(int count) =>
		Assert.Equal(LegacyPositions(count), ScanEngine.BuildSamplePositions(count, SamplePositionLayout.Nested));

	[Theory]
	[InlineData(2)]
	[InlineData(5)]
	[InlineData(6)]
	[InlineData(50)]
	public void LegacyLayout_ReproducesTheOldKeysBitForBit(int count) =>
		// Databases scanned before nested positions keep every cached frame at the same count.
		Assert.Equal(LegacyPositions(count), ScanEngine.BuildSamplePositions(count, SamplePositionLayout.Legacy));

	[Fact]
	public void Positions_AreNested_SoAnyCountChangeReusesTheSmallerSet() {
		for (int larger = 1; larger <= 64; larger++) {
			var big = new HashSet<float>(ScanEngine.BuildSamplePositions(larger, SamplePositionLayout.Nested));
			Assert.Equal(larger, big.Count);
			for (int smaller = 1; smaller < larger; smaller++)
				Assert.All(ScanEngine.BuildSamplePositions(smaller, SamplePositionLayout.Nested), p => Assert.Contains(p, big));
		}
	}

	[Theory]
	[InlineData(2)]
	[InlineData(4)]
	[InlineData(5)]
	[InlineData(6)]
	[InlineData(10)]
	[InlineData(50)]
	[InlineData(200)]
	public void Positions_AreSortedInsideTheVideo_AndEvenWithinFactorTwo(int count) {
		var positions = ScanEngine.BuildSamplePositions(count, SamplePositionLayout.Nested);
		Assert.Equal(count, positions.Count);
		Assert.Equal(positions.OrderBy(p => p), positions);
		Assert.True(positions[0] > 0f && positions[^1] < 1f);
		var gaps = new List<float> { positions[0] };
		for (int i = 1; i < count; i++)
			gaps.Add(positions[i] - positions[i - 1]);
		gaps.Add(1f - positions[^1]);
		Assert.True(gaps.Max() <= 2f * gaps.Min() + 1e-6f, $"gaps {gaps.Min()}..{gaps.Max()}");
	}

	static FileEntry VideoWithFrames(double durationSeconds, IEnumerable<float> positions, double relativeNoise = 0d) {
		var entry = new FileEntry {
			_Path = @"X:\v.mp4",
			Folder = @"X:\",
			mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(durationSeconds) },
			invalid = false,
		};
		foreach (float p in positions) {
			double key = entry.GetGrayBytesIndex(p, null) * (1d + relativeNoise);
			entry.grayBytes[key] = new byte[32 * 32];
			entry.PHashes[key] = 42UL;
		}
		return entry;
	}

	[Theory]
	[InlineData(5)]
	[InlineData(50)]
	public void FloatNoiseKeys_SnapOntoExactKeys(int count) {
		// Keys a few ulps off (older position math, AI sidecar records) still count as cached.
		var positions = ScanEngine.BuildSamplePositions(count, SamplePositionLayout.Nested);
		var entry = VideoWithFrames(3600.123, positions, relativeNoise: 3e-6);

		Assert.Equal(count, entry.SnapCachedPositionKeys(positions, null));

		Assert.Equal(count, entry.grayBytes.Count);
		Assert.Equal(count, entry.PHashes.Count);
		foreach (float p in positions) {
			double key = entry.GetGrayBytesIndex(p, null);
			Assert.True(ScanEngine.HasUsableCachedGray(entry, key));
			Assert.Equal(42UL, entry.PHashes[key]);
		}
	}

	[Fact]
	public void Snap_NeverMergesDifferentPositions() {
		// The old 4-frame layout (fifths) shares no position with the new 5-frame layout.
		var entry = VideoWithFrames(7200, LegacyPositions(4));
		var five = ScanEngine.BuildSamplePositions(5, SamplePositionLayout.Nested);

		Assert.Equal(0, entry.SnapCachedPositionKeys(five, null));
		foreach (float p in five)
			Assert.False(entry.grayBytes.ContainsKey(entry.GetGrayBytesIndex(p, null)));
		Assert.Equal(4, entry.grayBytes.Count);
	}

	[Theory]
	[InlineData(11, 5)]
	[InlineData(5, 4)]
	[InlineData(7, 2)]
	public void CompareSnapshot_LowerCount_NeedsNoDecoding(int cached, int wanted) {
		var engine = new ScanEngine { Settings = new Settings { ThumbnailCount = wanted, UsePHashing = false, CombineGrayscaleAndPHash = false } };
		engine.positionList.Clear();
		engine.positionList.AddRange(ScanEngine.BuildSamplePositions(wanted, SamplePositionLayout.Nested));
		var entry = VideoWithFrames(1234.5, ScanEngine.BuildSamplePositions(cached, SamplePositionLayout.Nested));

		Assert.True(engine.TryBuildCompareSnapshot(entry, usePHashing: true));
		Assert.Equal(wanted, entry.comparePHashes!.Length);
	}

	[Fact]
	public void ThumbnailReload_UsesTheSamePositionsAsTheScan() {
		var engine = new ScanEngine();
		engine.Settings.ThumbnailCount = 7;

		engine.EnsureThumbnailPositions();

		// 7 frames sit at the same positions in both layouts, whatever the shared database says.
		Assert.Equal(ScanEngine.BuildSamplePositions(7, SamplePositionLayout.Legacy), engine.positionList);
	}
}
