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
/// Web UI's frame stepping all read them from <see cref="ScanEngine.BuildSamplePositions"/>;
/// the values are part of the database contract (gray bytes are keyed by them), so a
/// change here re-extracts every library.
/// </summary>
public class SamplePositionsTests {
	[Fact]
	public void OneFrame_IsTheMidpoint() =>
		Assert.Equal(new[] { 0.5f }, ScanEngine.BuildSamplePositions(1));

	[Fact]
	public void ThreeFrames_AreTheQuarters() =>
		Assert.Equal(new[] { 0.25f, 0.5f, 0.75f }, ScanEngine.BuildSamplePositions(3));

	[Theory]
	[InlineData(0)]
	[InlineData(-1)]
	public void NoFrames_IsEmptyInsteadOfThrowing(int count) =>
		Assert.Empty(ScanEngine.BuildSamplePositions(count));

	[Fact]
	public void Positions_AreExactFractions() {
		var positions = ScanEngine.BuildSamplePositions(50);

		Assert.Equal(50, positions.Count);
		for (int i = 0; i < 50; i++)
			Assert.Equal((float)((i + 1) / 51d), positions[i]);
		Assert.True(positions[0] > 0f && positions[^1] < 1f);
	}

	[Theory]
	[InlineData(5, 11)]
	[InlineData(11, 5)]
	[InlineData(2, 5)]
	[InlineData(4, 9)]
	[InlineData(3, 7)]
	public void SharedPositions_OfTwoCounts_AreTheSameFloat(int a, int b) {
		// k/(a+1) == m/(b+1) must give identical floats so the cached frame is reused
		// when the frame count changes (float accumulation broke 2 of 5 for 5 <-> 11).
		var pa = ScanEngine.BuildSamplePositions(a);
		var pb = new HashSet<float>(ScanEngine.BuildSamplePositions(b));
		int shared = 0;
		for (int k = 1; k <= a; k++)
			for (int m = 1; m <= b; m++)
				if (k * (b + 1) == m * (a + 1)) shared++;
		Assert.Equal(shared, pa.Count(pb.Contains));
	}

	static List<float> AccumulatedPositions(int count) {
		// How databases written before exact positions were keyed.
		var positions = new List<float>();
		float counter = 0f;
		for (int i = 0; i < count; i++) {
			counter += 1.0F / (count + 1);
			positions.Add(counter);
		}
		return positions;
	}

	static FileEntry VideoWithFrames(double durationSeconds, IEnumerable<float> positions) {
		var entry = new FileEntry {
			_Path = @"X:\v.mp4",
			Folder = @"X:\",
			mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(durationSeconds) },
			invalid = false,
		};
		foreach (float p in positions) {
			double key = entry.GetGrayBytesIndex(p, null);
			entry.grayBytes[key] = new byte[32 * 32];
			entry.PHashes[key] = 42UL;
		}
		return entry;
	}

	[Theory]
	[InlineData(5)]
	[InlineData(11)]
	[InlineData(50)]
	[InlineData(200)]
	public void OldAccumulatedKeys_SnapOntoExactKeys(int count) {
		var entry = VideoWithFrames(3600.123, AccumulatedPositions(count));
		var exact = ScanEngine.BuildSamplePositions(count);

		entry.SnapCachedPositionKeys(exact, null);

		Assert.Equal(count, entry.grayBytes.Count);
		Assert.Equal(count, entry.PHashes.Count);
		foreach (float p in exact) {
			double key = entry.GetGrayBytesIndex(p, null);
			Assert.True(ScanEngine.HasUsableCachedGray(entry, key));
			Assert.Equal(42UL, entry.PHashes[key]);
		}
	}

	[Fact]
	public void Snap_NeverMergesDifferentPositions() {
		// 4 -> 5 shares no position: nothing may be treated as cached.
		var entry = VideoWithFrames(7200, ScanEngine.BuildSamplePositions(4));
		var five = ScanEngine.BuildSamplePositions(5);

		Assert.Equal(0, entry.SnapCachedPositionKeys(five, null));
		foreach (float p in five)
			Assert.False(entry.grayBytes.ContainsKey(entry.GetGrayBytesIndex(p, null)));
		Assert.Equal(4, entry.grayBytes.Count);
	}

	[Fact]
	public void CompareSnapshot_ReusesSubsetFromLargerCount() {
		// 11 frames cached (positions k/12); a 5-frame scan (k/6) needs no decoding.
		var engine = new ScanEngine { Settings = new Settings { ThumbnailCount = 5, UsePHashing = false, CombineGrayscaleAndPHash = false } };
		engine.positionList.Clear();
		engine.positionList.AddRange(ScanEngine.BuildSamplePositions(5));
		var entry = VideoWithFrames(1234.5, AccumulatedPositions(11));

		Assert.True(engine.TryBuildCompareSnapshot(entry, usePHashing: true));
		Assert.Equal(5, entry.comparePHashes!.Length);
	}

	[Fact]
	public void ThumbnailReload_UsesTheSamePositionsAsTheScan() {
		var engine = new ScanEngine();
		engine.Settings.ThumbnailCount = 7;

		engine.EnsureThumbnailPositions();

		Assert.Equal(ScanEngine.BuildSamplePositions(7), engine.positionList);
	}
}
