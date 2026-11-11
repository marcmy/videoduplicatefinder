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

using VDF.Core.Utils;
using VDF.TestSupport;

namespace VDF.Core.Tests;

/// <summary>
/// The database header records which sample-position layout its frames were keyed with.
/// A database keeps its layout while the frame count stays the same, so upgrading to the
/// nested layout costs no decoding; changing the count (or starting a new database) moves
/// to the nested layout, after which further count changes are cheap.
/// </summary>
[Collection("DatabaseUtils")] // reads and writes the shared static database and its header
public class SamplePositionLayoutDatabaseTests : IDisposable {
	readonly List<FileEntry> saved;
	readonly (int Layout, int Count) savedHeader;

	public SamplePositionLayoutDatabaseTests() {
		saved = DatabaseUtils.Database.ToList();
		savedHeader = DatabaseUtils.SampleLayoutHeader;
		DatabaseUtils.Database.Clear();
		DatabaseUtils.SampleLayoutHeader = (0, 0);
	}

	public void Dispose() {
		DatabaseUtils.Database.Clear();
		foreach (var entry in saved)
			DatabaseUtils.Database.Add(entry);
		DatabaseUtils.SampleLayoutHeader = savedHeader;
	}

	static FileEntry VideoSampledAt(string name, IEnumerable<float> positions) {
		var entry = new FileEntry {
			_Path = TestPaths.Native(@"C:\vdf-layout\" + name),
			Folder = TestPaths.Native(@"C:\vdf-layout"),
			mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(1234.5) },
			invalid = false,
		};
		foreach (float p in positions)
			entry.grayBytes[entry.GetGrayBytesIndex(p, null)] = new byte[32 * 32];
		return entry;
	}

	static ScanEngine Engine(int thumbnailCount) =>
		new() { Settings = new Settings { ThumbnailCount = thumbnailCount } };

	[Fact]
	public void LegacyDatabase_SameCount_KeepsLegacyPositions_AndDecodesNothing() {
		// A database from before nested positions: no recorded layout or count, frames at k/7.
		var entry = VideoSampledAt("six.mp4", ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Legacy));
		DatabaseUtils.Database.Add(entry);
		var engine = Engine(6);

		engine.AdoptDatabaseSampleLayout();

		Assert.Equal(SamplePositionLayout.Legacy, engine.GatheredSampleLayout);
		Assert.Equal(ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Legacy), engine.positionList);
		// The scan's "already sampled" gate: every position is cached, so nothing is decoded.
		Assert.All(engine.positionList, p => Assert.True(ScanEngine.HasUsableCachedGray(entry, entry.GetGrayBytesIndex(p, null))));
		Assert.Equal(((int)SamplePositionLayout.Legacy, 6), DatabaseUtils.SampleLayoutHeader);
	}

	[Fact]
	public void LegacyDatabase_RecordedCount_StaysLegacyOnLaterScans() {
		DatabaseUtils.Database.Add(VideoSampledAt("six.mp4", ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Legacy)));
		DatabaseUtils.SampleLayoutHeader = ((int)SamplePositionLayout.Legacy, 6);
		var engine = Engine(6);

		engine.AdoptDatabaseSampleLayout();

		Assert.Equal(SamplePositionLayout.Legacy, engine.GatheredSampleLayout);
	}

	[Fact]
	public void LegacyDatabase_ChangedCount_SwitchesToNested_AndRecordsIt() {
		DatabaseUtils.Database.Add(VideoSampledAt("six.mp4", ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Legacy)));
		DatabaseUtils.SampleLayoutHeader = ((int)SamplePositionLayout.Legacy, 6);
		var engine = Engine(5);

		engine.AdoptDatabaseSampleLayout();

		Assert.Equal(SamplePositionLayout.Nested, engine.GatheredSampleLayout);
		Assert.Equal(ScanEngine.BuildSamplePositions(5, SamplePositionLayout.Nested), engine.positionList);
		Assert.Equal(((int)SamplePositionLayout.Nested, 5), DatabaseUtils.SampleLayoutHeader);
	}

	[Fact]
	public void NestedDatabase_StaysNested_ForAnyCount() {
		DatabaseUtils.Database.Add(VideoSampledAt("five.mp4", ScanEngine.BuildSamplePositions(5, SamplePositionLayout.Nested)));
		DatabaseUtils.SampleLayoutHeader = ((int)SamplePositionLayout.Nested, 5);

		var lower = Engine(3);
		lower.AdoptDatabaseSampleLayout();
		Assert.Equal(SamplePositionLayout.Nested, lower.GatheredSampleLayout);

		var higher = Engine(9);
		higher.AdoptDatabaseSampleLayout();
		Assert.Equal(SamplePositionLayout.Nested, higher.GatheredSampleLayout);
	}

	[Fact]
	public void QuickRescan_LowerCount_OnlyWithTheNestedLayout() {
		DatabaseUtils.Database.Add(VideoSampledAt("six.mp4", ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Legacy)));
		var legacy = Engine(6);
		legacy.AdoptDatabaseSampleLayout();
		Assert.True(legacy.CanQuickRescanWith(6));
		Assert.False(legacy.CanQuickRescanWith(3)); // k/4 is not among the k/7 frames

		DatabaseUtils.SampleLayoutHeader = ((int)SamplePositionLayout.Nested, 6);
		var nested = Engine(6);
		nested.AdoptDatabaseSampleLayout();
		Assert.True(nested.CanQuickRescanWith(3));
		Assert.False(nested.CanQuickRescanWith(7));
		Assert.False(nested.CanQuickRescanWith(0));
	}

	[Fact]
	public void NewDatabase_IsNested() {
		var engine = Engine(6);

		engine.AdoptDatabaseSampleLayout();

		Assert.Equal(SamplePositionLayout.Nested, engine.GatheredSampleLayout);
		Assert.Equal(((int)SamplePositionLayout.Nested, 6), DatabaseUtils.SampleLayoutHeader);
	}

	[Fact]
	public void DatabaseWithOnlyImages_IsNested() {
		var image = new FileEntry { _Path = TestPaths.Native(@"C:\vdf-layout\a.jpg"), Folder = TestPaths.Native(@"C:\vdf-layout") };
		image.Flags.Set(EntryFlags.IsImage);
		image.grayBytes[0] = new byte[32 * 32];
		DatabaseUtils.Database.Add(image);

		var engine = Engine(6);
		engine.AdoptDatabaseSampleLayout();

		Assert.Equal(SamplePositionLayout.Nested, engine.GatheredSampleLayout);
	}

	[Fact]
	public void DisplayPositions_FollowTheDatabaseLayout() {
		// Thumbnail reload, the pair diagnostic and the Web frame stepping use the
		// one-argument overload: it must name the positions the scan compared.
		DatabaseUtils.SampleLayoutHeader = ((int)SamplePositionLayout.Legacy, 6);
		Assert.Equal(ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Legacy), ScanEngine.BuildSamplePositions(6));

		DatabaseUtils.SampleLayoutHeader = ((int)SamplePositionLayout.Nested, 6);
		Assert.Equal(ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Nested), ScanEngine.BuildSamplePositions(6));
	}

	[Theory]
	[InlineData(0, 0, 6, SamplePositionLayout.Legacy)]   // pre-upgrade database: keep its layout
	[InlineData(0, 6, 6, SamplePositionLayout.Legacy)]
	[InlineData(0, 6, 5, SamplePositionLayout.Nested)]   // count changed: move once
	[InlineData(1, 6, 6, SamplePositionLayout.Nested)]
	[InlineData(1, 6, 2, SamplePositionLayout.Nested)]
	public void Resolve_FollowsTheRule(int layout, int recordedCount, int count, SamplePositionLayout expected) =>
		Assert.Equal(expected, DatabaseUtils.ResolveSamplePositionLayout(
			new DatabaseWrapper { SamplePositionLayout = layout, SamplePositionCount = recordedCount }, count));

	[Fact]
	public void Header_RoundTrips_ThroughTheStreamingFormat() {
		var wrapper = new DatabaseWrapper { SamplePositionLayout = (int)SamplePositionLayout.Nested, SamplePositionCount = 6 };
		wrapper.Entries.Add(VideoSampledAt("six.mp4", ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Nested)));
		using var stream = new MemoryStream();
		DatabaseUtils.SerializeDatabaseStreaming(stream, wrapper);

		stream.Position = 8; // the caller consumes the format magic before deserializing
		var read = DatabaseUtils.DeserializeDatabaseStreaming(stream);

		Assert.Equal((int)SamplePositionLayout.Nested, read.SamplePositionLayout);
		Assert.Equal(6, read.SamplePositionCount);
		Assert.Single(read.Entries);
	}

	[Fact]
	public void DatabaseWithoutTheField_ReadsAsLegacy() {
		// Strip the trailer: what a database written before this change looks like.
		var wrapper = new DatabaseWrapper { SamplePositionLayout = (int)SamplePositionLayout.Nested, SamplePositionCount = 6 };
		wrapper.Entries.Add(VideoSampledAt("six.mp4", ScanEngine.BuildSamplePositions(6, SamplePositionLayout.Legacy)));
		using var full = new MemoryStream();
		DatabaseUtils.SerializeDatabaseStreaming(full, wrapper);
		using var old = new MemoryStream(full.ToArray(), 8, (int)full.Length - 8 - 16);

		var read = DatabaseUtils.DeserializeDatabaseStreaming(old);

		Assert.Equal((int)SamplePositionLayout.Legacy, read.SamplePositionLayout);
		Assert.Equal(0, read.SamplePositionCount);
		Assert.Single(read.Entries);
	}

	[Fact]
	public void Header_RoundTrips_ThroughTheMemoryPackWrapper() {
		var wrapper = new DatabaseWrapper { SamplePositionLayout = (int)SamplePositionLayout.Nested, SamplePositionCount = 6 };

		var read = MemoryPack.MemoryPackSerializer.Deserialize<DatabaseWrapper>(MemoryPack.MemoryPackSerializer.Serialize(wrapper))!;

		Assert.Equal((int)SamplePositionLayout.Nested, read.SamplePositionLayout);
		Assert.Equal(6, read.SamplePositionCount);
	}
}
