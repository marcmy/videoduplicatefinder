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
// End-to-end cover for Settings.ExcludeHardLinks in the main compare (#904 follow-up): two real
// hard links of one file with identical frames are driven through ScanForDuplicates on every
// path the gate lives on - images, the linear video loop and the bucketed video loop - and the
// gate must drop the pair only when the setting is on, and only for true hard links. Beyond the
// direct pair, CollapseHardLinkedDuplicates must keep one entry per physical file in every final
// group (links meeting through a third file, partial-clip groups), cached entries whose durations
// differ slightly must still be excluded, and paths past MAX_PATH must still be recognised.

using VDF.Core.Utils;
using VDF.Core.ViewModels;

namespace VDF.Core.Tests;

[Collection("DatabaseUtils")] // ScanForDuplicates reads the shared static database
public class HardLinkExclusionTests : IDisposable {

	readonly List<FileEntry> added = new();
	readonly string dir = Path.Combine(Path.GetTempPath(), "vdf-hardlink-" + Guid.NewGuid().ToString("N"));

	public HardLinkExclusionTests() => Directory.CreateDirectory(dir);

	public void Dispose() {
		foreach (var e in added)
			DatabaseUtils.Database.Remove(e);
		try { Directory.Delete(dir, recursive: true); }
		catch (IOException) { }
		catch (UnauthorizedAccessException) { }
	}

	const int FrameLength = GrayBytesUtils.Side * GrayBytesUtils.Side; // 1024
	static readonly byte[] SharedFrame = Frame(0x80);
	static readonly byte[] FileBytes = { 1, 2, 3, 4 };

	static byte[] Frame(byte value) {
		var frame = new byte[FrameLength];
		Array.Fill(frame, value);
		return frame;
	}

	/// <summary>Writes a real file and a real hard link to it; returns both paths.</summary>
	(string original, string link) CreateHardLinkedPair(string extension) {
		string original = Path.Combine(dir, "original" + extension);
		string link = Path.Combine(dir, "link" + extension);
		File.WriteAllBytes(original, FileBytes);
		HardLinkUtils.CreateHardLink(link, original);
		Assert.True(HardLinkUtils.AreSameFile(original, link), "test setup: both paths must resolve to the same file");
		return (original, link);
	}

	/// <summary>Writes two separate files with the same bytes (duplicates, but not hard links).</summary>
	(string a, string b) CreateSeparateCopies(string extension) {
		string a = Path.Combine(dir, "copy-a" + extension);
		string b = Path.Combine(dir, "copy-b" + extension);
		File.WriteAllBytes(a, FileBytes);
		File.WriteAllBytes(b, FileBytes);
		Assert.False(HardLinkUtils.AreSameFile(a, b), "test setup: separate files must not resolve to the same file");
		return (a, b);
	}

	FileEntry AddVideo(string path, double durationSeconds, byte[]? frame = null) {
		var entry = new FileEntry {
			_Path = path,
			FileSize = FileBytes.Length,
			invalid = false,
			IsImage = false,
			mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(durationSeconds) },
		};
		// ThumbnailCount is 1 with position 0.5 -> index = duration * 0.5.
		entry.grayBytes[entry.GetGrayBytesIndex(0.5f)] = frame ?? SharedFrame;
		DatabaseUtils.Database.Add(entry);
		added.Add(entry);
		return entry;
	}

	FileEntry AddImage(string path, TimeSpan duration = default) {
		var entry = new FileEntry {
			_Path = path,
			FileSize = FileBytes.Length,
			invalid = false,
			IsImage = true,
			// The scanner builds image MediaInfo without a Duration (TimeSpan.Zero).
			mediaInfo = new MediaInfo { Duration = duration },
		};
		// Image entries store their single frame under index 0.
		entry.grayBytes[0] = SharedFrame;
		DatabaseUtils.Database.Add(entry);
		added.Add(entry);
		return entry;
	}

	static ScanEngine NewEngine(bool excludeHardLinks) {
		var engine = new ScanEngine();
		engine.Settings.ThumbnailCount = 1;
		engine.positionList.Add(0.5f);
		engine.Settings.Percent = 95f;
		engine.Settings.ExcludeHardLinks = excludeHardLinks;
		engine.ElapsedTimer.Start();
		return engine;
	}

	static bool PairReported(ScanEngine engine, FileEntry a, FileEntry b) =>
		engine.Duplicates.Any(d => d.Path == a.Path) && engine.Duplicates.Any(d => d.Path == b.Path);

	[Fact]
	public void Videos_LinearPath_HardLinksDroppedOnlyWhenExcluded() {
		var (original, link) = CreateHardLinkedPair(".mp4");
		var a = AddVideo(original, 100);
		var b = AddVideo(link, 100);

		var excluding = NewEngine(excludeHardLinks: true);
		excluding.ScanForDuplicates();
		Assert.False(PairReported(excluding, a, b), "hard links of one video must not be reported when ExcludeHardLinks is on");

		var reporting = NewEngine(excludeHardLinks: false);
		reporting.ScanForDuplicates();
		Assert.True(PairReported(reporting, a, b), "with ExcludeHardLinks off the pair is an ordinary duplicate");
	}

	[Fact]
	public void Images_HardLinksDroppedOnlyWhenExcluded() {
		var (original, link) = CreateHardLinkedPair(".jpg");
		var a = AddImage(original);
		var b = AddImage(link);

		var excluding = NewEngine(excludeHardLinks: true);
		excluding.ScanForDuplicates();
		Assert.False(PairReported(excluding, a, b), "hard links of one image must not be reported when ExcludeHardLinks is on");

		var reporting = NewEngine(excludeHardLinks: false);
		reporting.ScanForDuplicates();
		Assert.True(PairReported(reporting, a, b), "with ExcludeHardLinks off the pair is an ordinary duplicate");
	}

	// Durations play no part in the gate (see the cached-duration test below). Image entries
	// normally carry none, but one from a legacy database or an animated format may; that must
	// not re-admit a hard-linked image pair.
	[Fact]
	public void Images_StillDroppedWhenOneSideCarriesADuration() {
		var (original, link) = CreateHardLinkedPair(".jpg");
		var a = AddImage(original);
		var b = AddImage(link, TimeSpan.FromMilliseconds(40)); // one frame at 25 fps, what ffprobe reports for a still

		var excluding = NewEngine(excludeHardLinks: true);
		excluding.ScanForDuplicates();
		Assert.False(PairReported(excluding, a, b), "an image pair's duration must not affect hard-link exclusion");
	}

	[Fact]
	public void SeparateCopies_StillReportedWhenExcluding() {
		var (pathA, pathB) = CreateSeparateCopies(".mp4");
		var a = AddVideo(pathA, 100);
		var b = AddVideo(pathB, 100);

		var excluding = NewEngine(excludeHardLinks: true);
		excluding.ScanForDuplicates();
		Assert.True(PairReported(excluding, a, b), "ExcludeHardLinks must only drop true hard links, not byte-identical copies");
	}

	/// <summary>Number of entries among <paramref name="paths"/> that are in the results.</summary>
	static int Reported(ScanEngine engine, params string[] paths) =>
		paths.Count(p => engine.Duplicates.Any(d => d.Path == p));

	// The bug the user saw: two links never pair with each other, but each matches a real copy,
	// so the union-by-transitivity grouping put both links into the copy's group anyway.
	[Fact]
	public void Videos_LinksMeetingThroughAThirdFile_KeepOneEntryPerPhysicalFile() {
		var (original, link) = CreateHardLinkedPair(".mp4");
		string copyPath = Path.Combine(dir, "copy.mp4");
		File.WriteAllBytes(copyPath, FileBytes);
		AddVideo(original, 100);
		AddVideo(link, 100);
		AddVideo(copyPath, 100);

		var excluding = NewEngine(excludeHardLinks: true);
		excluding.ScanForDuplicates();
		excluding.CollapseHardLinkedDuplicates();
		Assert.Equal(1, Reported(excluding, original, link));
		Assert.Equal(1, Reported(excluding, copyPath));
		Assert.Single(excluding.Duplicates.Select(d => d.GroupId).Distinct());
		Assert.Equal(2, excluding.Duplicates.Count);

		var reporting = NewEngine(excludeHardLinks: false);
		reporting.ScanForDuplicates();
		reporting.CollapseHardLinkedDuplicates();
		Assert.Equal(3, reporting.Duplicates.Count);
	}

	// Entries come from ScannedFiles.db: a round trip through the on-disk format, with the two
	// links' durations a few milliseconds apart as they are when one was probed by the ffprobe
	// CLI and the other by the native binding (or an older VDF). The old pair gate required
	// equal durations and let exactly this pair through.
	[Fact]
	public void Videos_FromDatabaseCache_WithSlightlyDifferentDurations_AreExcluded() {
		var (original, link) = CreateHardLinkedPair(".mp4");
		var wrapper = new DatabaseWrapper { Entries = new() };
		foreach (var (path, seconds) in new[] { (original, 100.0), (link, 100.021) }) {
			var e = new FileEntry {
				_Path = path,
				FileSize = FileBytes.Length,
				invalid = false,
				IsImage = false,
				mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(seconds) },
			};
			e.grayBytes[e.GetGrayBytesIndex(0.5f)] = SharedFrame;
			wrapper.Entries.Add(e);
		}
		using var ms = new MemoryStream();
		DatabaseUtils.SerializeDatabaseStreaming(ms, wrapper);
		ms.Position = "VDFDB002"u8.Length; // LoadDatabase consumes the file magic before deserializing
		var loaded = DatabaseUtils.DeserializeDatabaseStreaming(ms);
		Assert.Equal(2, loaded.Entries.Count);
		foreach (var e in loaded.Entries) {
			Assert.True(e.invalid, "a freshly loaded cache entry is invalid until the scan re-validates it");
			e.invalid = false; // what the scan does for a cached file that still exists unchanged
			DatabaseUtils.Database.Add(e);
			added.Add(e);
		}

		var excluding = NewEngine(excludeHardLinks: true);
		excluding.ScanForDuplicates();
		Assert.Equal(0, Reported(excluding, original, link));
		excluding.CollapseHardLinkedDuplicates();
		Assert.Empty(excluding.Duplicates);

		var reporting = NewEngine(excludeHardLinks: false);
		reporting.ScanForDuplicates();
		Assert.Equal(2, Reported(reporting, original, link));
	}

	FileEntry NewVideoEntry(string path) => new() {
		_Path = path,
		FileSize = FileBytes.Length,
		invalid = false,
		IsImage = false,
		mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(100) },
	};

	// The partial-clip passes (audio and AI) emit their groups directly, with no pair gate:
	// two links of one clip assigned to the same source must collapse to one clip entry, and
	// the source is preferred over a partial-clip entry when the links span both roles.
	[Fact]
	public void PartialClipGroups_KeepOneEntryPerPhysicalFile() {
		var (clip, clipLink) = CreateHardLinkedPair(".mp4");
		string source = Path.Combine(dir, "source.mp4");
		File.WriteAllBytes(source, new byte[] { 9, 9, 9, 9, 9, 9, 9, 9 });
		var groupId = Guid.NewGuid();

		var engine = NewEngine(excludeHardLinks: true);
		engine.Duplicates.Add(new DuplicateItem(NewVideoEntry(source), 0f, groupId, DuplicateFlags.None));
		engine.Duplicates.Add(new DuplicateItem(NewVideoEntry(clip), 0.10f, groupId, DuplicateFlags.PartialClip));
		engine.Duplicates.Add(new DuplicateItem(NewVideoEntry(clipLink), 0.05f, groupId, DuplicateFlags.PartialClip | DuplicateFlags.AiMatched));
		engine.CollapseHardLinkedDuplicates();

		Assert.Equal(2, engine.Duplicates.Count);
		Assert.Equal(1, Reported(engine, source));
		// The closer match of the two links is the one kept.
		Assert.Equal(1, Reported(engine, clipLink));

		// A source and its own hard link as the only clip: nothing real is left to compare.
		var (src2, src2Link) = CreateHardLinkedPair(".mkv");
		var group2 = Guid.NewGuid();
		var engine2 = NewEngine(excludeHardLinks: true);
		engine2.Duplicates.Add(new DuplicateItem(NewVideoEntry(src2), 0f, group2, DuplicateFlags.None));
		engine2.Duplicates.Add(new DuplicateItem(NewVideoEntry(src2Link), 0.02f, group2, DuplicateFlags.PartialClip));
		engine2.CollapseHardLinkedDuplicates();
		Assert.Empty(engine2.Duplicates);
	}

	[Fact]
	public void Collapse_DoesNothingWhenSettingIsOff() {
		var (original, link) = CreateHardLinkedPair(".mp4");
		var groupId = Guid.NewGuid();
		var engine = NewEngine(excludeHardLinks: false);
		engine.Duplicates.Add(new DuplicateItem(NewVideoEntry(original), 0f, groupId, DuplicateFlags.None));
		engine.Duplicates.Add(new DuplicateItem(NewVideoEntry(link), 0f, groupId, DuplicateFlags.None));
		engine.CollapseHardLinkedDuplicates();
		Assert.Equal(2, engine.Duplicates.Count);
	}

	// The raw CreateFileW call bypasses .NET's long-path handling: past MAX_PATH the file id
	// could not be read and the pair silently counted as "not hard links".
	[Fact]
	public void LongPaths_AreRecognisedAsHardLinks() {
		string deep = dir;
		while (deep.Length < 300)
			deep = Path.Combine(deep, new string('d', 40));
		Directory.CreateDirectory(deep);
		string original = Path.Combine(deep, "original.mp4");
		string link = Path.Combine(deep, "link.mp4");
		File.WriteAllBytes(original, FileBytes);
		HardLinkUtils.CreateHardLink(link, original);

		Assert.True(original.Length > 260);
		Assert.True(HardLinkUtils.TryGetFileIdentity(original, out var a));
		Assert.True(HardLinkUtils.TryGetFileIdentity(link, out var b));
		Assert.Equal(a, b);
		Assert.True(HardLinkUtils.AreSameFile(original, link));

		string other = Path.Combine(deep, "other.mp4");
		File.WriteAllBytes(other, FileBytes);
		Assert.False(HardLinkUtils.AreSameFile(original, other));
	}

	// Libraries at or above ScanEngine.BucketActivationThreshold videos take the duration-bucketed
	// compare path instead of the linear loop; the gate has to hold there too. Filler entries sit
	// alone in their own whole-second buckets under a zero duration tolerance, so the only pair
	// ever compared is the hard-linked one.
	[Fact]
	public void Videos_BucketedPath_HardLinksDroppedOnlyWhenExcluded() {
		var (original, link) = CreateHardLinkedPair(".mp4");
		var a = AddVideo(original, 100);
		var b = AddVideo(link, 100);
		byte[] fillerFrame = Frame(0x10);
		for (int i = 0; i < ScanEngine.BucketActivationThreshold - 2; i++)
			AddVideo(Path.Combine(dir, "filler" + i + ".mp4"), 1000 + i, fillerFrame);

		var excluding = NewEngine(excludeHardLinks: true);
		excluding.Settings.PercentDurationDifference = 0;
		excluding.ScanForDuplicates();
		Assert.False(PairReported(excluding, a, b), "hard links must be dropped on the bucketed compare path too");
		Assert.Empty(excluding.Duplicates);

		var reporting = NewEngine(excludeHardLinks: false);
		reporting.Settings.PercentDurationDifference = 0;
		reporting.ScanForDuplicates();
		Assert.True(PairReported(reporting, a, b), "the bucketed path must still compare the pair when ExcludeHardLinks is off");
	}
}
