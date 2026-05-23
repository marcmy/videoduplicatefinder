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

using VDF.Core;

namespace VDF.Core.Tests;

public class ScanEngineTests {
	[Fact]
	public void IsReparsePoint_NormalFile_ReturnsFalse() {
		string path = Path.Combine(Directory.GetCurrentDirectory(), $"{Guid.NewGuid():N}.tmp");
		try {
			File.WriteAllText(path, "not a link");

			Assert.False(ScanEngine.IsReparsePoint(path));
		}
		finally {
			File.Delete(path);
		}
	}

	[Fact]
	public void IsReparsePoint_MissingFile_ReturnsFalse() {
		string path = Path.Combine(Directory.GetCurrentDirectory(), $"{Guid.NewGuid():N}.tmp");

		Assert.False(ScanEngine.IsReparsePoint(path));
	}

	[Fact]
	public void InvalidEntryForDuplicateCheck_StaleThumbnailErrorWithRequiredGrayBytes_ReturnsFalse() {
		var scanner = new ScanEngine { Settings = new Settings { ThumbnailCount = 3 } };
		var entry = CreateVideoEntry(grayByteCount: 3);
		entry.Flags.Set(EntryFlags.ThumbnailError);

		Assert.False(scanner.InvalidEntryForDuplicateCheck(entry));
	}

	[Fact]
	public void InvalidEntryForDuplicateCheck_ThumbnailErrorMissingGrayBytes_ReturnsTrue() {
		var scanner = new ScanEngine { Settings = new Settings { ThumbnailCount = 3 } };
		var entry = CreateVideoEntry(grayByteCount: 2);
		entry.Flags.Set(EntryFlags.ThumbnailError);

		Assert.True(scanner.InvalidEntryForDuplicateCheck(entry));
	}

	[Fact]
	public void HasRequiredGrayBytes_NullCachedSample_ReturnsFalse() {
		var scanner = new ScanEngine { Settings = new Settings { ThumbnailCount = 3 } };
		var entry = CreateVideoEntry(grayByteCount: 3);
		entry.grayBytes[1] = null;

		Assert.False(scanner.HasRequiredGrayBytes(entry));
	}

	[Fact]
	public void TryGetOrComputePHash_NullCachedPHashWithGrayBytes_RecomputesAndPersists() {
		var entry = CreateVideoEntry(grayByteCount: 1);
		entry.PHashes[0] = null;

		Assert.True(ScanEngine.TryGetOrComputePHash(entry, entry.grayBytes, 0, persist: true, out ulong computed));
		Assert.Equal(computed, entry.PHashes[0]);
	}

	[Fact]
	public void TryGetOrComputePHash_NullCachedPHashWithoutPersist_DoesNotOverwriteEntryCache() {
		var entry = CreateVideoEntry(grayByteCount: 1);
		var alternateGrayBytes = new Dictionary<double, byte[]?> { [0] = Enumerable.Repeat((byte)255, 1024).ToArray() };
		entry.PHashes[0] = null;

		Assert.True(ScanEngine.TryGetOrComputePHash(entry, alternateGrayBytes, 0, persist: false, out _));
		Assert.Null(entry.PHashes[0]);
	}

	[Fact]
	public void CheckIfDuplicate_PHashUsesAllSamples_NotJustFirst() {
		var scanner = new ScanEngine { Settings = new Settings { UsePHashing = true, Percent = 90f, ThumbnailCount = 2 } };
		SetSamplePositions(scanner, 0.25f, 0.5f);
		var entry = CreateVideoEntryWithPHashes((1d, 0UL), (2d, 0UL));
		var compItem = CreateVideoEntryWithPHashes((1d, 0UL), (2d, ulong.MaxValue));

		Assert.False(InvokeCheckIfDuplicate(scanner, entry, compItem, out _));
	}

	[Fact]
	public void CheckIfDuplicate_PHashAveragesAllSampleSimilarities() {
		var scanner = new ScanEngine { Settings = new Settings { UsePHashing = true, Percent = 75f, ThumbnailCount = 2 } };
		SetSamplePositions(scanner, 0.25f, 0.5f);
		var entry = CreateVideoEntryWithPHashes((1d, 0UL), (2d, 0UL));
		var compItem = CreateVideoEntryWithPHashes((1d, 0UL), (2d, 0xFFFFUL));

		Assert.True(InvokeCheckIfDuplicate(scanner, entry, compItem, out float difference));
		Assert.InRange(difference, 0.124f, 0.126f);
	}

	[Fact]
	public void CheckIfDuplicate_PHashRequiresMatchingSampleQuorum() {
		var scanner = new ScanEngine { Settings = new Settings { UsePHashing = true, Percent = 75f, ThumbnailCount = 5 } };
		SetSamplePositions(scanner, 0.25f, 0.5f, 0.75f, 1f, 1.25f);
		var entry = CreateVideoEntryWithPHashes((1d, 0UL), (2d, 0UL), (3d, 0UL), (4d, 0UL), (5d, 0UL));
		var compItem = CreateVideoEntryWithPHashes(
			(1d, 0UL),
			(2d, 0UL),
			(3d, 0xFFFFFUL),
			(4d, 0xFFFFFUL),
			(5d, 0xFFFFFUL));

		Assert.False(InvokeCheckIfDuplicate(scanner, entry, compItem, out _));
	}

	static FileEntry CreateVideoEntry(int grayByteCount) {
		var entry = new FileEntry {
			_Path = @"X:\video.mp4",
			Folder = @"X:\",
			mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(10), Streams = [] },
			invalid = false,
		};
		for (int i = 0; i < grayByteCount; i++)
			entry.grayBytes[i] = new byte[1024];
		return entry;
	}

	static FileEntry CreateVideoEntryWithPHashes(params (double Index, ulong PHash)[] samples) {
		var entry = CreateVideoEntry(grayByteCount: 0);
		entry.mediaInfo!.Duration = TimeSpan.FromSeconds(4);
		foreach ((double index, ulong pHash) in samples) {
			entry.grayBytes[index] = new byte[1024];
			entry.PHashes[index] = pHash;
		}
		return entry;
	}

	static void SetSamplePositions(ScanEngine scanner, params float[] positions) {
		var field = typeof(ScanEngine).GetField("positionList", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
		var list = (List<float>)field.GetValue(scanner)!;
		list.Clear();
		list.AddRange(positions);
	}

	static bool InvokeCheckIfDuplicate(ScanEngine scanner, FileEntry entry, FileEntry compItem, out float difference) {
		var method = typeof(ScanEngine).GetMethod("CheckIfDuplicate", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
		object?[] args = [entry, null, compItem, 0f];
		bool result = (bool)method.Invoke(scanner, args)!;
		difference = (float)args[3]!;
		return result;
	}
}
