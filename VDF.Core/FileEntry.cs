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

using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using MemoryPack;
using VDF.Core.Utils;

namespace VDF.Core {

	[MemoryPackable(GenerateType.VersionTolerant)]
	[DebuggerDisplay("{" + nameof(_Path) + ",nq}")]
	public partial class FileEntry {
#pragma warning disable CS8618 // Non-nullable field is uninitialized.
		[MemoryPackConstructor]
		public FileEntry() { }
#pragma warning restore CS8618 // Non-nullable field is uninitialized.
		public FileEntry(string file) : this(new FileInfo(file)) { }
		public FileEntry(FileInfo fileInfo) {
			_Path = fileInfo.FullName;
			Folder = fileInfo.Directory?.FullName ?? string.Empty;
			var extension = fileInfo.Extension;
			IsImage = FileUtils.ImageExtensions.Any(x => extension.EndsWith(x, StringComparison.OrdinalIgnoreCase));
			DateCreated = fileInfo.CreationTimeUtc;
			DateModified = fileInfo.LastWriteTimeUtc;
			FileSize = fileInfo.Length;
			// Attributes are already populated on enumerated FileInfos, so stamping the
			// reparse flag here is free and spares the scan a per-file syscall later.
			FileAttributes attributes = fileInfo.Attributes;
			if (attributes != (FileAttributes)(-1)) {
				Flags.Set(EntryFlags.ReparsePoint, (attributes & FileAttributes.ReparsePoint) != 0);
				Flags.Set(EntryFlags.ReparsePointChecked);
			}
		}

		[MemoryPackInclude, MemoryPackOrder(0)]
		internal string _Path;
		[MemoryPackIgnore]
		public string Path {
			get => _Path;
			set {
				FileInfo fileInfo = new(value);
				_Path = fileInfo.FullName;
				Folder = fileInfo.Directory?.FullName ?? string.Empty;
			}
		}
		[MemoryPackOrder(1)]
		public string Folder;
		[MemoryPackOrder(2)]
		public Dictionary<double, byte[]?> grayBytes = new();
		[MemoryPackOrder(3)]
		public MediaInfo? mediaInfo;
		[MemoryPackOrder(4)]
		public EntryFlags Flags;
		[MemoryPackOrder(5)]
		public DateTime DateCreated;
		[MemoryPackOrder(6)]
		public DateTime DateModified;
		[MemoryPackOrder(7)]
		public long FileSize;
		[MemoryPackOrder(8)]
		public Dictionary<double, ulong?> PHashes = new();
		/// <summary>
		/// Aggregated audio fingerprint: one <c>uint</c> per second of audio.
		/// <c>null</c> = not yet extracted; empty array = file has no audio track.
		/// </summary>
		[MemoryPackOrder(9)]
		public uint[]? AudioFingerprint;
		/// <summary>
		/// OpenSubtitles-style content fingerprint (size + head/tail 64 KiB checksum), cached so a
		/// later scan can recognise a MOVED file (same OsHash, old path now gone) and relink it —
		/// reusing its thumbnails/mediaInfo instead of re-decoding. <c>null</c> = not yet computed
		/// (or the file was too small / unreadable when last seen).
		/// </summary>
		[MemoryPackOrder(10)]
		public string? OsHash;
		// Neural embeddings deliberately do NOT live on FileEntry: at library scale they
		// would permanently bloat the main database and resident memory even for users who
		// never enable AI matching. They are cached in the UnionEmbeddingStore sidecar
		// (VDF.Core.AI), keyed by path and validated by size + mtime.

		[MemoryPackIgnore]
		internal bool invalid = true;

		// Transient compare-phase snapshot, built by ScanEngine.ScanForDuplicates and
		// cleared when the phase ends. compareGray holds the gray-byte arrays aligned
		// with the scan's thumbnail position order, comparePHashes the pHash of every
		// sampled position (same order), and compareIndex the entry's position in the
		// scan list. The per-pair hot path reads these instead of probing
		// Dictionary<double,...> with computed keys and hashing the full path string
		// for every candidate pair.
		[MemoryPackIgnore]
		internal byte[]?[]? compareGray;
		[MemoryPackIgnore]
		internal ulong[]? comparePHashes;
		// AI-pass snapshot, aligned like compareGray: the entry's quantized embeddings and,
		// as black-frame guard, whether each position's gray frame has enough non-dark
		// pixels to make its embedding trustworthy (two dark frames embed near-identically).
		[MemoryPackIgnore]
		internal byte[]?[]? compareEmbeddings;
		[MemoryPackIgnore]
		internal bool[]? compareEmbeddingValid;
		[MemoryPackIgnore]
		internal int compareIndex;

		[MemoryPackIgnore]
		internal bool IsImage {
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get => Flags.Has(EntryFlags.IsImage);
			set => Flags.Set(EntryFlags.IsImage, value);
		}
		[MemoryPackIgnore]
		public bool IsManuallyExcluded {
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get => Flags.Has(EntryFlags.ManuallyExcluded);
			protected set => Flags.Set(EntryFlags.ManuallyExcluded, value);
		}
		[MemoryPackIgnore]
		public bool HasMetadataError {
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get => Flags.Has(EntryFlags.MetadataError);
			protected set => Flags.Set(EntryFlags.MetadataError, value);
		}
		[MemoryPackIgnore]
		public bool HasThubmanilError {
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get => Flags.Has(EntryFlags.ThumbnailError);
			protected set => Flags.Set(EntryFlags.ThumbnailError, value);
		}
		[MemoryPackIgnore]
		public bool IsTooDark {
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get => Flags.Has(EntryFlags.TooDark);
			protected set => Flags.Set(EntryFlags.TooDark, value);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		/// <summary>
		/// Drops everything derived from the file's content so the next scan re-probes and
		/// re-extracts it: media info, gray frames, pHashes, audio fingerprint and every
		/// error/audio flag. Identity data stays (path, dates, size, OsHash, manual
		/// exclusion, reparse-point state) — this is the database editor's "clear cached
		/// hashes" repair action, the lighter alternative to deleting the entry. AI
		/// embeddings live in sidecar caches that self-validate by size + mtime, so they
		/// need no explicit clearing here.
		/// </summary>
		public void ClearCachedMediaData() {
			mediaInfo = null;
			grayBytes.Clear();
			PHashes.Clear();
			AudioFingerprint = null;
			Flags.Set(EntryFlags.ThumbnailError | EntryFlags.MetadataError | EntryFlags.TooDark |
				EntryFlags.NoAudioTrack | EntryFlags.AudioFingerprintError | EntryFlags.SilentAudioTrack, false);
			invalid = true;
		}

		public double GetGrayBytesIndex(float position) => mediaInfo!.Duration.TotalSeconds * position;
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public double GetGrayBytesIndex(float position, double? maxSamplingDurationSeconds) {
			double durationSeconds = mediaInfo!.Duration.TotalSeconds;
			if (maxSamplingDurationSeconds.HasValue && maxSamplingDurationSeconds.Value > 0d && durationSeconds > maxSamplingDurationSeconds.Value)
				durationSeconds = maxSamplingDurationSeconds.Value;
			return durationSeconds * position;
		}

		/// <summary>
		/// Relative tolerance under which two cached position keys name the same frame.
		/// Databases written before sample positions were computed exactly keyed frames by
		/// float-accumulated positions, which drift from k/(n+1) by a few float ulps (under
		/// 3e-6 relative for 200 frames). Distinct positions of any two frame counts up to
		/// a few hundred lie at least 1e-5 apart relatively, so this only absorbs that noise
		/// and never treats two different moments as one.
		/// </summary>
		internal const double PositionKeyRelativeTolerance = 1e-5;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static bool PositionKeysMatch(double a, double b) =>
			Math.Abs(a - b) <= PositionKeyRelativeTolerance * Math.Max(Math.Abs(a), Math.Abs(b)) + 1e-9;

		/// <summary>
		/// Finds the stored key in <paramref name="keys"/> that names the same frame as
		/// <paramref name="positionKey"/>: the exact key when present, else the closest key
		/// within <see cref="PositionKeyRelativeTolerance"/>.
		/// </summary>
		internal static bool TryFindMatchingPositionKey<T>(IReadOnlyDictionary<double, T> keys, double positionKey, out double match) {
			if (keys.ContainsKey(positionKey)) {
				match = positionKey;
				return true;
			}
			match = 0;
			bool found = false;
			double bestDistance = double.MaxValue;
			foreach (double key in keys.Keys) {
				if (!PositionKeysMatch(key, positionKey)) continue;
				double distance = Math.Abs(key - positionKey);
				if (distance < bestDistance) {
					bestDistance = distance;
					match = key;
					found = true;
				}
			}
			return found;
		}

		/// <summary>
		/// Moves cached gray frames and pHashes stored under a near-identical key (float
		/// noise from an older position calculation) onto the exact keys of
		/// <paramref name="positions"/>, so every exact-key lookup of the scan and compare
		/// reuses them instead of decoding the file again. Returns the number of keys moved.
		/// </summary>
		internal int SnapCachedPositionKeys(IReadOnlyList<float> positions, double? maxSamplingDurationSeconds) {
			if (mediaInfo == null || IsImage) return 0;
			int moved = 0;
			for (int i = 0; i < positions.Count; i++) {
				double key = GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (grayBytes != null && SnapKey(grayBytes, key)) moved++;
				if (PHashes != null) SnapKey(PHashes, key);
			}
			return moved;
		}

		static bool SnapKey<T>(Dictionary<double, T> cache, double key) {
			if (cache.Count == 0 || cache.ContainsKey(key)) return false;
			if (!TryFindMatchingPositionKey(cache, key, out double stored)) return false;
			cache[key] = cache[stored];
			cache.Remove(stored);
			return true;
		}
		public override bool Equals(object? obj) =>
			obj is FileEntry entry &&
			Path.Equals(entry.Path, CoreUtils.IsWindows ?
				StringComparison.OrdinalIgnoreCase :
				StringComparison.Ordinal);

		public override int GetHashCode() => CoreUtils.IsWindows ?
			StringComparer.OrdinalIgnoreCase.GetHashCode(Path) :
			HashCode.Combine(Path);
	}
}
