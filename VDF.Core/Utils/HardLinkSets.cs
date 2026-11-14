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

using System.Collections.Concurrent;
using System.Linq;
using VDF.Core.ViewModels;

namespace VDF.Core.Utils {
	/// <summary>
	/// Hard links in duplicate results, with "Exclude hard links" on. Every link stays listed so
	/// the user can choose which name to keep; this only drops groups where every entry is the
	/// same physical file (nothing to compare) and tags the rest: entries that are links of one
	/// file share a <see cref="DuplicateItem.HardLinkSetId"/>, across groups too, and carry the
	/// file's total link count so the GUI can tell when links exist outside the results.
	/// Shared by the scan engine and the GUI (imported results, and after a deletion).
	/// </summary>
	internal static class HardLinkSets {
		internal readonly record struct LinkInfo(HardLinkUtils.FileIdentity Identity, uint LinkCount);

		/// <summary>Reads the identity and link count of a file on disk; null when unreadable.</summary>
		internal static LinkInfo? ReadFromDisk(string path) =>
			HardLinkUtils.TryGetLinkInfo(path, out var id, out uint count) ? new LinkInfo(id, count) : null;

		internal readonly record struct Outcome(HashSet<DuplicateItem> Dropped, int GroupsDropped, int LinkSets, int LinkedEntries);

		/// <summary>
		/// Tags <paramref name="items"/> in place (clearing any earlier tags) and returns the
		/// entries to drop: every member of a group made only of links of one file. An entry
		/// whose identity cannot be read (missing file, offline drive, a provider without file
		/// ids) counts as a file of its own and is never tagged.
		/// Lookups run in parallel and once per path: on a network share each one is a round
		/// trip, and the results can hold tens of thousands of entries.
		/// </summary>
		internal static Outcome Resolve(IReadOnlyCollection<DuplicateItem> items, Func<string, LinkInfo?> lookup) {
			var pathComparer = CoreUtils.IsWindows ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
			var infos = new ConcurrentDictionary<string, LinkInfo?>(pathComparer);
			Parallel.ForEach(items.Select(i => i.Path).Distinct(pathComparer),
				new ParallelOptions { MaxDegreeOfParallelism = 8 },
				path => infos[path] = lookup(path));

			foreach (var item in items) {
				item.HardLinkSetId = 0;
				item.HardLinkCount = 0;
			}

			var dropped = new HashSet<DuplicateItem>(ReferenceEqualityComparer.Instance);
			int groupsDropped = 0;
			foreach (var group in items.GroupBy(i => i.GroupId)) {
				var members = group.ToList();
				if (members.Count < 2)
					continue;
				if (infos[members[0].Path] is not { } first)
					continue;
				bool allSameFile = members.All(m => infos[m.Path] is { } info && info.Identity == first.Identity);
				if (!allSameFile)
					continue;
				foreach (var m in members)
					dropped.Add(m);
				groupsDropped++;
			}

			// Ids follow first appearance, so a re-run over the same list numbers sets the same way.
			var kept = items.Where(i => !dropped.Contains(i)).ToList();
			var pathsPerFile = new Dictionary<HardLinkUtils.FileIdentity, HashSet<string>>();
			foreach (var item in kept) {
				if (infos[item.Path] is not { } info)
					continue;
				if (!pathsPerFile.TryGetValue(info.Identity, out var paths))
					pathsPerFile[info.Identity] = paths = new HashSet<string>(pathComparer);
				paths.Add(item.Path);
			}

			var ids = new Dictionary<HardLinkUtils.FileIdentity, int>();
			int linkedEntries = 0;
			foreach (var item in kept) {
				if (infos[item.Path] is not { } info)
					continue;
				int inResults = pathsPerFile[info.Identity].Count;
				// Two names in the results, or one here and others elsewhere on the volume:
				// either way deleting this name alone frees nothing.
				if (inResults < 2 && info.LinkCount < 2)
					continue;
				if (!ids.TryGetValue(info.Identity, out int id))
					ids[info.Identity] = id = ids.Count + 1;
				item.HardLinkSetId = id;
				// A count read before a link was added elsewhere can be lower than what the
				// results hold; never report fewer links than are listed.
				item.HardLinkCount = (int)Math.Max(info.LinkCount, (uint)inResults);
				linkedEntries++;
			}

			return new Outcome(dropped, groupsDropped, ids.Count, linkedEntries);
		}
	}
}
