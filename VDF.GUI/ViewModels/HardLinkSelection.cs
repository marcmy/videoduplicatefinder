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

using System.Linq;
using System.Text;
using VDF.Core.Utils;

namespace VDF.GUI.ViewModels {
	/// <summary>
	/// Selection rules for hard-linked results (entries with a HardLinkSetId). A link set is one
	/// physical file under several names: deleting some of its names frees no space and only
	/// removes those paths, deleting every name (counting names outside the results) deletes the
	/// video. Pure functions, so the rules are testable without the UI.
	/// </summary>
	internal static class HardLinkSelection {
		static StringComparer PathComparer => CoreUtils.IsWindows ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

		// SizeLong is -1 when the file no longer exists on disk.
		static long SizeOf(DuplicateItemVM item) => Math.Max(0, item.ItemInfo.SizeLong);

		/// <summary>
		/// Space deleting <paramref name="selected"/> frees: ordinary entries in full, a
		/// hard-linked file once and only when every one of its links is selected.
		/// </summary>
		internal static long FreedBytes(IEnumerable<DuplicateItemVM> selected) {
			long total = 0;
			var linked = new Dictionary<int, (HashSet<string> Paths, int LinkCount, long Size)>();
			foreach (var item in selected) {
				int id = item.ItemInfo.HardLinkSetId;
				if (id == 0) {
					total += SizeOf(item);
					continue;
				}
				if (!linked.TryGetValue(id, out var set))
					linked[id] = set = (new HashSet<string>(PathComparer), item.ItemInfo.HardLinkCount, SizeOf(item));
				set.Paths.Add(item.ItemInfo.Path);
			}
			foreach (var (paths, linkCount, size) in linked.Values)
				if (paths.Count >= linkCount)
					total += size;
			return total;
		}

		/// <summary>What a deletion does to one hard-linked file.</summary>
		internal sealed record LinkSetImpact(int SetId, IReadOnlyList<string> SelectedPaths, int LinkCount) {
			/// <summary>Every name is selected: the data itself is deleted.</summary>
			public bool DeletesFile => SelectedPaths.Count >= LinkCount;
		}

		/// <summary>Groups the hard-linked entries of a pending deletion by physical file.</summary>
		internal static List<LinkSetImpact> AnalyzeDelete(IEnumerable<DuplicateItemVM> selected) {
			var sets = new Dictionary<int, (List<string> Paths, HashSet<string> Seen, int LinkCount)>();
			var order = new List<int>();
			foreach (var item in selected) {
				int id = item.ItemInfo.HardLinkSetId;
				if (id == 0)
					continue;
				if (!sets.TryGetValue(id, out var set)) {
					sets[id] = set = (new List<string>(), new HashSet<string>(PathComparer), item.ItemInfo.HardLinkCount);
					order.Add(id);
				}
				if (set.Seen.Add(item.ItemInfo.Path))
					set.Paths.Add(item.ItemInfo.Path);
			}
			return order.Select(id => new LinkSetImpact(id, sets[id].Paths, sets[id].LinkCount)).ToList();
		}

		/// <summary>
		/// The warning shown in the delete confirmation, or null when no hard-linked file is
		/// affected. Partial sets come first: they are the surprise ("I deleted it, but the
		/// space did not come back").
		/// </summary>
		internal static string? BuildDeleteWarning(IReadOnlyList<LinkSetImpact> impacts, Func<string, string> lang, int maxListedPerKind = 8) {
			if (impacts.Count == 0)
				return null;
			var sb = new StringBuilder();
			void AppendKind(IReadOnlyList<LinkSetImpact> kind, string headerKey) {
				if (kind.Count == 0)
					return;
				if (sb.Length > 0)
					sb.AppendLine().AppendLine();
				sb.Append(lang(headerKey));
				foreach (var impact in kind.Take(maxListedPerKind)) {
					sb.AppendLine();
					sb.Append("• ").Append(string.Format(lang("Message.HardLinkFileLine"),
						string.Join(" | ", impact.SelectedPaths), impact.SelectedPaths.Count, impact.LinkCount));
				}
				if (kind.Count > maxListedPerKind)
					sb.AppendLine().Append(string.Format(lang("Message.HardLinkMoreFiles"), kind.Count - maxListedPerKind));
			}
			AppendKind(impacts.Where(i => !i.DeletesFile).ToList(), "Message.HardLinkPartialWarning");
			AppendKind(impacts.Where(i => i.DeletesFile).ToList(), "Message.HardLinkWholeWarning");
			return sb.ToString();
		}

		/// <summary>
		/// Treats each link set as one candidate after an automatic selection tool: the file a
		/// tool chose to keep is kept under every one of its names in <paramref name="scope"/>,
		/// in any group. Marking its other names would free nothing, since the data survives
		/// under the kept name. Links of files the tool did not keep are left as decided (all
		/// names of a losing file get marked, which deletes it). Unchecking is the safe
		/// direction, so this never marks anything new for deletion.
		/// Returns how many entries were unchecked.
		/// </summary>
		internal static int KeepWholeLinkSets(IEnumerable<DuplicateItemVM> scope, IEnumerable<DuplicateItemVM> keepers) {
			var keptSets = new HashSet<int>();
			foreach (var keeper in keepers)
				if (keeper.ItemInfo.HardLinkSetId != 0)
					keptSets.Add(keeper.ItemInfo.HardLinkSetId);
			if (keptSets.Count == 0)
				return 0;
			int uncheckedCount = 0;
			foreach (var item in scope)
				if (item.Checked && keptSets.Contains(item.ItemInfo.HardLinkSetId)) {
					item.Checked = false;
					uncheckedCount++;
				}
			return uncheckedCount;
		}

		/// <summary>Every listed entry that is a name of the same file as <paramref name="item"/>.</summary>
		internal static List<DuplicateItemVM> LinksOf(DuplicateItemVM item, IEnumerable<DuplicateItemVM> all) {
			int id = item.ItemInfo.HardLinkSetId;
			if (id == 0)
				return new List<DuplicateItemVM>();
			return all.Where(d => d.ItemInfo.HardLinkSetId == id).ToList();
		}

		/// <summary>
		/// The badge tooltip: what a hard link means for deletion, the other names of this file
		/// that are in the results, and how many more exist outside them.
		/// </summary>
		internal static string BuildTooltip(DuplicateItemVM item, IReadOnlyCollection<string> otherPathsInResults, Func<string, string> lang) {
			var sb = new StringBuilder();
			sb.Append(string.Format(lang("Results.Row.HardLinkTip"), item.ItemInfo.HardLinkCount));
			sb.AppendLine().AppendLine();
			if (otherPathsInResults.Count == 0)
				sb.Append(lang("Results.Row.HardLinkTipNoOthers"));
			else {
				sb.Append(lang("Results.Row.HardLinkTipOthers"));
				foreach (var path in otherPathsInResults)
					sb.AppendLine().Append("• ").Append(path);
			}
			int outside = item.ItemInfo.HardLinkCount - otherPathsInResults.Count - 1;
			if (outside > 0)
				sb.AppendLine().AppendLine().Append(string.Format(lang("Results.Row.HardLinkTipOutside"), outside));
			return sb.ToString();
		}
	}

	/// <summary>
	/// Running "space to be freed" for the checked entries, kept incrementally because it is
	/// updated on every checkbox change: ordinary entries count in full, a hard-linked file
	/// counts once and only while all of its links are checked.
	/// </summary>
	internal sealed class CheckedSizeTracker {
		// By entry, not path: a checked entry can be moved or renamed, which changes its path.
		readonly Dictionary<int, HashSet<DuplicateItemVM>> checkedBySet = new();

		public long Total { get; private set; }

		static long SizeOf(DuplicateItemVM item) => Math.Max(0, item.ItemInfo.SizeLong);

		public void Add(DuplicateItemVM item) {
			int id = item.ItemInfo.HardLinkSetId;
			if (id == 0) {
				Total += SizeOf(item);
				return;
			}
			if (!checkedBySet.TryGetValue(id, out var paths))
				checkedBySet[id] = paths = new HashSet<DuplicateItemVM>(ReferenceEqualityComparer.Instance);
			bool wasWhole = paths.Count >= item.ItemInfo.HardLinkCount;
			paths.Add(item);
			if (!wasWhole && paths.Count >= item.ItemInfo.HardLinkCount)
				Total += SizeOf(item);
		}

		public void Remove(DuplicateItemVM item) {
			int id = item.ItemInfo.HardLinkSetId;
			if (id == 0) {
				Total -= SizeOf(item);
				return;
			}
			if (!checkedBySet.TryGetValue(id, out var paths) || !paths.Contains(item))
				return;
			bool wasWhole = paths.Count >= item.ItemInfo.HardLinkCount;
			paths.Remove(item);
			if (wasWhole && paths.Count < item.ItemInfo.HardLinkCount)
				Total -= SizeOf(item);
			if (paths.Count == 0)
				checkedBySet.Remove(id);
		}

		public void Clear() {
			checkedBySet.Clear();
			Total = 0;
		}

		/// <summary>Rebuilds from scratch, after link-set ids were reassigned.</summary>
		public void Reset(IEnumerable<DuplicateItemVM> items) {
			Clear();
			foreach (var item in items)
				if (item.Checked)
					Add(item);
		}
	}
}
