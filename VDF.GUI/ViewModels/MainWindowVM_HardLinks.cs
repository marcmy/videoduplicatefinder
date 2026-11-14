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
using System.Reactive;
using ReactiveUI;
using VDF.Core.Utils;
using VDF.GUI.Data;

namespace VDF.GUI.ViewModels {
	/// <summary>
	/// Hard links in the results, with "Exclude hard links" on. Every link stays listed so the
	/// user can choose which name to keep: links of one file share a numbered, coloured badge,
	/// automatic selection treats them as one candidate, the space count only includes a file
	/// once all of its links are checked, and the delete confirmation says what deleting some or
	/// all links of a file does.
	/// </summary>
	public partial class MainWindowVM {

		/// <summary>
		/// Re-reads link sets from disk and drops groups made only of links of one file. With
		/// <paramref name="onlyTagged"/>, only entries that were already links are read again:
		/// after a deletion only their link counts can have changed.
		/// With the setting off, any tags carried by imported results are cleared.
		/// </summary>
		internal async Task ResolveHardLinksAsync(bool onlyTagged) {
			var infos = Duplicates.Select(d => d.ItemInfo).ToList();
			if (!SettingsFile.Instance.ExcludeHardLinks) {
				foreach (var info in infos) {
					info.HardLinkSetId = 0;
					info.HardLinkCount = 0;
				}
				checkedSizeTracker.Reset(Duplicates);
				RaiseCheckedSizeChanged();
				return;
			}

			HashSet<string>? toRead = null;
			if (onlyTagged) {
				toRead = new HashSet<string>(infos.Where(i => i.HardLinkSetId != 0).Select(i => i.Path),
					CoreUtils.IsWindows ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
				if (toRead.Count == 0)
					return;
			}
			var outcome = await Task.Run(() => HardLinkSets.Resolve(infos,
				path => toRead == null || toRead.Contains(path) ? HardLinkSets.ReadFromDisk(path) : null));

			if (outcome.Dropped.Count > 0) {
				for (int i = Duplicates.Count - 1; i >= 0; i--)
					if (outcome.Dropped.Contains(Duplicates[i].ItemInfo))
						Duplicates.RemoveAt(i);
				Logger.Instance.Info($"Exclude hard links: dropped {outcome.GroupsDropped} group(s) made only of hard links of one file");
			}
			// Ids were reassigned, so the per-set bookkeeping of the space count starts over.
			checkedSizeTracker.Reset(Duplicates);
			RaiseCheckedSizeChanged();
		}

		/// <summary>
		/// Updates every row's hard-link badge and tooltip from the current list. No disk
		/// access: the tooltip lists the other names of the file that are in the results, which
		/// changes whenever entries leave the list.
		/// </summary>
		internal void RefreshHardLinkDisplay() {
			Dictionary<int, List<DuplicateItemVM>>? bySet = null;
			foreach (var d in Duplicates) {
				int id = d.ItemInfo.HardLinkSetId;
				if (id == 0)
					continue;
				bySet ??= new();
				if (!bySet.TryGetValue(id, out var members))
					bySet[id] = members = new();
				members.Add(d);
			}
			string badgeFormat = App.Lang["Results.Row.HardLink"];
			foreach (var d in Duplicates) {
				int id = d.ItemInfo.HardLinkSetId;
				if (id == 0 || bySet == null) {
					d.SetHardLinkDisplay(null, null);
					continue;
				}
				var others = bySet[id]
					.Where(m => !ReferenceEquals(m, d))
					.Select(m => m.ItemInfo.Path)
					.Distinct(CoreUtils.IsWindows ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal)
					.ToList();
				d.SetHardLinkDisplay(string.Format(badgeFormat, id),
					HardLinkSelection.BuildTooltip(d, others, key => App.Lang[key]));
			}
		}

		/// <summary>
		/// Runs an automatic selection tool that reports each keeper it picks through the callback
		/// it is given, then keeps every name of each kept file: a keeper that is one name of a
		/// file must not leave the file's other names marked, since deleting them frees nothing.
		/// </summary>
		internal static void RunKeepingWholeLinkSets(IReadOnlyCollection<DuplicateItemVM> scope, Action<Action<DuplicateItemVM>> tool) {
			var keepers = new List<DuplicateItemVM>();
			tool(keepers.Add);
			HardLinkSelection.KeepWholeLinkSets(scope, keepers);
		}

		/// <summary>Row menu: checks every listed name of the right-clicked file, in any group.</summary>
		public ReactiveCommand<DuplicateItemVM, Unit> SelectAllHardLinksCommand => ReactiveCommand.Create<DuplicateItemVM>(item => {
			if (item == null) return;
			var links = HardLinkSelection.LinksOf(item, Duplicates);
			if (links.Count == 0) return;
			using var undoBatch = BeginSelectionUndoBatch();
			foreach (var link in links)
				link.Checked = true;
		});

		/// <summary>The delete confirmation's hard-link section, or null when none is affected.</summary>
		internal static string? BuildHardLinkDeleteWarning(IEnumerable<DuplicateItemVM> toDelete) =>
			HardLinkSelection.BuildDeleteWarning(HardLinkSelection.AnalyzeDelete(toDelete), key => App.Lang[key]);
	}
}
