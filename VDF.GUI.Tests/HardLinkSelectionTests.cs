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

using VDF.Core.ViewModels;
using VDF.GUI.ViewModels;

namespace VDF.GUI.Tests {
	/// <summary>
	/// Hard-linked results with "Exclude hard links" on: a link set (entries sharing a
	/// HardLinkSetId) is one file under several names. Deleting some names frees nothing;
	/// deleting every name, counting names outside the results, deletes the video. These
	/// cover the space count, the delete-time warning, and automatic selection treating a
	/// link set as one candidate.
	/// </summary>
	public class HardLinkSelectionTests {
		const long Size = 100L * 1024 * 1024;

		static DuplicateItemVM Item(Guid group, string path, int linkSet = 0, int linkCount = 0,
				long size = Size, bool @checked = false, DateTime? created = null) => new() {
			Checked = @checked,
			ItemInfo = new DuplicateItem {
				GroupId = group,
				Path = path,
				SizeLong = size,
				HardLinkSetId = linkSet,
				HardLinkCount = linkCount,
				DateCreated = created ?? new DateTime(2020, 1, 1),
				Duration = TimeSpan.FromMinutes(2),
			}
		};

		static string Lang(string key) => key switch {
			"Message.HardLinkPartialWarning" => "PARTIAL:",
			"Message.HardLinkWholeWarning" => "WHOLE:",
			"Message.HardLinkFileLine" => "{0} ({1}/{2})",
			"Message.HardLinkMoreFiles" => "+{0} more",
			"Results.Row.HardLinkTip" => "One of {0} names.",
			"Results.Row.HardLinkTipOthers" => "Others:",
			"Results.Row.HardLinkTipNoOthers" => "No others.",
			"Results.Row.HardLinkTipOutside" => "{0} outside.",
			_ => key,
		};

		// ---- space to be freed ----

		[Fact]
		public void FreedBytes_CountsALinkedFileOnceAndOnlyWhenEveryLinkIsSelected() {
			var g = Guid.NewGuid();
			var a = Item(g, @"D:\a.mp4", linkSet: 1, linkCount: 2);
			var b = Item(g, @"D:\b.mp4", linkSet: 1, linkCount: 2);
			var x = Item(g, @"D:\x.mp4", size: 40);

			Assert.Equal(0, HardLinkSelection.FreedBytes(new[] { a }));
			Assert.Equal(Size, HardLinkSelection.FreedBytes(new[] { a, b }));
			Assert.Equal(Size + 40, HardLinkSelection.FreedBytes(new[] { a, b, x }));
			Assert.Equal(40, HardLinkSelection.FreedBytes(new[] { b, x }));
		}

		[Fact]
		public void FreedBytes_LinksOutsideTheResultsKeepTheFileAlive() {
			var g = Guid.NewGuid();
			// Three names on disk, two listed: selecting both listed names still frees nothing.
			var a = Item(g, @"D:\a.mp4", linkSet: 1, linkCount: 3);
			var b = Item(Guid.NewGuid(), @"D:\b.mp4", linkSet: 1, linkCount: 3);
			Assert.Equal(0, HardLinkSelection.FreedBytes(new[] { a, b }));
		}

		[Fact]
		public void FreedBytes_SamePathListedTwiceCountsAsOneName() {
			var a = Item(Guid.NewGuid(), @"D:\a.mp4", linkSet: 1, linkCount: 2);
			var aAgain = Item(Guid.NewGuid(), @"D:\a.mp4", linkSet: 1, linkCount: 2);
			Assert.Equal(0, HardLinkSelection.FreedBytes(new[] { a, aAgain }));
		}

		[Fact]
		public void CheckedSizeTracker_FollowsCheckboxChanges() {
			var g = Guid.NewGuid();
			var a = Item(g, @"D:\a.mp4", linkSet: 1, linkCount: 2);
			var b = Item(Guid.NewGuid(), @"D:\b.mp4", linkSet: 1, linkCount: 2);
			var x = Item(g, @"D:\x.mp4", size: 40);
			var tracker = new CheckedSizeTracker();

			tracker.Add(a);
			Assert.Equal(0, tracker.Total);
			tracker.Add(x);
			Assert.Equal(40, tracker.Total);
			tracker.Add(b);
			Assert.Equal(Size + 40, tracker.Total);
			tracker.Remove(a);
			Assert.Equal(40, tracker.Total);
			tracker.Remove(b);
			tracker.Remove(x);
			Assert.Equal(0, tracker.Total);

			// A rename of a checked entry must not leave its size stuck in the total.
			tracker.Add(a);
			tracker.Add(b);
			a.ItemInfo.Path = @"E:\moved\a.mp4";
			tracker.Remove(a);
			Assert.Equal(0, tracker.Total);

			a.Checked = true;
			b.Checked = true;
			x.Checked = false;
			tracker.Reset(new[] { a, b, x });
			Assert.Equal(Size, tracker.Total);
		}

		[Fact]
		public void CheckedSizeTracker_MissingFileCountsAsZero() {
			var tracker = new CheckedSizeTracker();
			tracker.Add(Item(Guid.NewGuid(), @"D:\gone.mp4", size: -1));
			Assert.Equal(0, tracker.Total);
		}

		// ---- delete-time warning ----

		[Fact]
		public void DeleteWarning_NoneWhenNoLinkedFileIsSelected() {
			var x = Item(Guid.NewGuid(), @"D:\x.mp4");
			Assert.Null(HardLinkSelection.BuildDeleteWarning(HardLinkSelection.AnalyzeDelete(new[] { x }), Lang));
		}

		[Fact]
		public void DeleteWarning_SeparatesPartialSetsFromWholeSets() {
			var g = Guid.NewGuid();
			var a = Item(g, @"D:\a.mp4", linkSet: 1, linkCount: 2);
			var b = Item(Guid.NewGuid(), @"D:\b.mp4", linkSet: 1, linkCount: 2);
			var c = Item(g, @"D:\c.mp4", linkSet: 2, linkCount: 3);
			var d = Item(g, @"D:\d.mp4", linkSet: 2, linkCount: 3);
			var x = Item(g, @"D:\x.mp4");

			var impacts = HardLinkSelection.AnalyzeDelete(new[] { a, b, c, d, x });
			Assert.Equal(2, impacts.Count);
			Assert.True(impacts.Single(i => i.SetId == 1).DeletesFile);
			Assert.False(impacts.Single(i => i.SetId == 2).DeletesFile);

			string warning = HardLinkSelection.BuildDeleteWarning(impacts, Lang)!;
			// The partial set (frees nothing) is listed first, with its selected names and count.
			int partial = warning.IndexOf("PARTIAL:", StringComparison.Ordinal);
			int whole = warning.IndexOf("WHOLE:", StringComparison.Ordinal);
			Assert.True(partial >= 0 && whole > partial);
			Assert.Contains(@"D:\c.mp4 | D:\d.mp4 (2/3)", warning);
			Assert.Contains(@"D:\a.mp4 | D:\b.mp4 (2/2)", warning);
			Assert.DoesNotContain(@"D:\x.mp4", warning);
		}

		[Fact]
		public void DeleteWarning_OnlyWholeSets_HasNoPartialSection() {
			var a = Item(Guid.NewGuid(), @"D:\a.mp4", linkSet: 1, linkCount: 2);
			var b = Item(Guid.NewGuid(), @"D:\b.mp4", linkSet: 1, linkCount: 2);
			string warning = HardLinkSelection.BuildDeleteWarning(HardLinkSelection.AnalyzeDelete(new[] { a, b }), Lang)!;
			Assert.DoesNotContain("PARTIAL:", warning);
			Assert.Contains("WHOLE:", warning);
		}

		[Fact]
		public void DeleteWarning_CapsTheListAndCountsTheRest() {
			var items = Enumerable.Range(1, 12)
				.Select(i => Item(Guid.NewGuid(), $@"D:\f{i}.mp4", linkSet: i, linkCount: 2))
				.ToList();
			string warning = HardLinkSelection.BuildDeleteWarning(HardLinkSelection.AnalyzeDelete(items), Lang, maxListedPerKind: 8)!;
			Assert.Contains(@"D:\f8.mp4", warning);
			Assert.DoesNotContain(@"D:\f9.mp4", warning);
			Assert.Contains("+4 more", warning);
		}

		// ---- automatic selection: a link set is one candidate ----

		[Fact]
		public void KeepWholeLinkSets_UnchecksTheKeptFilesOtherNamesInAnyGroup() {
			Guid g1 = Guid.NewGuid(), g2 = Guid.NewGuid();
			var a = Item(g1, @"D:\a.mp4", linkSet: 1, linkCount: 2);
			var b = Item(g2, @"D:\b.mp4", linkSet: 1, linkCount: 2, @checked: true);
			var c = Item(g1, @"D:\c.mp4", linkSet: 2, linkCount: 2, @checked: true);
			var d = Item(g1, @"D:\d.mp4", linkSet: 2, linkCount: 2, @checked: true);
			var x = Item(g1, @"D:\x.mp4", @checked: true);
			var all = new[] { a, b, c, d, x };

			int unchecked_ = HardLinkSelection.KeepWholeLinkSets(all, new[] { a });

			Assert.Equal(1, unchecked_);
			Assert.False(b.Checked); // a name of the kept file
			Assert.True(c.Checked && d.Checked); // a losing file: every name stays marked
			Assert.True(x.Checked);
		}

		[Fact]
		public void KeepWholeLinkSets_OrdinaryKeeperChangesNothing() {
			var g = Guid.NewGuid();
			var a = Item(g, @"D:\a.mp4", linkSet: 1, linkCount: 2, @checked: true);
			var x = Item(g, @"D:\x.mp4");
			Assert.Equal(0, HardLinkSelection.KeepWholeLinkSets(new[] { a, x }, new[] { x }));
			Assert.True(a.Checked);
		}

		// A cluster of two links of the best file plus a smaller real copy, run through the same
		// cluster engine the "keep the largest" tool uses: the copy is marked, and neither name of
		// the kept file is.
		[Fact]
		public void ClusterTool_NeverMarksAnotherNameOfTheKeptFile() {
			var g = Guid.NewGuid();
			var linkA = Item(g, @"D:\a.mp4", linkSet: 1, linkCount: 2, size: 900);
			var linkB = Item(g, @"D:\b.mp4", linkSet: 1, linkCount: 2, size: 900);
			var copy = Item(g, @"D:\copy.mp4", size: 500);
			var scope = new List<DuplicateItemVM> { copy, linkA, linkB };

			MainWindowVM.RunKeepingWholeLinkSets(scope, keep =>
				MainWindowVM.ForEachGroupCluster(scope, (d, first) => d.EqualsButQuality(first), (first, cluster) => {
					cluster.Add(first);
					cluster = cluster.OrderByDescending(s => s.ItemInfo.SizeLong).ToList();
					cluster[0].Checked = false;
					for (int i = 1; i < cluster.Count; i++)
						cluster[i].Checked = true;
					keep(cluster[0]);
				}));

			Assert.True(copy.Checked);
			Assert.False(linkA.Checked);
			Assert.False(linkB.Checked);
			Assert.Equal(500, HardLinkSelection.FreedBytes(scope.Where(s => s.Checked)));
		}

		// The other way round: the real copy is best, so both names of the linked file are
		// marked together and the file is deleted as a whole.
		[Fact]
		public void ClusterTool_MarksEveryNameOfALosingLinkedFile() {
			var g = Guid.NewGuid();
			var linkA = Item(g, @"D:\a.mp4", linkSet: 1, linkCount: 2, size: 500);
			var linkB = Item(g, @"D:\b.mp4", linkSet: 1, linkCount: 2, size: 500);
			var copy = Item(g, @"D:\copy.mp4", size: 900);
			var scope = new List<DuplicateItemVM> { linkA, linkB, copy };

			MainWindowVM.RunKeepingWholeLinkSets(scope, keep =>
				MainWindowVM.ForEachGroupCluster(scope, (d, first) => d.EqualsButQuality(first), (first, cluster) => {
					cluster.Add(first);
					cluster = cluster.OrderByDescending(s => s.ItemInfo.SizeLong).ToList();
					cluster[0].Checked = false;
					for (int i = 1; i < cluster.Count; i++)
						cluster[i].Checked = true;
					keep(cluster[0]);
				}));

			Assert.False(copy.Checked);
			Assert.True(linkA.Checked && linkB.Checked);
			Assert.Equal(500, HardLinkSelection.FreedBytes(scope.Where(s => s.Checked)));
		}

		[Fact]
		public void LinksOf_FindsEveryListedNameAcrossGroups() {
			var a = Item(Guid.NewGuid(), @"D:\a.mp4", linkSet: 3, linkCount: 2);
			var b = Item(Guid.NewGuid(), @"D:\b.mp4", linkSet: 3, linkCount: 2);
			var x = Item(Guid.NewGuid(), @"D:\x.mp4");
			Assert.Equal(new[] { a, b }, HardLinkSelection.LinksOf(a, new[] { a, x, b }));
			Assert.Empty(HardLinkSelection.LinksOf(x, new[] { a, x, b }));
		}

		// ---- badge tooltip ----

		[Fact]
		public void Tooltip_ListsOtherNamesAndLinksOutsideTheResults() {
			var a = Item(Guid.NewGuid(), @"D:\a.mp4", linkSet: 1, linkCount: 4);
			string tip = HardLinkSelection.BuildTooltip(a, new[] { @"D:\b.mp4" }, Lang);
			Assert.Contains("One of 4 names.", tip);
			Assert.Contains(@"D:\b.mp4", tip);
			Assert.Contains("2 outside.", tip);

			var lone = Item(Guid.NewGuid(), @"D:\lone.mp4", linkSet: 2, linkCount: 2);
			string loneTip = HardLinkSelection.BuildTooltip(lone, Array.Empty<string>(), Lang);
			Assert.Contains("No others.", loneTip);
			Assert.Contains("1 outside.", loneTip);

			var pair = Item(Guid.NewGuid(), @"D:\p.mp4", linkSet: 3, linkCount: 2);
			Assert.DoesNotContain("outside", HardLinkSelection.BuildTooltip(pair, new[] { @"D:\q.mp4" }, Lang));
		}
	}
}
