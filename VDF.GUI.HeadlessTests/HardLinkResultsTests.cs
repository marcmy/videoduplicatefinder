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
using VDF.GUI.Data;
using VDF.GUI.ViewModels;

namespace VDF.GUI.HeadlessTests;

/// <summary>
/// Hard-linked results in the main view model, as a scan with "Exclude hard links" on leaves
/// them: every name listed, names of one file sharing a link-set id. Covers the running "space
/// to be freed", the row menu's "select all hard links", the group keeper commands treating a
/// link set as one file, the badge, and the delete confirmation's warning in the real locale.
/// </summary>
public sealed class HardLinkResultsTests {
	const long Size = 2_000_000_000;

	static DuplicateItemVM Add(MainWindowVM vm, Guid group, string path, int linkSet = 0, int linkCount = 0, long size = Size, int width = 1920) {
		var item = new DuplicateItemVM(new DuplicateItem {
			Path = path, GroupId = group, Folder = Path.GetDirectoryName(path)!, SizeLong = size, Similarity = 100f,
			Duration = TimeSpan.FromSeconds(10), FrameSize = $"{width}x{width * 9 / 16}", FrameSizeInt = width * (width * 9 / 16),
			HardLinkSetId = linkSet, HardLinkCount = linkCount,
		});
		vm.Duplicates.Add(item);
		return item;
	}

	[Fact]
	public Task SpaceToBeFreed_CountsALinkedFileOnlyWhenAllItsLinksAreChecked() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		Guid g1 = Guid.NewGuid(), g2 = Guid.NewGuid();
		var a = Add(vm, g1, @"D:\Videos\a.mp4", linkSet: 1, linkCount: 2);
		var x = Add(vm, g1, @"D:\Videos\x.mp4", size: 500);
		var b = Add(vm, g2, @"D:\Other\b.mp4", linkSet: 1, linkCount: 2);
		Add(vm, g2, @"D:\Other\y.mp4", size: 700);

		a.Checked = true;
		Assert.Equal(0, vm.DuplicatesCheckedSizeInternal);
		Assert.Equal(1, vm.DuplicatesCheckedCounter);
		x.Checked = true;
		Assert.Equal(500, vm.DuplicatesCheckedSizeInternal);
		b.Checked = true;
		Assert.Equal(Size + 500, vm.DuplicatesCheckedSizeInternal);
		a.Checked = false;
		Assert.Equal(500, vm.DuplicatesCheckedSizeInternal);

		// Removing a checked entry from the list takes it out of the count as well.
		a.Checked = true;
		vm.Duplicates.Remove(b);
		Assert.Equal(500, vm.DuplicatesCheckedSizeInternal);
	});

	[Fact]
	public Task SpaceToBeFreed_LinkOutsideTheResultsMeansNothingIsFreed() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		var g = Guid.NewGuid();
		var a = Add(vm, g, @"D:\Videos\a.mp4", linkSet: 1, linkCount: 3);
		var b = Add(vm, g, @"D:\Videos\b.mp4", linkSet: 1, linkCount: 3);
		Add(vm, g, @"D:\Videos\x.mp4");
		a.Checked = true;
		b.Checked = true;
		Assert.Equal(0, vm.DuplicatesCheckedSizeInternal);
	});

	[Fact]
	public Task SelectAllHardLinks_ChecksEveryNameOfTheFileAcrossGroups_InOneUndoStep() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		Guid g1 = Guid.NewGuid(), g2 = Guid.NewGuid();
		var a = Add(vm, g1, @"D:\Videos\a.mp4", linkSet: 4, linkCount: 2);
		var x = Add(vm, g1, @"D:\Videos\x.mp4");
		var b = Add(vm, g2, @"D:\Other\b.mp4", linkSet: 4, linkCount: 2);
		var y = Add(vm, g2, @"D:\Other\y.mp4");

		vm.SelectAllHardLinksCommand.Execute(a).Subscribe();
		Assert.True(a.Checked && b.Checked);
		Assert.False(x.Checked || y.Checked);
		Assert.Equal(Size, vm.DuplicatesCheckedSizeInternal);

		vm.UndoSelectionCommand.Execute().Subscribe();
		Assert.False(a.Checked || b.Checked);

		// Not a link: nothing to select.
		vm.SelectAllHardLinksCommand.Execute(x).Subscribe();
		Assert.False(x.Checked);
	});

	[Fact]
	public Task KeepBestInGroup_KeepsEveryNameOfTheBestFile() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		Guid g1 = Guid.NewGuid(), g2 = Guid.NewGuid();
		// The linked file has the higher resolution, so it is the keeper.
		var a = Add(vm, g1, @"D:\Videos\a.mp4", linkSet: 1, linkCount: 3, size: 900);
		var b = Add(vm, g1, @"D:\Videos\b.mp4", linkSet: 1, linkCount: 3, size: 900);
		var copy = Add(vm, g1, @"D:\Videos\copy.mp4", size: 400, width: 1280);
		// Its third name sits in another group and was marked earlier.
		var c = Add(vm, g2, @"D:\Other\c.mp4", linkSet: 1, linkCount: 3, size: 900);
		Add(vm, g2, @"D:\Other\z.mp4", size: 100);
		c.Checked = true;

		vm.KeepBestInGroup(g1);

		Assert.True(copy.Checked);
		Assert.False(a.Checked || b.Checked || c.Checked);
		Assert.Equal(400, vm.DuplicatesCheckedSizeInternal);
	});

	[Fact]
	public Task CustomSelection_KeepsEveryNameOfTheKeptFile() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		var g = Guid.NewGuid();
		var a = Add(vm, g, @"D:\Videos\a.mp4", linkSet: 1, linkCount: 2);
		var b = Add(vm, g, @"D:\Videos\b.mp4", linkSet: 1, linkCount: 2);
		var copy = Add(vm, g, @"D:\Videos\copy.mp4");

		// "Any" over the whole group keeps the first item and checks the rest.
		vm.RunCustomSelection(new CustomSelectionData { IgnoreGroupsWithCheckedItems = false });

		Assert.False(a.Checked);
		Assert.False(b.Checked);
		Assert.True(copy.Checked);
	});

	[Fact]
	public Task Badge_NamesOfOneFileShareNumberAndColour_TooltipListsTheOthers() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		Guid g1 = Guid.NewGuid(), g2 = Guid.NewGuid();
		var a = Add(vm, g1, @"D:\Videos\a.mp4", linkSet: 2, linkCount: 3);
		var x = Add(vm, g1, @"D:\Videos\x.mp4");
		var b = Add(vm, g2, @"D:\Other\b.mp4", linkSet: 2, linkCount: 3);
		var c = Add(vm, g2, @"D:\Other\c.mp4", linkSet: 3, linkCount: 2);
		Add(vm, g2, @"D:\Other\y.mp4");

		vm.RefreshHardLinkDisplay();

		Assert.True(a.IsHardLink && b.IsHardLink && c.IsHardLink);
		Assert.False(x.IsHardLink);
		Assert.Equal(a.HardLinkBadgeText, b.HardLinkBadgeText);
		Assert.NotEqual(a.HardLinkBadgeText, c.HardLinkBadgeText);
		Assert.Contains("2", a.HardLinkBadgeText);
		Assert.Same(a.HardLinkBadgeBrush, b.HardLinkBadgeBrush);
		Assert.NotSame(a.HardLinkBadgeBrush, c.HardLinkBadgeBrush);
		Assert.Contains(@"D:\Other\b.mp4", a.HardLinkTooltip);
		Assert.DoesNotContain(@"D:\Videos\a.mp4", a.HardLinkTooltip);
		// Three names on disk, two listed: one more outside the results.
		Assert.Contains("1", a.HardLinkTooltip!.Split(Environment.NewLine).Last());

		// Once the entry stops being a link (e.g. its other names were deleted), the badge goes.
		a.ItemInfo.HardLinkSetId = 0;
		vm.RefreshHardLinkDisplay();
		Assert.False(a.IsHardLink);
		Assert.Null(a.HardLinkBadgeText);
	});

	[Fact]
	public Task DeleteWarning_UsesTheLocaleAndListsAffectedFiles() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		var g = Guid.NewGuid();
		var a = Add(vm, g, @"D:\Videos\a.mp4", linkSet: 1, linkCount: 2);
		var b = Add(vm, g, @"D:\Videos\b.mp4", linkSet: 1, linkCount: 2);
		var c = Add(vm, g, @"D:\Videos\c.mp4", linkSet: 2, linkCount: 2);
		var x = Add(vm, g, @"D:\Videos\x.mp4");

		Assert.Null(MainWindowVM.BuildHardLinkDeleteWarning(new[] { x }));

		string partial = MainWindowVM.BuildHardLinkDeleteWarning(new[] { c, x })!;
		Assert.Contains(App.Lang["Message.HardLinkPartialWarning"], partial);
		Assert.DoesNotContain(App.Lang["Message.HardLinkWholeWarning"], partial);
		Assert.Contains(@"D:\Videos\c.mp4", partial);

		string whole = MainWindowVM.BuildHardLinkDeleteWarning(new[] { a, b })!;
		Assert.Contains(App.Lang["Message.HardLinkWholeWarning"], whole);
		Assert.Contains(@"D:\Videos\a.mp4", whole);
		Assert.Contains(@"D:\Videos\b.mp4", whole);
	});

	[Fact]
	public Task ResolveWithSettingOff_ClearsTagsCarriedByImportedResults() => HeadlessUi.Run(async () => {
		bool saved = SettingsFile.Instance.ExcludeHardLinks;
		try {
			SettingsFile.Instance.ExcludeHardLinks = false;
			var vm = new MainWindowVM();
			var g = Guid.NewGuid();
			var a = Add(vm, g, @"D:\Videos\a.mp4", linkSet: 1, linkCount: 2);
			var b = Add(vm, g, @"D:\Videos\b.mp4", linkSet: 1, linkCount: 2);
			a.Checked = true;
			b.Checked = true;
			await vm.ResolveHardLinksAsync(onlyTagged: false);
			Assert.Equal(0, a.ItemInfo.HardLinkSetId);
			Assert.Equal(0, b.ItemInfo.HardLinkSetId);
			// Untagged, the two entries count as ordinary files again.
			Assert.Equal(2 * Size, vm.DuplicatesCheckedSizeInternal);
		}
		finally {
			SettingsFile.Instance.ExcludeHardLinks = saved;
		}
	});
}
