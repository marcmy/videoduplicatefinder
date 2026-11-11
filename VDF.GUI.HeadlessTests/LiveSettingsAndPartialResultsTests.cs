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
// Settings stay editable while a scan runs (they apply to the next scan), and a stopped
// scan can finish with partial results. These pin the GUI side of both.

using Avalonia.Controls;
using Avalonia.LogicalTree;
using Avalonia.VisualTree;
using VDF.GUI.Data;
using VDF.GUI.ViewModels;
using VDF.GUI.Views;

namespace VDF.GUI.HeadlessTests;

public class LiveSettingsAndPartialResultsTests {

	[Fact]
	public Task SettingsPage_StaysEditableDuringAScan_AndSaysChangesApplyNextScan() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM { IsScanning = true };
		var view = new SettingsView { DataContext = vm };
		var window = HeadlessUi.Show(view);
		try {
			var firstToggle = view.GetLogicalDescendants().OfType<ToggleSwitch>().First();
			Assert.True(firstToggle.IsEffectivelyEnabled, "settings are locked while a scan runs");
			Assert.True(view.FindControl<Border>("ScanRunningNote")!.IsVisible);

			vm.IsScanning = false;
			HeadlessUi.Pump();
			Assert.False(view.FindControl<Border>("ScanRunningNote")!.IsVisible);
		}
		finally {
			window.Close();
			vm.IsScanning = false;
		}
	});

	[Fact]
	public Task SyncCoreSettings_LeavesTheRunningScansCopyAlone() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		var settings = SettingsFile.Instance;
		float percentBefore = settings.Percent;
		try {
			settings.Percent = 91;
			vm.SyncCoreSettings();
			Assert.Equal(91f, vm.Scanner.Settings.Percent);

			vm.IsScanning = true;
			settings.Percent = 77;
			vm.SyncCoreSettings();
			Assert.Equal(91f, vm.Scanner.Settings.Percent); // the scan keeps what it started with

			vm.IsScanning = false;
			vm.SyncCoreSettings();
			Assert.Equal(77f, vm.Scanner.Settings.Percent); // and the next one gets the edit
		}
		finally {
			vm.IsScanning = false;
			settings.Percent = percentBefore;
		}
	});

	[Fact]
	public Task ActionsThatTouchWhatAScanUses_StayLockedDuringAScan() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM { IsScanning = true };
		try {
			Assert.False(((System.Windows.Input.ICommand)vm.DownloadAiComponentsCommand).CanExecute(null));
			Assert.False(((System.Windows.Input.ICommand)vm.ExportGrayBytesCommand).CanExecute(null));
		}
		finally {
			vm.IsScanning = false;
		}
	});

	[Fact]
	public Task PartialFlag_DrivesTheResultsBannerAndTheEmptyNoticeWording() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		Assert.False(vm.ShowPartialResultsNotice);
		string completeTitle = vm.NoDuplicatesNoticeTitle;

		vm.LastScanWasPartial = true;
		Assert.True(vm.ShowPartialResultsNotice);
		Assert.NotEqual(completeTitle, vm.NoDuplicatesNoticeTitle);

		vm.DismissPartialResultsNoticeCommand.Execute().Subscribe();
		Assert.False(vm.ShowPartialResultsNotice);
		Assert.True(vm.LastScanWasPartial); // dismissing the banner does not change what the results are

		vm.DiscardResultsToSetup();
		Assert.False(vm.LastScanWasPartial);
		Assert.Equal(completeTitle, vm.NoDuplicatesNoticeTitle);
	});

	[Fact]
	public Task ResultsView_ShowsTheStoppedEarlyBanner() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM();
		var view = new DuplicateResultsView { DataContext = vm };
		var window = HeadlessUi.Show(view);
		try {
			var banner = view.GetVisualDescendants().OfType<Border>().First(b => b.Name == "PartialResultsBanner");
			Assert.False(banner.IsVisible);
			vm.LastScanWasPartial = true;
			HeadlessUi.Pump();
			Assert.True(banner.IsVisible);
		}
		finally {
			window.Close();
		}
	});

	[Fact]
	public Task ScanningView_StoppingWithResults_SwapsThePauseNote() => HeadlessUi.Run(() => {
		var vm = new MainWindowVM { IsScanning = true };
		var view = new ScanningView { DataContext = vm };
		var window = HeadlessUi.Show(view);
		try {
			var note = view.FindControl<TextBlock>("StoppingNote")!;
			Assert.False(note.IsVisible);
			vm.IsStoppingWithResults = true;
			HeadlessUi.Pump();
			Assert.True(note.IsVisible);
		}
		finally {
			window.Close();
			vm.IsScanning = false;
		}
	});

	[Fact]
	public void ShowPartialResultsOnStop_IsOnByDefault() =>
		Assert.True(new SettingsFile().ShowPartialResultsOnStop);
}
