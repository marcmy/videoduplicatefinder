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
// Settings -> Processing -> Drive types edits SettingsFile.DriveTypeOverrides, which was
// only reachable by hand-editing Settings.json.

using System.Reactive.Linq;
using Avalonia.Controls;
using Avalonia.LogicalTree;
using VDF.GUI.Data;
using VDF.GUI.ViewModels;
using VDF.GUI.Views;

namespace VDF.GUI.HeadlessTests;

public class DriveTypeSettingsTests {

	[Fact]
	public Task DriveTypesRow_EditsTheOverrideMap() => HeadlessUi.Run(() => {
		var map = SettingsFile.Instance.DriveTypeOverrides;
		var before = new Dictionary<string, string>(map, StringComparer.OrdinalIgnoreCase);
		var vm = new MainWindowVM();
		var view = new SettingsView { DataContext = vm };
		var window = HeadlessUi.Show(view);
		try {
			map.Clear();
			map["C:"] = "SSD";
			vm.RebuildDriveTypeRows(new[] { "C:", "F:" });
			view.FindControl<SettingRow>("DriveTypesRow")!.IsVisible = true;
			foreach (var panel in view.FindControl<StackPanel>("SectionsHost")!.Children.OfType<StackPanel>())
				panel.IsVisible = true;
			HeadlessUi.Pump();

			var combos = view.FindControl<SettingRow>("DriveTypesRow")!.GetLogicalDescendants().OfType<ComboBox>().ToList();
			Assert.Equal(2, combos.Count);
			Assert.Equal((int)DriveTypeChoice.Fast, combos[0].SelectedIndex); // read from the existing "C:" key
			Assert.Equal((int)DriveTypeChoice.Automatic, combos[1].SelectedIndex);

			combos[1].SelectedIndex = (int)DriveTypeChoice.Slow;
			Assert.Equal("HDD", map["F:"]);

			combos[0].SelectedIndex = (int)DriveTypeChoice.Automatic;
			Assert.False(map.ContainsKey("C:"));
			Assert.Single(map);

			AccessibleNameTests.AssertEverythingIsNamed(window);
		}
		finally {
			window.Close();
			map.Clear();
			foreach (var (k, v) in before) map[k] = v;
		}
	});

	[Fact]
	public Task AddingARoot_ListsIt_WithoutWritingAnOverride() => HeadlessUi.Run(async () => {
		var map = SettingsFile.Instance.DriveTypeOverrides;
		int countBefore = map.Count;
		var vm = new MainWindowVM { NewDriveTypeRoot = "/mnt/vdf-test-usb/" };
		await vm.AddDriveTypeRootCommand.Execute();

		Assert.Contains(vm.DriveTypeRows, r => r.Root == "/mnt/vdf-test-usb");
		Assert.Equal(string.Empty, vm.NewDriveTypeRoot);
		Assert.Equal(countBefore, map.Count); // nothing chosen yet, so nothing stored
	});
}
