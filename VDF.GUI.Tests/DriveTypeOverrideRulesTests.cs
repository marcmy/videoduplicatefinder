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
// The settings page edits SettingsFile.DriveTypeOverrides, which the scan reads through
// Core's DriveScanPlanner. These pin that both read the map the same way.

using VDF.GUI.Data;

namespace VDF.GUI.Tests;

public class DriveTypeOverrideRulesTests {

	static Dictionary<string, string> Map(params (string Key, string Value)[] entries) {
		var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		foreach (var (k, v) in entries) map[k] = v;
		return map;
	}

	[Theory]
	[InlineData("SSD", DriveTypeChoice.Fast)]
	[InlineData("ssd", DriveTypeChoice.Fast)]
	[InlineData("HDD", DriveTypeChoice.Slow)]
	[InlineData("nvme", DriveTypeChoice.Automatic)] // the scan ignores unusable values
	public void Get_ReadsValuesLikeTheScan(string value, DriveTypeChoice expected) =>
		Assert.Equal(expected, DriveTypeOverrideRules.Get(Map(("D:", value)), @"D:\"));

	[Theory]
	[InlineData("C:", @"C:\")]
	[InlineData(@"C:\", "C:")]
	[InlineData("c:", @"C:\")]
	public void Get_TrailingSeparatorAndCase_NameTheSameDrive(string key, string root) =>
		Assert.Equal(DriveTypeChoice.Fast, DriveTypeOverrideRules.Get(Map((key, "SSD")), root));

	[Fact]
	public void Get_UnlistedDrive_IsAutomatic() =>
		Assert.Equal(DriveTypeChoice.Automatic, DriveTypeOverrideRules.Get(Map(("C:", "SSD")), @"D:\"));

	[Fact]
	public void Set_ExistingKey_IsUpdatedInPlace_KeepingItsSpelling() {
		var map = Map(("C:", "SSD"), ("F:", "HDD"));
		Assert.True(DriveTypeOverrideRules.Set(map, @"C:\", DriveTypeChoice.Slow));
		Assert.Equal(Map(("C:", "HDD"), ("F:", "HDD")), map);
		Assert.True(map.ContainsKey("C:"));
	}

	[Fact]
	public void Set_DuplicateSpellings_CollapseToOneEntry() {
		var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["C:"] = "SSD", [@"C:\"] = "HDD" };
		DriveTypeOverrideRules.Set(map, "C:", DriveTypeChoice.Fast);
		Assert.Single(map);
		Assert.Equal("SSD", map["C:"]);
	}

	[Fact]
	public void Set_NewDrive_IsStoredWithoutTrailingSeparator() {
		var map = Map();
		Assert.True(DriveTypeOverrideRules.Set(map, @"G:\", DriveTypeChoice.Slow));
		Assert.Equal(Map(("G:", "HDD")), map);
	}

	[Fact]
	public void Set_Automatic_RemovesTheOverride() {
		var map = Map(("C:", "SSD"), ("F:", "HDD"));
		Assert.True(DriveTypeOverrideRules.Set(map, @"F:\", DriveTypeChoice.Automatic));
		Assert.Equal(Map(("C:", "SSD")), map);
	}

	[Fact]
	public void Set_NoChange_ReportsNothingChanged() {
		var map = Map(("C:", "SSD"));
		Assert.False(DriveTypeOverrideRules.Set(map, @"C:\", DriveTypeChoice.Fast));
		Assert.False(DriveTypeOverrideRules.Set(map, @"D:\", DriveTypeChoice.Automatic));
		Assert.Equal(Map(("C:", "SSD")), map);
	}

	[Fact]
	public void ListRoots_UnionsDrivesIncludesOverridesAndAdded_OnePerRoot() {
		var roots = DriveTypeOverrideRules.ListRoots(
			driveRoots: new[] { @"C:\", @"F:\", @"Z:\" },
			includedFolders: new[] { @"F:\Videos", @"\\nas\media\clips" },
			rootOfIncludedFolder: f => f.StartsWith(@"\\") ? @"\\nas\media" : f[..3],
			overrideKeys: new[] { "C:", "R:" },
			addedRoots: new[] { "/mnt/usb/" });
		// Ordinal order: '/' sorts before letters, '\' after them.
		Assert.Equal(new[] { "/mnt/usb", "C:", "F:", "R:", "Z:", @"\\nas\media" }, roots);
	}

	[Fact]
	public void ListRoots_ARootLookupThatFails_SkipsThatFolder() {
		var roots = DriveTypeOverrideRules.ListRoots(new[] { @"C:\" }, new[] { "bad" },
			_ => throw new IOException("unreadable"), Array.Empty<string>(), Array.Empty<string>());
		Assert.Equal(new[] { "C:" }, roots);
	}

	[Fact]
	public void ListRoots_FilesystemRoot_KeepsItsSlash() =>
		Assert.Equal(new[] { "/" }, DriveTypeOverrideRules.ListRoots(Array.Empty<string>(), Array.Empty<string>(),
			_ => null, Array.Empty<string>(), new[] { "/" }));
}
