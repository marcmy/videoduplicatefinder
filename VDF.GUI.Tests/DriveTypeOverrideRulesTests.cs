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

	// Roots are built with the platform's separator: the rules trim it like Core's scan does,
	// and on Linux a backslash is an ordinary character, so "C:\\" is not "C:" there.
	static readonly string Sep = Path.DirectorySeparatorChar.ToString();

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
		Assert.Equal(expected, DriveTypeOverrideRules.Get(Map(("D:", value)), $"D:{Sep}"));

	[Theory]
	// "|" stands for the platform separator (attribute arguments must be constants).
	[InlineData("C:", "C:|")]
	[InlineData("C:|", "C:")]
	[InlineData("c:", "C:|")]
	public void Get_TrailingSeparatorAndCase_NameTheSameDrive(string key, string root) =>
		Assert.Equal(DriveTypeChoice.Fast,
			DriveTypeOverrideRules.Get(Map((key.Replace("|", Sep), "SSD")), root.Replace("|", Sep)));

	[Fact]
	public void Get_UnlistedDrive_IsAutomatic() =>
		Assert.Equal(DriveTypeChoice.Automatic, DriveTypeOverrideRules.Get(Map(("C:", "SSD")), $"D:{Sep}"));

	[Fact]
	public void Set_ExistingKey_IsUpdatedInPlace_KeepingItsSpelling() {
		var map = Map(("C:", "SSD"), ("F:", "HDD"));
		Assert.True(DriveTypeOverrideRules.Set(map, $"C:{Sep}", DriveTypeChoice.Slow));
		Assert.Equal(Map(("C:", "HDD"), ("F:", "HDD")), map);
		Assert.True(map.ContainsKey("C:"));
	}

	[Fact]
	public void Set_DuplicateSpellings_CollapseToOneEntry() {
		var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["C:"] = "SSD", [$"C:{Sep}"] = "HDD" };
		DriveTypeOverrideRules.Set(map, "C:", DriveTypeChoice.Fast);
		Assert.Single(map);
		Assert.Equal("SSD", map["C:"]);
	}

	[Fact]
	public void Set_NewDrive_IsStoredWithoutTrailingSeparator() {
		var map = Map();
		Assert.True(DriveTypeOverrideRules.Set(map, $"G:{Sep}", DriveTypeChoice.Slow));
		Assert.Equal(Map(("G:", "HDD")), map);
	}

	[Fact]
	public void Set_Automatic_RemovesTheOverride() {
		var map = Map(("C:", "SSD"), ("F:", "HDD"));
		Assert.True(DriveTypeOverrideRules.Set(map, $"F:{Sep}", DriveTypeChoice.Automatic));
		Assert.Equal(Map(("C:", "SSD")), map);
	}

	[Fact]
	public void Set_NoChange_ReportsNothingChanged() {
		var map = Map(("C:", "SSD"));
		Assert.False(DriveTypeOverrideRules.Set(map, $"C:{Sep}", DriveTypeChoice.Fast));
		Assert.False(DriveTypeOverrideRules.Set(map, $"D:{Sep}", DriveTypeChoice.Automatic));
		Assert.Equal(Map(("C:", "SSD")), map);
	}

	[Fact]
	public void ListRoots_IsTheDetectedDrives_OnePerRoot_Sorted() {
		var roots = DriveTypeOverrideRules.ListRoots(new[] { $"Z:{Sep}", $"C:{Sep}", $"F:{Sep}", "c:", " " });
		Assert.Equal(new[] { "C:", "F:", "Z:" }, roots);
	}

	[Fact]
	public void ListRoots_FilesystemRoot_KeepsItsSlash() =>
		Assert.Equal(new[] { "/" }, DriveTypeOverrideRules.ListRoots(new[] { "/" }));
}
