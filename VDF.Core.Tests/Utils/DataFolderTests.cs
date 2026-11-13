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

using VDF.Core.Utils;

namespace VDF.Core.Tests.Utils;

public sealed class DataFolderTests : IDisposable {
	readonly string root = Path.Combine(Path.GetTempPath(), "vdf-datafolder-" + Guid.NewGuid().ToString("N"));

	public DataFolderTests() => Directory.CreateDirectory(root);
	public void Dispose() {
		try { Directory.Delete(root, true); } catch { }
	}

	string Dir(params string[] parts) {
		string p = Path.Combine(new[] { root }.Concat(parts).ToArray());
		Directory.CreateDirectory(p);
		return p;
	}

	DataFolderEnvironment Env(string install, bool windows = true, bool mac = false, Dictionary<string, string>? vars = null) =>
		new(install, windows, mac,
			v => vars != null && vars.TryGetValue(v, out var value) ? value : null,
			f => f switch {
				Environment.SpecialFolder.ApplicationData => Path.Combine(root, "home", "AppData", "Roaming"),
				Environment.SpecialFolder.LocalApplicationData => Path.Combine(root, "home", "AppData", "Local"),
				Environment.SpecialFolder.UserProfile => Path.Combine(root, "home"),
				_ => throw new ArgumentOutOfRangeException(nameof(f)),
			},
			Directory.Exists,
			File.Exists);

	// --- resolution -----------------------------------------------------------

	[Fact]
	public void Override_WinsOverEverything() {
		string scoop = Dir("scoop");
		Dir("scoop", "shims");
		string install = Dir("scoop", "apps", "vdf", "current");
		File.WriteAllText(Path.Combine(install, DataFolder.PortableMarker), "");
		string target = Path.Combine(root, "custom");

		var r = DataFolder.Resolve(Env(install, vars: new() { [DataFolder.OverrideVariable] = target }));

		Assert.Equal(DataFolderSource.Override, r.Source);
		Assert.Equal(target, r.StateFolder);
		Assert.Equal(target, r.SettingsFolder);
	}

	[Fact]
	public void BlankOverride_IsIgnored() {
		string install = Dir("app");
		var r = DataFolder.Resolve(Env(install, vars: new() { [DataFolder.OverrideVariable] = "  " }));
		Assert.Equal(DataFolderSource.Default, r.Source);
	}

	[Fact]
	public void PortableMarker_KeepsDataInInstallFolder() {
		string install = Dir("portable-vdf");
		File.WriteAllText(Path.Combine(install, DataFolder.PortableMarker), "");

		var r = DataFolder.Resolve(Env(install + Path.DirectorySeparatorChar));

		Assert.Equal(DataFolderSource.Portable, r.Source);
		Assert.Equal(install, r.StateFolder);
		Assert.Equal(install, r.SettingsFolder);
	}

	[Fact]
	public void PortableMarker_BeatsScoop() {
		Dir("scoop", "shims");
		string install = Dir("scoop", "apps", "vdf", "1.0");
		File.WriteAllText(Path.Combine(install, DataFolder.PortableMarker), "");
		Assert.Equal(DataFolderSource.Portable, DataFolder.Resolve(Env(install)).Source);
	}

	[Fact]
	public void DataInInstallFolder_WithoutMarker_IsNotPortable() {
		string install = Dir("unzipped");
		File.WriteAllText(Path.Combine(install, "ScannedFiles.db"), "x");
		Assert.Equal(DataFolderSource.Default, DataFolder.Resolve(Env(install)).Source);
	}

	[Theory]
	[InlineData("current")]
	[InlineData("4.1.0-perf-git-20261112-1913")]
	public void Scoop_UserInstall_DetectedFromShimsLayout(string versionFolder) {
		string scoop = Dir("Users", "me", "scoop");
		Dir("Users", "me", "scoop", "shims");
		string install = Dir("Users", "me", "scoop", "apps", "videoduplicatefinder-perf", versionFolder);

		var r = DataFolder.Resolve(Env(install));

		Assert.Equal(DataFolderSource.Scoop, r.Source);
		Assert.Equal("videoduplicatefinder-perf", r.ScoopApp);
		Assert.Equal(Path.Combine(scoop, "persist", "videoduplicatefinder-perf"), r.StateFolder);
		Assert.Equal(r.StateFolder, r.SettingsFolder);
	}

	[Fact]
	public void Scoop_GlobalInstall_DetectedFromScoopGlobalVariable() {
		// No shims/persist yet: only SCOOP_GLOBAL identifies the root.
		string global = Dir("ProgramData", "scoop");
		string install = Dir("ProgramData", "scoop", "apps", "vdf", "current");

		var r = DataFolder.Resolve(Env(install, vars: new() { ["SCOOP_GLOBAL"] = global + Path.DirectorySeparatorChar }));

		Assert.Equal(DataFolderSource.Scoop, r.Source);
		Assert.Equal(Path.Combine(global, "persist", "vdf"), r.StateFolder);
	}

	[Fact]
	public void Scoop_CustomRoot_DetectedFromScoopVariable() {
		string custom = Dir("D", "tools");
		string install = Dir("D", "tools", "apps", "vdf", "current");
		var r = DataFolder.Resolve(Env(install, vars: new() { ["SCOOP"] = custom }));
		Assert.Equal(Path.Combine(custom, "persist", "vdf"), r.StateFolder);
	}

	[Fact]
	public void Scoop_ExeInSubfolderOfVersionFolder_StillDetected() {
		string scoop = Dir("scoop");
		Dir("scoop", "persist");
		string install = Dir("scoop", "apps", "vdf", "current", "gui");
		var r = DataFolder.Resolve(Env(install));
		Assert.Equal(Path.Combine(scoop, "persist", "vdf"), r.StateFolder);
	}

	[Fact]
	public void AppsFolderWithoutScoopRoot_IsNotScoop() {
		string install = Dir("work", "apps", "vdf", "current");
		Assert.Equal(DataFolderSource.Default, DataFolder.Resolve(Env(install)).Source);
	}

	[Fact]
	public void Scoop_OnlyOnWindows() {
		Dir("scoop", "shims");
		string install = Dir("scoop", "apps", "vdf", "current");
		Assert.Equal(DataFolderSource.Default, DataFolder.Resolve(Env(install, windows: false)).Source);
	}

	[Fact]
	public void Default_Windows_UsesLocalAppDataForStateAndAppDataForSettings() {
		var r = DataFolder.Resolve(Env(Dir("Program Files", "VDF")));
		Assert.Equal(DataFolderSource.Default, r.Source);
		Assert.Equal(Path.Combine(root, "home", "AppData", "Local", "VDF"), r.StateFolder);
		Assert.Equal(Path.Combine(root, "home", "AppData", "Roaming", "VDF"), r.SettingsFolder);
	}

	[Fact]
	public void Default_MacOS_UsesApplicationSupportAndPreferences() {
		var r = DataFolder.Resolve(Env(Dir("VDF.app"), windows: false, mac: true));
		Assert.Equal(Path.Combine(root, "home", "Library", "Application Support", "VDF"), r.StateFolder);
		Assert.Equal(Path.Combine(root, "home", "Library", "Preferences", "VDF"), r.SettingsFolder);
	}

	[Fact]
	public void Default_Linux_UsesXdgFallbacks() {
		var r = DataFolder.Resolve(Env(Dir("opt", "vdf"), windows: false));
		Assert.Equal(Path.Combine(root, "home", ".local", "state", "VDF"), r.StateFolder);
		Assert.Equal(Path.Combine(root, "home", ".config", "VDF"), r.SettingsFolder);
	}

	[Fact]
	public void Default_Linux_HonoursXdgVariables() {
		string state = Path.Combine(root, "xdg-state"), config = Path.Combine(root, "xdg-config");
		var r = DataFolder.Resolve(Env(Dir("opt", "vdf"), windows: false,
			vars: new() { ["XDG_STATE_HOME"] = state, ["XDG_CONFIG_HOME"] = config }));
		Assert.Equal(Path.Combine(state, "VDF"), r.StateFolder);
		Assert.Equal(Path.Combine(config, "VDF"), r.SettingsFolder);
	}

	// --- migration ------------------------------------------------------------

	static void Write(string path, string content, DateTime? utc = null) {
		Directory.CreateDirectory(Path.GetDirectoryName(path)!);
		File.WriteAllText(path, content);
		if (utc != null) File.SetLastWriteTimeUtc(path, utc.Value);
	}

	DataFolderResolution Split() => new(Path.Combine(root, "state"), Path.Combine(root, "settings"), DataFolderSource.Default);

	[Fact]
	public void Migration_CopiesEverythingIntoEmptyTarget_AndLeavesOriginals() {
		string install = Dir("install");
		Write(Path.Combine(install, "Settings.json"), "settings");
		Write(Path.Combine(install, "ScannedFiles.db"), "db");
		Write(Path.Combine(install, "ScannedFiles.db-wal"), "wal");
		Write(Path.Combine(install, "DenseEmbeddings.db"), "dense");
		Write(Path.Combine(install, "log.txt"), "log");
		Write(Path.Combine(install, "ai", "runtime", "onnx.dll"), "dll");
		Write(Path.Combine(install, "VDF.GUI.exe"), "exe");
		var r = Split();

		var result = DataFolder.MigrateFromInstallFolder(install, r, isWindows: true);

		Assert.True(result.Migrated);
		Assert.Equal("settings", File.ReadAllText(Path.Combine(r.SettingsFolder, "Settings.json")));
		Assert.False(File.Exists(Path.Combine(r.StateFolder, "Settings.json")));
		Assert.Equal("db", File.ReadAllText(Path.Combine(r.StateFolder, "ScannedFiles.db")));
		Assert.Equal("wal", File.ReadAllText(Path.Combine(r.StateFolder, "ScannedFiles.db-wal")));
		Assert.Equal("dense", File.ReadAllText(Path.Combine(r.StateFolder, "DenseEmbeddings.db")));
		Assert.Equal("log", File.ReadAllText(Path.Combine(r.StateFolder, "log.txt")));
		Assert.Equal("dll", File.ReadAllText(Path.Combine(r.StateFolder, "ai", "runtime", "onnx.dll")));
		Assert.False(File.Exists(Path.Combine(r.StateFolder, "VDF.GUI.exe")));
		Assert.True(File.Exists(Path.Combine(install, "ScannedFiles.db")), "copy, not move");
		Assert.True(File.Exists(Path.Combine(r.StateFolder, DataFolder.MigrationMarker)));
		Assert.Empty(Directory.EnumerateFiles(r.StateFolder, "*.migrating", SearchOption.AllDirectories));
		Assert.Contains(result.Messages, m => m.Contains("ScannedFiles.db"));
	}

	[Fact]
	public void Migration_NeverOverwritesNewerTargetFiles() {
		string install = Dir("install");
		var old = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		Write(Path.Combine(install, "ScannedFiles.db"), "stale", old);
		var r = Split();
		Write(Path.Combine(r.StateFolder, "ScannedFiles.db"), "fresh", old.AddDays(1));

		DataFolder.MigrateFromInstallFolder(install, r, isWindows: true);

		Assert.Equal("fresh", File.ReadAllText(Path.Combine(r.StateFolder, "ScannedFiles.db")));
		Assert.Empty(Directory.EnumerateFiles(r.StateFolder, "*.before-migration*"));
	}

	[Fact]
	public void Migration_NewerInstallCopy_SetsOlderTargetAsideInsteadOfOverwriting() {
		// The Scoop case: persist holds an older snapshot, the install folder the live data.
		string install = Dir("install");
		var t = new DateTime(2026, 11, 11, 0, 0, 0, DateTimeKind.Utc);
		Write(Path.Combine(install, "ScannedFiles.db"), "live", t.AddDays(1));
		var r = Split();
		Write(Path.Combine(r.StateFolder, "ScannedFiles.db"), "snapshot", t);
		Write(Path.Combine(r.StateFolder, "ScannedFiles.db-wal"), "snapshot-wal", t);

		DataFolder.MigrateFromInstallFolder(install, r, isWindows: true);

		Assert.Equal("live", File.ReadAllText(Path.Combine(r.StateFolder, "ScannedFiles.db")));
		Assert.Equal("snapshot", File.ReadAllText(Path.Combine(r.StateFolder, "ScannedFiles.db.before-migration")));
		// The old WAL must not be replayed into the new database.
		Assert.False(File.Exists(Path.Combine(r.StateFolder, "ScannedFiles.db-wal")));
		Assert.Equal("snapshot-wal", File.ReadAllText(Path.Combine(r.StateFolder, "ScannedFiles.db-wal.before-migration")));
	}

	[Fact]
	public void Migration_RunsOnlyOnce() {
		string install = Dir("install");
		Write(Path.Combine(install, "Settings.json"), "v1");
		var r = Split();
		Assert.True(DataFolder.MigrateFromInstallFolder(install, r, isWindows: true).Migrated);

		File.Delete(Path.Combine(r.SettingsFolder, "Settings.json"));
		var second = DataFolder.MigrateFromInstallFolder(install, r, isWindows: true);

		Assert.False(second.Migrated);
		Assert.Empty(second.Messages);
		Assert.False(File.Exists(Path.Combine(r.SettingsFolder, "Settings.json")));
	}

	[Fact]
	public void Migration_NothingToCopy_WritesNoMarker() {
		string install = Dir("install");
		Write(Path.Combine(install, "VDF.GUI.exe"), "exe");
		var r = Split();

		var result = DataFolder.MigrateFromInstallFolder(install, r, isWindows: true);

		Assert.False(result.Migrated);
		Assert.False(File.Exists(Path.Combine(r.StateFolder, DataFolder.MigrationMarker)));
	}

	[Fact]
	public void Migration_ScoopWithNothingToCopy_StillMarksPersistAsAppManaged() {
		string install = Dir("install");
		var r = new DataFolderResolution(Path.Combine(root, "persist", "vdf"), Path.Combine(root, "persist", "vdf"), DataFolderSource.Scoop, "vdf");

		var result = DataFolder.MigrateFromInstallFolder(install, r, isWindows: true);

		Assert.False(result.Migrated);
		Assert.True(File.Exists(Path.Combine(r.StateFolder, DataFolder.MigrationMarker)));
	}

	[Fact]
	public void Migration_SkippedInPortableModeAndWhenTargetIsInstallFolder() {
		string install = Dir("install");
		Write(Path.Combine(install, "ScannedFiles.db"), "db");

		var portable = new DataFolderResolution(install, install, DataFolderSource.Portable);
		Assert.False(DataFolder.MigrateFromInstallFolder(install, portable, isWindows: true).Migrated);
		var same = new DataFolderResolution(install, install, DataFolderSource.Override);
		Assert.False(DataFolder.MigrateFromInstallFolder(install, same, isWindows: true).Migrated);
		Assert.False(File.Exists(Path.Combine(install, DataFolder.MigrationMarker)));
	}

	[Fact]
	public void Migration_AiFolderMergesWithoutOverwriting() {
		string install = Dir("install");
		Write(Path.Combine(install, "ai", "model.onnx"), "install-model");
		Write(Path.Combine(install, "ai", "extra.bin"), "extra");
		var r = Split();
		Write(Path.Combine(r.StateFolder, "ai", "model.onnx"), "persist-model");

		DataFolder.MigrateFromInstallFolder(install, r, isWindows: true);

		Assert.Equal("persist-model", File.ReadAllText(Path.Combine(r.StateFolder, "ai", "model.onnx")));
		Assert.Equal("extra", File.ReadAllText(Path.Combine(r.StateFolder, "ai", "extra.bin")));
	}

	[Fact]
	public void TestHost_RunsInPortableMode() {
		// Directory.Build.props drops portable.txt next to every test assembly so tests
		// never touch the developer's real data folder.
		Assert.Equal(DataFolderSource.Portable, CoreUtils.DataFolderInfo.Source);
	}
}
