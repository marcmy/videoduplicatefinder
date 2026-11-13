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

using System.Runtime.InteropServices;

namespace VDF.Core.Utils {
	public static class CoreUtils {
		public static bool IsWindows;
		public static string CurrentFolder;
		static CoreUtils() {
			IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
			CurrentFolder = AppContext.BaseDirectory;
		}

		/// <summary>
		/// One-line process memory snapshot for phase logs and the diagnostics export,
		/// so memory reports (#878) show which scan phase balloons and by how much.
		/// </summary>
		public static string DescribeProcessMemory() {
			var gc = GC.GetGCMemoryInfo();
			return $"working set {Environment.WorkingSet / (1024 * 1024)} MB, " +
				$"GC heap {gc.HeapSizeBytes / (1024 * 1024)} MB, " +
				$"GC committed {gc.TotalCommittedBytes / (1024 * 1024)} MB";
		}

		// Where user data lives (VDF_DATA_DIR > portable.txt > Scoop persist > OS default),
		// resolved once per process; see DataFolder. The first access also copies data
		// out of the install folder once, so it must happen before anything opens it.
		static readonly Lazy<DataFolderResolution> _DataFolder = new(ResolveDataFolder);
		public static DataFolderResolution DataFolderInfo => _DataFolder.Value;
		/// <summary>Result of the one-time copy out of the install folder (empty if none ran).</summary>
		public static DataFolderMigrationResult DataFolderMigration { get; private set; } = DataFolderMigrationResult.None;
		static readonly Lazy<bool> _CurrentFolderWritable = new(() => CanWriteToDirectory(CurrentFolder!));
		// Portable mode (keep data next to the executable) is a convenience for desktop/unzip installs.
		// In Docker the working directory is writable by coincidence but is ephemeral, so skip it there.
		static readonly Lazy<bool> _RunningInContainer = new(() =>
			string.Equals(Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER"), "true", StringComparison.OrdinalIgnoreCase));

		public static string StateFolder => _DataFolder.Value.StateFolder;
		public static string SettingsFolder => _DataFolder.Value.SettingsFolder;
		public static bool IsCurrentFolderWritable => _CurrentFolderWritable.Value;
		public static bool IsRunningInContainer => _RunningInContainer.Value;

		public static bool CanWriteToDirectory(string path) {
			try {
				if (!Directory.Exists(path))
					return false;
				var testFile = Path.Combine(path, $".vdf_write_test_{Guid.NewGuid():N}");
				using var stream = new FileStream(testFile, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1, FileOptions.DeleteOnClose);
				stream.WriteByte(0);
				return true;
			}
			catch {
				return false;
			}
		}

		public static string ResolveDatabaseFolder(string? customFolder) {
			if (Directory.Exists(customFolder))
				return customFolder!;
			return StateFolder;
		}

		static DataFolderResolution ResolveDataFolder() {
			var env = DataFolderEnvironment.Current();
			var resolution = DataFolder.Resolve(env);
			try {
				Directory.CreateDirectory(resolution.StateFolder);
				Directory.CreateDirectory(resolution.SettingsFolder);
			}
			catch (Exception ex) when (resolution.Source == DataFolderSource.Override) {
				// An unusable VDF_DATA_DIR must not stop the app; fall back and say why.
				var fallback = DataFolder.Resolve(env with { GetEnvironmentVariable = v => v == DataFolder.OverrideVariable ? null : env.GetEnvironmentVariable(v) });
				Directory.CreateDirectory(fallback.StateFolder);
				Directory.CreateDirectory(fallback.SettingsFolder);
				DataFolderMigration = new DataFolderMigrationResult(string.Empty,
					new[] { $"{DataFolder.OverrideVariable}={resolution.StateFolder} is not usable ({ex.Message}); using {fallback.StateFolder}." }, false);
				resolution = fallback;
			}
			try {
				var migration = DataFolder.MigrateFromInstallFolder(CurrentFolder, resolution, IsWindows);
				if (migration.Messages.Count > 0)
					DataFolderMigration = migration;
			}
			catch (Exception ex) {
				DataFolderMigration = new DataFolderMigrationResult(CurrentFolder, new[] { $"Data folder migration failed: {ex.Message}" }, false);
			}
			return resolution;
		}

		public static string GetDefaultStateFolder() {
			string baseFolder;
			if (IsWindows) {
				baseFolder = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
			}
			else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) {
				baseFolder = Path.Combine(
					Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
					"Library", "Application Support");
			}
			else {
				baseFolder = Environment.GetEnvironmentVariable("XDG_STATE_HOME")
					?? Path.Combine(
						Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
						".local", "state");
			}
			var folder = Path.Combine(baseFolder, "VDF");
			Directory.CreateDirectory(folder);
			return folder;
		}

		public static string GetDefaultSettingsFolder() {
			string baseFolder;
			if (IsWindows) {
				baseFolder = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
			}
			else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) {
				baseFolder = Path.Combine(
					Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
					"Library", "Preferences");
			}
			else {
				baseFolder = Environment.GetEnvironmentVariable("XDG_CONFIG_HOME")
					?? Path.Combine(
						Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
						".config");
			}
			var folder = Path.Combine(baseFolder, "VDF");
			Directory.CreateDirectory(folder);
			return folder;
		}
	}
}
