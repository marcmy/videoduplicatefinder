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
using System.Linq;
using System.Text;

namespace VDF.Core.Utils {

	/// <summary>Why the data folder ended up where it did.</summary>
	public enum DataFolderSource {
		/// <summary><c>VDF_DATA_DIR</c> names the folder.</summary>
		Override,
		/// <summary>A <c>portable.txt</c> marker next to the executable keeps data in the install folder.</summary>
		Portable,
		/// <summary>The executable runs from a Scoop install; data lives in <c>&lt;scoop&gt;\persist\&lt;app&gt;</c>.</summary>
		Scoop,
		/// <summary>The per-user OS default (AppData / XDG / Application Support).</summary>
		Default,
	}

	/// <summary>
	/// Where user data lives. <see cref="StateFolder"/> holds databases, logs, the AI
	/// components and other machine-local state; <see cref="SettingsFolder"/> holds
	/// Settings.json. They are the same folder except in <see cref="DataFolderSource.Default"/>
	/// mode, which keeps the long-standing OS split (LocalAppData vs. roaming AppData, etc.).
	/// </summary>
	public sealed record DataFolderResolution(string StateFolder, string SettingsFolder, DataFolderSource Source, string? ScoopApp = null) {
		public string Describe() => Source switch {
			DataFolderSource.Override => $"{DataFolder.OverrideVariable}",
			DataFolderSource.Portable => $"portable ({DataFolder.PortableMarker})",
			DataFolderSource.Scoop => $"Scoop persist ({ScoopApp})",
			_ => "per-user default",
		};
	}

	/// <summary>Everything the resolver reads from the machine, injectable for tests.</summary>
	public sealed record DataFolderEnvironment(
		string InstallFolder,
		bool IsWindows,
		bool IsMacOS,
		Func<string, string?> GetEnvironmentVariable,
		Func<Environment.SpecialFolder, string> GetFolderPath,
		Func<string, bool> DirectoryExists,
		Func<string, bool> FileExists) {

		public static DataFolderEnvironment Current() => new(
			AppContext.BaseDirectory,
			RuntimeInformation.IsOSPlatform(OSPlatform.Windows),
			RuntimeInformation.IsOSPlatform(OSPlatform.OSX),
			Environment.GetEnvironmentVariable,
			Environment.GetFolderPath,
			Directory.Exists,
			File.Exists);
	}

	/// <summary>Outcome of the one-time copy of user data out of the install folder.</summary>
	public sealed record DataFolderMigrationResult(string SourceFolder, IReadOnlyList<string> Messages, bool Migrated) {
		public static readonly DataFolderMigrationResult None = new(string.Empty, Array.Empty<string>(), false);
	}

	/// <summary>
	/// Resolves VDF's user-data folder so that uninstalling or updating the app never
	/// deletes or replaces databases, settings, logs or downloaded AI components.
	///
	/// Resolution order:
	/// <list type="number">
	/// <item><c>VDF_DATA_DIR</c> environment variable.</item>
	/// <item>Portable mode: a <c>portable.txt</c> file next to the executable. Only the
	/// explicit marker turns this on; data merely sitting in the install folder is
	/// copied out once (see <see cref="MigrateFromInstallFolder"/>) instead.</item>
	/// <item>Scoop (Windows): an executable under <c>&lt;root&gt;\apps\&lt;app&gt;\&lt;version|current&gt;\</c>
	/// uses <c>&lt;root&gt;\persist\&lt;app&gt;\</c>.</item>
	/// <item>The per-user OS default.</item>
	/// </list>
	/// </summary>
	public static class DataFolder {
		public const string OverrideVariable = "VDF_DATA_DIR";
		public const string PortableMarker = "portable.txt";
		/// <summary>Written into the state folder once data has been copied out of an install folder.</summary>
		public const string MigrationMarker = "vdf-data-migration.txt";
		const string AppFolderName = "VDF";
		const string BackupSuffix = ".before-migration";

		/// <summary>Files that only belong in the settings folder.</summary>
		static readonly string[] SettingsFiles = { "Settings.json" };
		/// <summary>Files that belong in the state folder. SQLite databases carry their -wal/-shm sidecars.</summary>
		static readonly string[] StateFiles = {
			"ScannedFiles.db", "ScannedFiles.fpheal1", "UnionEmbeddings.db", "DenseEmbeddings.db",
			"BlacklistedGroups.json", "backup.scanresults", "log.txt",
		};
		static readonly string[] StateDirectories = { "ai" };
		static readonly string[] SqliteSidecars = { "-wal", "-shm" };

		public static DataFolderResolution Resolve(DataFolderEnvironment env) {
			string install = NormalizeFolder(env.InstallFolder);

			string? overrideDir = env.GetEnvironmentVariable(OverrideVariable);
			if (!string.IsNullOrWhiteSpace(overrideDir)) {
				string full = NormalizeFolder(Environment.ExpandEnvironmentVariables(overrideDir.Trim()));
				return new DataFolderResolution(full, full, DataFolderSource.Override);
			}

			if (env.FileExists(Path.Combine(install, PortableMarker)))
				return new DataFolderResolution(install, install, DataFolderSource.Portable);

			if (env.IsWindows && TryDetectScoop(install, env, out string? scoopRoot, out string? app)) {
				string persist = Path.Combine(scoopRoot!, "persist", app!);
				return new DataFolderResolution(persist, persist, DataFolderSource.Scoop, app);
			}

			return new DataFolderResolution(DefaultStateFolder(env), DefaultSettingsFolder(env), DataFolderSource.Default);
		}

		/// <summary>
		/// Finds <c>&lt;root&gt;\apps\&lt;app&gt;\&lt;version&gt;</c> at or above <paramref name="installFolder"/>,
		/// where <c>&lt;root&gt;</c> is <c>SCOOP</c>, <c>SCOOP_GLOBAL</c>, or any folder with Scoop's
		/// <c>shims</c> / <c>persist</c> layout. No root is hard-coded.
		/// </summary>
		public static bool TryDetectScoop(string installFolder, DataFolderEnvironment env, out string? scoopRoot, out string? app) {
			scoopRoot = null;
			app = null;
			var cmp = env.IsWindows ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			string? versionDir = NormalizeFolder(installFolder);
			while (versionDir != null) {
				string? appDir = Path.GetDirectoryName(versionDir);
				string? appsDir = appDir == null ? null : Path.GetDirectoryName(appDir);
				string? root = appsDir == null ? null : Path.GetDirectoryName(appsDir);
				if (root != null && string.Equals(Path.GetFileName(appsDir), "apps", cmp) && IsScoopRoot(root, env, cmp)) {
					scoopRoot = root;
					app = Path.GetFileName(appDir);
					return !string.IsNullOrEmpty(app);
				}
				versionDir = appDir;
			}
			return false;
		}

		static bool IsScoopRoot(string root, DataFolderEnvironment env, StringComparison cmp) {
			foreach (var variable in new[] { "SCOOP", "SCOOP_GLOBAL" }) {
				string? value = env.GetEnvironmentVariable(variable);
				if (!string.IsNullOrWhiteSpace(value) && string.Equals(NormalizeFolder(value), root, cmp))
					return true;
			}
			return env.DirectoryExists(Path.Combine(root, "shims")) || env.DirectoryExists(Path.Combine(root, "persist"));
		}

		static string DefaultStateFolder(DataFolderEnvironment env) {
			string baseFolder;
			if (env.IsWindows)
				baseFolder = env.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
			else if (env.IsMacOS)
				baseFolder = Path.Combine(env.GetFolderPath(Environment.SpecialFolder.UserProfile), "Library", "Application Support");
			else
				baseFolder = NonEmpty(env.GetEnvironmentVariable("XDG_STATE_HOME"))
					?? Path.Combine(env.GetFolderPath(Environment.SpecialFolder.UserProfile), ".local", "state");
			return Path.Combine(baseFolder, AppFolderName);
		}

		static string DefaultSettingsFolder(DataFolderEnvironment env) {
			string baseFolder;
			if (env.IsWindows)
				baseFolder = env.GetFolderPath(Environment.SpecialFolder.ApplicationData);
			else if (env.IsMacOS)
				baseFolder = Path.Combine(env.GetFolderPath(Environment.SpecialFolder.UserProfile), "Library", "Preferences");
			else
				baseFolder = NonEmpty(env.GetEnvironmentVariable("XDG_CONFIG_HOME"))
					?? Path.Combine(env.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config");
			return Path.Combine(baseFolder, AppFolderName);
		}

		static string? NonEmpty(string? s) => string.IsNullOrWhiteSpace(s) ? null : s;

		static string NormalizeFolder(string path) =>
			Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));

		/// <summary>
		/// One-time copy of user data from the install folder into the resolved folders.
		/// Files are copied, never moved, so the install folder keeps its copy as a fallback.
		/// An existing target file is never overwritten: when the install-folder copy is
		/// newer (e.g. a Scoop persist snapshot older than the live install files), the
		/// target is first renamed to <c>&lt;name&gt;.before-migration</c>. Once anything was
		/// copied, <see cref="MigrationMarker"/> is written and the migration never runs again.
		/// </summary>
		public static DataFolderMigrationResult MigrateFromInstallFolder(string installFolder, DataFolderResolution resolution, bool isWindows) {
			string source = NormalizeFolder(installFolder);
			var cmp = isWindows ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
			if (resolution.Source == DataFolderSource.Portable)
				return DataFolderMigrationResult.None;
			if (string.Equals(source, NormalizeFolder(resolution.StateFolder), cmp) &&
				string.Equals(source, NormalizeFolder(resolution.SettingsFolder), cmp))
				return DataFolderMigrationResult.None;
			if (!Directory.Exists(source) || File.Exists(Path.Combine(resolution.StateFolder, MigrationMarker)))
				return DataFolderMigrationResult.None;
			if (!HasAnyData(source)) {
				// Under Scoop the marker also tells the bucket manifest that VDF now owns
				// persist\<app> directly, so its install scripts must stop mirroring state
				// files between the install folder and persist (they would copy stale files
				// over live data, or delete persist files absent from the install folder).
				if (resolution.Source == DataFolderSource.Scoop)
					TryWriteMarker(resolution.StateFolder, new[] { $"VDF stores its data directly in {resolution.StateFolder}; nothing needed copying from {source}." });
				return DataFolderMigrationResult.None;
			}

			var messages = new List<string>();
			bool any = false;
			try {
				Directory.CreateDirectory(resolution.StateFolder);
				Directory.CreateDirectory(resolution.SettingsFolder);
				foreach (var name in SettingsFiles)
					any |= MigrateFile(source, resolution.SettingsFolder, name, Array.Empty<string>(), messages);
				foreach (var name in StateFiles)
					any |= MigrateFile(source, resolution.StateFolder, name,
						name.EndsWith(".db", StringComparison.OrdinalIgnoreCase) ? SqliteSidecars : Array.Empty<string>(), messages);
				foreach (var name in StateDirectories)
					any |= MigrateDirectory(Path.Combine(source, name), Path.Combine(resolution.StateFolder, name), messages);
			}
			catch (Exception ex) {
				messages.Add($"Data folder migration stopped: {ex.Message}");
				// No marker: a later start retries the files that were not copied yet.
				return new DataFolderMigrationResult(source, messages, any);
			}

			messages.Insert(0, $"Copied user data from {source} to {resolution.StateFolder}" +
				(string.Equals(resolution.StateFolder, resolution.SettingsFolder, cmp) ? "" : $" (settings: {resolution.SettingsFolder})") +
				". The originals were left in place.");
			if (!TryWriteMarker(resolution.StateFolder, messages))
				messages.Add($"Could not write {MigrationMarker}; the copy will be re-checked on the next start.");
			return new DataFolderMigrationResult(source, messages, any);
		}

		static bool TryWriteMarker(string stateFolder, IEnumerable<string> lines) {
			try {
				Directory.CreateDirectory(stateFolder);
				var sb = new StringBuilder();
				sb.AppendLine($"VDF manages its user data in this folder (since {DateTime.Now:yyyy-MM-dd HH:mm:ss}).");
				sb.AppendLine("While this file exists VDF will not copy data from its install folder again.");
				sb.AppendLine();
				foreach (var line in lines)
					sb.AppendLine(line);
				File.WriteAllText(Path.Combine(stateFolder, MigrationMarker), sb.ToString());
				return true;
			}
			catch {
				return false;
			}
		}

		static bool HasAnyData(string source) =>
			SettingsFiles.Concat(StateFiles).Any(n => File.Exists(Path.Combine(source, n))) ||
			StateDirectories.Any(d => Directory.Exists(Path.Combine(source, d)));

		static bool MigrateFile(string sourceFolder, string targetFolder, string name, string[] sidecars, List<string> messages) {
			string src = Path.Combine(sourceFolder, name);
			string dst = Path.Combine(targetFolder, name);
			if (!File.Exists(src))
				return false;
			if (File.Exists(dst)) {
				// Two seconds of slack: FAT/exFAT timestamps and Copy-Item round-trips.
				if (File.GetLastWriteTimeUtc(src) <= File.GetLastWriteTimeUtc(dst).AddSeconds(2)) {
					messages.Add($"Kept {dst} (already present and not older than {src}).");
					return false;
				}
				BackUp(dst, messages);
			}
			// A leftover -wal/-shm next to the incoming database would be replayed into it
			// by SQLite, so set any existing target sidecars aside as well.
			foreach (var sidecar in sidecars)
				BackUp(Path.Combine(targetFolder, name + sidecar), messages);
			// Sidecars first, so a crash mid-way never leaves a database without its WAL.
			foreach (var sidecar in sidecars) {
				string s = Path.Combine(sourceFolder, name + sidecar);
				if (File.Exists(s))
					CopyFileSafely(s, Path.Combine(targetFolder, name + sidecar), messages);
			}
			CopyFileSafely(src, dst, messages);
			return true;
		}

		static bool MigrateDirectory(string sourceDir, string targetDir, List<string> messages) {
			if (!Directory.Exists(sourceDir))
				return false;
			bool any = false;
			foreach (var file in Directory.EnumerateFiles(sourceDir, "*", SearchOption.AllDirectories)) {
				string dst = Path.Combine(targetDir, Path.GetRelativePath(sourceDir, file));
				if (File.Exists(dst))
					continue;
				Directory.CreateDirectory(Path.GetDirectoryName(dst)!);
				CopyFileSafely(file, dst, messages);
				any = true;
			}
			return any;
		}

		static void CopyFileSafely(string src, string dst, List<string> messages) {
			// Copy to a temp name and rename, so an interrupted copy of a 400 MB database
			// never leaves a truncated file that a later start would treat as real data.
			string tmp = dst + ".migrating";
			File.Copy(src, tmp, overwrite: true);
			File.SetLastWriteTimeUtc(tmp, File.GetLastWriteTimeUtc(src));
			File.Move(tmp, dst, overwrite: false);
			messages.Add($"Copied {src} -> {dst} ({new FileInfo(dst).Length:N0} bytes).");
		}

		static void BackUp(string existing, List<string> messages) {
			if (!File.Exists(existing)) return;
			string backup = UniqueBackupPath(existing);
			File.Move(existing, backup);
			messages.Add($"Renamed {existing} to {Path.GetFileName(backup)} before copying the newer install-folder copy.");
		}

		static string UniqueBackupPath(string path) {
			string candidate = path + BackupSuffix;
			if (!File.Exists(candidate)) return candidate;
			return $"{path}{BackupSuffix}-{DateTime.Now:yyyyMMdd-HHmmss}";
		}
	}
}
