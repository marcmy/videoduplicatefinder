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

namespace VDF.GUI.Data {

	/// <summary>What the settings page offers per drive. Order matches the dropdown.</summary>
	public enum DriveTypeChoice {
		/// <summary>No override: the scan classifies the drive (network = slow, else a seek probe).</summary>
		Automatic,
		/// <summary>Written as <c>SSD</c>.</summary>
		Fast,
		/// <summary>Written as <c>HDD</c>.</summary>
		Slow,
	}

	/// <summary>
	/// Reads and edits <see cref="SettingsFile.DriveTypeOverrides"/> the way the scan reads it
	/// (Core's DriveScanPlanner): keys are drive or mount roots compared after trimming the
	/// trailing separator, so "C:" and "C:\" name one drive, and values are <c>SSD</c> or
	/// <c>HDD</c>. Kept free of Avalonia so it stays unit-testable.
	/// </summary>
	public static class DriveTypeOverrideRules {

		/// <summary>Same normalization as Core's DriveScanPlanner.NormalizeRoot.</summary>
		internal static string NormalizeRoot(string root) =>
			root.Trim().TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

		/// <summary>
		/// The form a root is shown and stored in: no trailing separator ("C:", "\\nas\media",
		/// "/mnt/usb"), except a filesystem root that would otherwise be empty ("/").
		/// </summary>
		internal static string DisplayRoot(string root) {
			string normalized = NormalizeRoot(root);
			return normalized.Length > 0 ? normalized : root.Trim();
		}

		static bool SameRoot(string a, string b) =>
			string.Equals(NormalizeRoot(a), NormalizeRoot(b), StringComparison.OrdinalIgnoreCase);

		/// <summary>
		/// The choice the scan would apply to <paramref name="root"/>. An unusable value (neither
		/// SSD nor HDD) is ignored by the scan, so it reads as <see cref="DriveTypeChoice.Automatic"/>.
		/// </summary>
		public static DriveTypeChoice Get(IReadOnlyDictionary<string, string> overrides, string root) {
			foreach (var (key, value) in overrides) {
				if (!SameRoot(key, root))
					continue;
				// The scan honours the first entry for a root; so does this.
				if (string.Equals(value, "SSD", StringComparison.OrdinalIgnoreCase)) return DriveTypeChoice.Fast;
				if (string.Equals(value, "HDD", StringComparison.OrdinalIgnoreCase)) return DriveTypeChoice.Slow;
				return DriveTypeChoice.Automatic;
			}
			return DriveTypeChoice.Automatic;
		}

		/// <summary>
		/// Sets <paramref name="root"/> to <paramref name="choice"/>. Every key naming the same root
		/// is replaced by one entry (so "C:" and "C:\" can never disagree); the spelling of the
		/// first existing key is kept. <see cref="DriveTypeChoice.Automatic"/> removes the override.
		/// Returns whether anything changed.
		/// </summary>
		public static bool Set(IDictionary<string, string> overrides, string root, DriveTypeChoice choice) {
			string? keptKey = null;
			var matching = new List<string>();
			foreach (string key in overrides.Keys) {
				if (!SameRoot(key, root)) continue;
				keptKey ??= key;
				matching.Add(key);
			}
			string? newValue = choice switch {
				DriveTypeChoice.Fast => "SSD",
				DriveTypeChoice.Slow => "HDD",
				_ => null,
			};
			if (newValue != null && matching.Count == 1 && overrides[matching[0]] == newValue)
				return false;
			if (newValue == null && matching.Count == 0)
				return false;
			foreach (string key in matching)
				overrides.Remove(key);
			if (newValue != null)
				overrides[keptKey ?? DisplayRoot(root)] = newValue;
			return true;
		}

		/// <summary>
		/// The roots the settings page lists: the machine's drives, the root each included folder
		/// lives under, roots that already carry an override, and roots the user added by hand.
		/// One entry per root (in display form), sorted.
		/// </summary>
		/// <param name="rootOfIncludedFolder">
		/// Maps an included folder to the root the scan groups it under (Core's mount-aware lookup).
		/// </param>
		public static List<string> ListRoots(IEnumerable<string> driveRoots, IEnumerable<string> includedFolders,
			Func<string, string?> rootOfIncludedFolder, IEnumerable<string> overrideKeys, IEnumerable<string> addedRoots) {
			var byNormalized = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			void Add(string? root) {
				if (string.IsNullOrWhiteSpace(root) || root == "?") return;
				string display = DisplayRoot(root);
				byNormalized.TryAdd(NormalizeRoot(root), display);
			}
			foreach (string root in driveRoots) Add(root);
			foreach (string folder in includedFolders) {
				string? root;
				try { root = rootOfIncludedFolder(folder); }
				catch { root = null; }
				Add(root);
			}
			foreach (string key in overrideKeys) Add(key);
			foreach (string root in addedRoots) Add(root);
			var roots = new List<string>(byNormalized.Values);
			roots.Sort(StringComparer.OrdinalIgnoreCase);
			return roots;
		}
	}
}
