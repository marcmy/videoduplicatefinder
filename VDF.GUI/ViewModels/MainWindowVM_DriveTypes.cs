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

using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive;
using ReactiveUI;
using VDF.Core.Utils;
using VDF.GUI.Data;

namespace VDF.GUI.ViewModels {

	/// <summary>One drive on the settings page's "Drive types" row.</summary>
	public sealed class DriveTypeRowVM : ReactiveObject {
		readonly Action<DriveTypeRowVM> onChanged;

		public DriveTypeRowVM(string root, DriveTypeChoice choice, IReadOnlyList<string> choiceNames, Action<DriveTypeRowVM> onChanged) {
			Root = root;
			ChoiceNames = choiceNames;
			_SelectedIndex = (int)choice;
			this.onChanged = onChanged;
		}

		/// <summary>The drive or mount root, as stored in the override map ("C:", "\\nas\media", "/mnt/usb").</summary>
		public string Root { get; }

		/// <summary>Dropdown entries, in <see cref="DriveTypeChoice"/> order.</summary>
		public IReadOnlyList<string> ChoiceNames { get; }

		int _SelectedIndex;
		public int SelectedIndex {
			get => _SelectedIndex;
			set {
				// A ComboBox reports -1 while its items are swapped; that is not a choice.
				if (value < 0 || value == _SelectedIndex) return;
				this.RaiseAndSetIfChanged(ref _SelectedIndex, value);
				onChanged(this);
			}
		}

		public DriveTypeChoice Choice => (DriveTypeChoice)_SelectedIndex;
	}

	// Settings -> Processing -> Drive types: per-drive overrides of the scan's storage-speed
	// classification (SettingsFile.DriveTypeOverrides), which was only editable in Settings.json.
	public partial class MainWindowVM {

		public ObservableCollection<DriveTypeRowVM> DriveTypeRows { get; } = new();

		// Roots typed into the "add" box this session that carry no override yet. They have
		// nothing in the settings file to come back from, so they are remembered here.
		readonly List<string> addedDriveTypeRoots = new();
		int driveTypeRefreshGeneration;
		bool driveTypeIncludesHooked;

		string _NewDriveTypeRoot = string.Empty;
		public string NewDriveTypeRoot {
			get => _NewDriveTypeRoot;
			set => this.RaiseAndSetIfChanged(ref _NewDriveTypeRoot, value);
		}

		/// <summary>Rebuilds the list from the machine's drives, the included folders and the overrides.</summary>
		internal async Task RefreshDriveTypeRowsAsync() {
			if (!driveTypeIncludesHooked) {
				driveTypeIncludesHooked = true;
				// A folder on a new drive or share should show its drive without reopening the page.
				SettingsFile.Instance.Includes.CollectionChanged += (_, _) => {
					if (IsShellSettingsVisible) _ = RefreshDriveTypeRowsAsync();
				};
			}
			int generation = ++driveTypeRefreshGeneration;
			var includes = SettingsFile.Instance.Includes.ToList();
			var keys = SettingsFile.Instance.DriveTypeOverrides.Keys.ToList();
			var added = addedDriveTypeRoots.ToList();
			List<string> roots;
			try {
				// DriveInfo.IsReady can block for seconds on a disconnected network drive.
				roots = await Task.Run(() => {
					var mounts = DriveScanPlanner.SnapshotMountRoots();
					return DriveTypeOverrideRules.ListRoots(ListedDriveRoots(), includes,
						folder => DriveScanPlanner.GetRootKey(folder, mounts), keys, added);
				});
			}
			catch (Exception ex) {
				Logger.Instance.Warn($"Listing drives for the drive type settings failed: {ex.Message}");
				roots = DriveTypeOverrideRules.ListRoots(Array.Empty<string>(), Array.Empty<string>(), _ => null, keys, added);
			}
			if (generation != driveTypeRefreshGeneration) return; // a newer refresh won
			RebuildDriveTypeRows(roots);
		}

		internal void RebuildDriveTypeRows(IEnumerable<string> roots) {
			var names = new[] {
				App.Lang["Settings.DriveTypes.Automatic"],
				App.Lang["Settings.DriveTypes.Fast"],
				App.Lang["Settings.DriveTypes.Slow"],
			};
			DriveTypeRows.Clear();
			foreach (string root in roots)
				DriveTypeRows.Add(new DriveTypeRowVM(root,
					DriveTypeOverrideRules.Get(SettingsFile.Instance.DriveTypeOverrides, root), names, OnDriveTypeChanged));
		}

		void OnDriveTypeChanged(DriveTypeRowVM row) {
			// Applies to the next scan: a running one works from the copy SyncCoreSettings made.
			if (DriveTypeOverrideRules.Set(SettingsFile.Instance.DriveTypeOverrides, row.Root, row.Choice))
				SettingsFile.Instance.NotifyDriveTypeOverridesChanged();
		}

		/// <summary>Ready local, removable and network drives: what a user scans from.</summary>
		static List<string> ListedDriveRoots() {
			var roots = new List<string>();
			foreach (DriveInfo drive in DriveInfo.GetDrives()) {
				try {
					if (drive.DriveType is DriveType.Fixed or DriveType.Removable or DriveType.Network && drive.IsReady)
						roots.Add(drive.RootDirectory.FullName);
				}
				catch { /* a dying drive is simply not listed */ }
			}
			return roots;
		}

		public ReactiveCommand<Unit, Unit> AddDriveTypeRootCommand => ReactiveCommand.CreateFromTask(async () => {
			string root = NewDriveTypeRoot.Trim();
			if (root.Length == 0) return;
			if (!addedDriveTypeRoots.Contains(root, StringComparer.OrdinalIgnoreCase))
				addedDriveTypeRoots.Add(root);
			NewDriveTypeRoot = string.Empty;
			await RefreshDriveTypeRowsAsync();
		});
	}
}
