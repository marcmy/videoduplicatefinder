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

using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Runtime.InteropServices;
using System.Text;
using Avalonia.Input.Platform;
using Avalonia.Platform.Storage;
using ReactiveUI;
using VDF.Core.FFTools;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;
using VDF.GUI.Data;
using VDF.GUI.Views;

namespace VDF.GUI.ViewModels {
	// Settings -> Test: environment/diagnostics helpers for bug reports.
	public partial class MainWindowVM : ReactiveObject {

		// Builds a copy-pasteable environment report: VDF/OS/ffmpeg versions, native-binding
		// health, and the settings that actually affect matching. This is the single most
		// useful artifact to attach to an issue.
		string BuildDiagnosticsReport() {
			SyncCoreSettings(); // make sure the report reflects the current (unsaved) settings
			var sb = new StringBuilder();
			sb.AppendLine($"VDF version:        {Utils.VersionInfo.LongDisplay}");
			sb.AppendLine($"OS:                 {RuntimeInformation.OSDescription} ({RuntimeInformation.OSArchitecture})");
			sb.AppendLine($".NET:               {RuntimeInformation.FrameworkDescription}");
			sb.AppendLine($"ffmpeg path:        {FfmpegEngine.FFmpegPath}");
			sb.AppendLine($"ffmpeg:             {FFToolsUtils.GetToolVersionLine(FFToolsUtils.FFTool.FFmpeg)}");
			sb.AppendLine($"ffprobe:            {FFToolsUtils.GetToolVersionLine(FFToolsUtils.FFTool.FFProbe)}");
			sb.AppendLine($"Native binding:     setting={Scanner.Settings.UseNativeFfmpegBinding}, libsPresent={FFmpegHelper.DoFFmpegLibraryFilesExist}, libsLoadable={FFmpegHelper.CanLoadNativeLibraries}");
			sb.AppendLine($"Expected libs:      {string.Join(", ", FFmpegHelper.GenerateLibraryFileNames())}");
			sb.AppendLine($"Hardware accel:     {Scanner.Settings.HardwareAccelerationMode}");
			sb.AppendLine($"Percent:            {Scanner.Settings.Percent}");
			sb.AppendLine($"Use pHash:          {Scanner.Settings.UsePHashing}");
			sb.AppendLine($"AI matching:        setting={Scanner.Settings.UseAiMatching} (threshold {Scanner.Settings.AiPercent}%), partial={Scanner.Settings.EnableAiPartialDetection} (hit {Scanner.Settings.AiPartialHitPercent}%), components={VDF.Core.AI.AiComponents.GetState()} (runtime {VDF.Core.AI.AiComponents.RuntimeDescription})");
			sb.AppendLine($"Compare flipped:    {Scanner.Settings.CompareHorizontallyFlipped}");
			sb.AppendLine($"Ignore black/white: {Scanner.Settings.IgnoreBlackPixels}/{Scanner.Settings.IgnoreWhitePixels}");
			sb.AppendLine($"Thumbnails:         {Scanner.Settings.ThumbnailCount}");
			sb.AppendLine($"Include images:     {Scanner.Settings.IncludeImages}");
			sb.AppendLine($"Parallelism:        {Scanner.Settings.MaxDegreeOfParallelism} (HDD cap: {Scanner.Settings.HddMaxDegreeOfParallelism}, drive overrides: {Scanner.Settings.DriveTypeOverrides.Count})");
			sb.AppendLine($"Custom FF args:     {(string.IsNullOrWhiteSpace(Scanner.Settings.CustomFFArguments) ? "(none)" : Scanner.Settings.CustomFFArguments)}");
			sb.AppendLine($"Database entries:   {DatabaseUtils.Database.Count}");
			sb.AppendLine($"Process memory:     {CoreUtils.DescribeProcessMemory()}");
			sb.AppendLine($"Data folder:        {CoreUtils.StateFolder} ({CoreUtils.DataFolderInfo.Describe()})");
			if (!string.Equals(CoreUtils.StateFolder, CoreUtils.SettingsFolder, StringComparison.Ordinal))
				sb.AppendLine($"Settings folder:    {CoreUtils.SettingsFolder}");
			return sb.ToString();
		}

		// Where databases, settings, logs and AI components live (see VDF.Core.Utils.DataFolder).
		public string DataFolderPath => CoreUtils.StateFolder;
		public string DataFolderSourceText => string.Format(App.Lang["MainWindow.Settings.Diagnostics.DataFolder.Source"], CoreUtils.DataFolderInfo.Describe());

		public ReactiveCommand<Unit, Unit> OpenDataFolderCommand => ReactiveCommand.Create(() => {
			try {
				Process.Start(new ProcessStartInfo {
					FileName = CoreUtils.StateFolder,
					UseShellExecute = true,
				});
			}
			catch (Exception ex) {
				Logger.Instance.Warn($"Could not open the data folder {CoreUtils.StateFolder}: {ex.Message}");
			}
		});

		// A redactor primed with the folders the user scans, so "D:\Private" is hidden even
		// when no file inside it shows up in the text. One instance per shared artifact.
		static LogRedactor CreateSharingRedactor() => new(LogRedactorOptions.ForCurrentUser(
			SettingsFile.Instance.Includes.Concat(SettingsFile.Instance.Blacklists)));

		public ReactiveCommand<Unit, Unit> CopyDiagnosticsCommand => ReactiveCommand.Create(() => {
			string report = BuildDiagnosticsReport();
			if (SettingsFile.Instance.RedactSharedDiagnostics)
				report = CreateSharingRedactor().Redact(report);
			FilePairTestResult = report; // show it too, so the user can see what's being shared
			ApplicationHelpers.MainWindow.Clipboard?.SetTextAsync(report);
		});

		// Writes a copy of log.txt for an issue or a forum post. Redaction happens on the way
		// out so the user's own log keeps real paths for their own troubleshooting.
		public ReactiveCommand<Unit, Unit> ExportLogCommand => ReactiveCommand.CreateFromTask(async () => {
			bool redact = SettingsFile.Instance.RedactSharedDiagnostics;
			var result = await Utils.PickerDialogUtils.SaveFilePicker(new FilePickerSaveOptions {
				Title = App.Lang["MainWindow.Settings.Diagnostics.ExportLog"],
				SuggestedFileName = redact ? "vdf-log-redacted.txt" : "vdf-log.txt",
				DefaultExtension = ".txt"
			});
			if (string.IsNullOrEmpty(result))
				return;
			string message;
			try {
				string text = File.Exists(Logger.LogFilePath) ? LogRedactor.ReadShared(Logger.LogFilePath) : string.Empty;
				if (redact) {
					var redactor = CreateSharingRedactor();
					text = redactor.Redact(text);
					message = string.Format(App.Lang["MainWindow.Settings.Diagnostics.ExportLog.DoneRedacted"], result, redactor.RedactedFileCount, redactor.RedactedFolderCount);
				}
				else {
					message = string.Format(App.Lang["MainWindow.Settings.Diagnostics.ExportLog.Done"], result);
				}
				if (string.Equals(Path.GetFullPath(result), Path.GetFullPath(Logger.LogFilePath), StringComparison.OrdinalIgnoreCase))
					throw new IOException("The export must not overwrite log.txt.");
				await File.WriteAllTextAsync(result, text, new UTF8Encoding(false));
			}
			catch (Exception ex) {
				Logger.Instance.Error($"Could not export the log: {ex.Message}");
				message = App.Lang["MainWindow.Settings.Diagnostics.ExportLog.Failed"];
			}
			await MessageBoxService.Show(message);
		});

		public ReactiveCommand<Unit, Unit> ExportGrayBytesCommand => ReactiveCommand.CreateFromTask(async () => {
			var result = await Utils.PickerDialogUtils.SaveFilePicker(new FilePickerSaveOptions {
				Title = App.Lang["MainWindow.Settings.Diagnostics.ExportGrayBytes"],
				SuggestedFileName = "vdf-graybytes-diagnostic.json",
				DefaultExtension = ".json"
			});
			if (string.IsNullOrEmpty(result))
				return;
			bool ok = DatabaseUtils.ExportGrayBytesDiagnostic(result);
			await MessageBoxService.Show(ok
				? string.Format(App.Lang["MainWindow.Settings.Diagnostics.ExportGrayBytes.Done"], result)
				: App.Lang["MainWindow.Settings.Diagnostics.ExportGrayBytes.Failed"]);
		// Locked during a scan: it walks the database the scan's workers are writing to.
		}, IsNotScanning);
	}
}
