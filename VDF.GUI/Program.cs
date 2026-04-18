// /*
//     Copyright (C) 2025 0x90d
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

global using System;
global using System.Collections.Generic;
global using System.IO;
global using System.Threading.Tasks;
using System.CommandLine;
using System.Linq;
using System.Text;
using Avalonia;
using ReactiveUI.Avalonia;
using VDF.GUI.Utils;

namespace VDF.GUI {
	class Program {
		static readonly string CrashLogPath = Path.Combine(
			Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
			"VideoDuplicateFinder",
			"unhandled-crash.log");

		static void WriteCrashLog(string source, Exception? ex, bool terminating, object? extra = null) {
			try {
				Directory.CreateDirectory(Path.GetDirectoryName(CrashLogPath)!);
				var sb = new StringBuilder();
				sb.AppendLine(new string('=', 80));
				sb.AppendLine($"UTC: {DateTime.UtcNow:O}");
				sb.AppendLine($"Source: {source}");
				sb.AppendLine($"Terminating: {terminating}");
				sb.AppendLine($"Process: {Environment.ProcessPath}");
				sb.AppendLine($"OS: {Environment.OSVersion}");
				sb.AppendLine($".NET: {Environment.Version}");
				if (extra != null) {
					sb.AppendLine("Extra:");
					sb.AppendLine(extra.ToString());
				}
				if (ex != null) {
					sb.AppendLine("Exception:");
					sb.AppendLine(ex.ToString());
				}
				File.AppendAllText(CrashLogPath, sb.ToString(), Encoding.UTF8);
			}
			catch { }
		}

		static void RegisterUnhandledExceptionLogging() {
			AppDomain.CurrentDomain.UnhandledException += (_, e) => {
				WriteCrashLog(
					source: "AppDomain.CurrentDomain.UnhandledException",
					ex: e.ExceptionObject as Exception,
					terminating: e.IsTerminating,
					extra: e.ExceptionObject);
			};

			TaskScheduler.UnobservedTaskException += (_, e) => {
				WriteCrashLog(
					source: "TaskScheduler.UnobservedTaskException",
					ex: e.Exception,
					terminating: false);
			};
		}

		// Initialization code. Don't use any Avalonia, third-party APIs or any
		// SynchronizationContext-reliant code before AppMain is called: things aren't initialized
		// yet and stuff might break.
		[STAThread]
		public static int Main(string[] args) {
			RegisterUnhandledExceptionLogging();

			Option<FileInfo> settingsOption = new("--settings", new[] { "-s" }) {
				Description = "Path to a settings file to load and save."
			};
			RootCommand rootCommand = new("VideoDuplicateFinder settings options");
			rootCommand.Options.Add(settingsOption);

			// This runs ONLY when parsing succeeded and no built-in action (like --help) took over
			rootCommand.SetAction(parseResult =>
			{
				if (parseResult.GetValue(settingsOption) is FileInfo parsedFile) {
					if (parsedFile.Exists) {
						Data.SettingsFile.SetSettingsPath(parsedFile.FullName);
						Console.Out.WriteLine($"Using custom settings file: '{parsedFile.FullName}'");
					}
					else {
						ConsoleAttach.EnsureConsole();
						Console.Error.WriteLine($"Settings file not found: '{parsedFile.FullName}'. Using default settings file.");
					}
				}

				try {
					BuildAvaloniaApp().StartWithClassicDesktopLifetime(args);
				}
				catch (Exception ex) {
					WriteCrashLog("Program.Main.StartWithClassicDesktopLifetime", ex, terminating: true);
					throw;
				}
			});
			var parseResult = rootCommand.Parse(args);
			// If help requested OR parse errors -> we want console output
			if (parseResult.Errors.Count > 0 || args.Contains("-h") || args.Contains("--help") || args.Contains("-?")) {
				ConsoleAttach.EnsureConsole();
			}
			try {
				return rootCommand.Parse(args).Invoke();
			}
			catch (Exception ex) {
				WriteCrashLog("Program.Main.Invoke", ex, terminating: true);
				throw;
			}
		}

		// Avalonia configuration, don't remove; also used by visual designer.
		public static AppBuilder BuildAvaloniaApp()
			=> AppBuilder.Configure<App>()
				.UsePlatformDetect()
				.With(new X11PlatformOptions {  UseDBusFilePicker = false })
				.UseReactiveUI()
				.RegisterReactiveUIViewsFromEntryAssembly();
	}
}
