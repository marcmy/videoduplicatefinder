// /*
//     Copyright (C) 2026 0x90d
//     This file is part of VideoDuplicateFinder
//     VideoDuplicateFinder is free software: you can redistribute it and/or modify
//     it under the terms of the GNU Affero General Public License as published by
//     the Free Software Foundation, either version 3 of the License, or
//     (at your option) any later version.
// */
//

using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using VDF.Core;
using VDF.Core.FFTools;
using VDF.Core.Utils;

namespace VDF.Benchmarks.Scenarios;

/// <summary>
/// Runs a real ScanEngine search+compare against either a supplied media directory or a
/// deterministic synthetic corpus. It measures the major wall-clock pipeline boundaries
/// and aggregates the perf branch's existing FFmpeg timing telemetry. Unlike the narrow
/// extraction regression probe, this includes discovery, database work, hashing/sampling,
/// comparison, grouping and finalization.
/// </summary>
public static partial class ScanPipelineProbe {
	sealed class Options {
		public string? CorpusPath { get; set; }
		public string Mode { get; set; } = "native-cpu";
		public string OutputPath { get; set; } = Path.Combine("artifacts", "vdf-scan-pipeline.json");
		public int CopiesPerCodec { get; set; } = 4;
		public int ThumbnailCount { get; set; } = 3;
		public int MediaParallelism { get; set; } = Math.Max(1, Environment.ProcessorCount - 2);
		public int MatchingParallelism { get; set; }
		public int TimeoutMinutes { get; set; } = 20;
		public bool UsePHash { get; set; }
		public bool EnablePartialClipDetection { get; set; }
		public bool KeepWorkDir { get; set; }
	}

	sealed class Report {
		public DateTimeOffset TimestampUtc { get; set; }
		public string Commit { get; set; } = string.Empty;
		public string Os { get; set; } = string.Empty;
		public string Framework { get; set; } = string.Empty;
		public string Architecture { get; set; } = string.Empty;
		public int ProcessorCount { get; set; }
		public string Mode { get; set; } = string.Empty;
		public string CorpusPath { get; set; } = string.Empty;
		public bool SyntheticCorpus { get; set; }
		public int InputFiles { get; set; }
		public int DatabaseEntries { get; set; }
		public int DuplicateItems { get; set; }
		public int DuplicateGroups { get; set; }
		public string Status { get; set; } = string.Empty;
		public double TotalWallMs { get; set; }
		public double FileDiscoveryWallMs { get; set; }
		public double AnalysisHashingWallMs { get; set; }
		public double CompareFinalizeWallMs { get; set; }
		public double ProcessCpuMs { get; set; }
		public long AllocatedBytes { get; set; }
		public List<ObservedStage> ObservedStages { get; set; } = [];
		public Dictionary<string, TimingAggregate> ExtractionTelemetry { get; set; } = new(StringComparer.Ordinal);
	}

	public sealed class ObservedStage {
		public string Stage { get; set; } = string.Empty;
		public int Events { get; set; }
		public double FirstSeenMs { get; set; }
		public double LastSeenMs { get; set; }
		public int MaxStageCurrent { get; set; }
		public int MaxStageMax { get; set; }
	}

	public sealed class TimingAggregate {
		public int Count { get; set; }
		public Dictionary<string, long> SumMs { get; set; } = new(StringComparer.Ordinal);
		public Dictionary<string, long> MaxMs { get; set; } = new(StringComparer.Ordinal);
	}

	sealed class ProbeObserver : IDisposable {
		readonly long startTimestamp;
		readonly object gate = new();
		readonly Dictionary<string, ObservedStage> stages = new(StringComparer.Ordinal);
		readonly Dictionary<string, TimingAggregate> extraction = new(StringComparer.Ordinal);
		readonly ScanEngine engine;
		long filesEnumeratedTimestamp;
		long hashesDoneTimestamp;
		long doneTimestamp;
		bool aborted;

		public ProbeObserver(ScanEngine engine) {
			this.engine = engine;
			startTimestamp = Stopwatch.GetTimestamp();
			engine.FilesEnumerated += OnFilesEnumerated;
			engine.BuildingHashesDone += OnHashesDone;
			engine.ScanDone += OnDone;
			engine.ScanAborted += OnAborted;
			engine.Progress += OnProgress;
			Logger.Instance.LogEntryAdded += OnLog;
		}

		public long StartTimestamp => startTimestamp;
		public long FilesEnumeratedTimestamp => Volatile.Read(ref filesEnumeratedTimestamp);
		public long HashesDoneTimestamp => Volatile.Read(ref hashesDoneTimestamp);
		public long DoneTimestamp => Volatile.Read(ref doneTimestamp);
		public bool Aborted => aborted;

		public List<ObservedStage> SnapshotStages() {
			lock (gate)
				return stages.Values.OrderBy(s => s.FirstSeenMs).Select(CloneStage).ToList();
		}

		public Dictionary<string, TimingAggregate> SnapshotExtraction() {
			lock (gate) {
				return extraction.ToDictionary(
					pair => pair.Key,
					pair => new TimingAggregate {
						Count = pair.Value.Count,
						SumMs = new Dictionary<string, long>(pair.Value.SumMs, StringComparer.Ordinal),
						MaxMs = new Dictionary<string, long>(pair.Value.MaxMs, StringComparer.Ordinal),
					},
					StringComparer.Ordinal);
			}
		}

		void OnFilesEnumerated(object? sender, EventArgs e) =>
			Interlocked.CompareExchange(ref filesEnumeratedTimestamp, Stopwatch.GetTimestamp(), 0);

		void OnHashesDone(object? sender, EventArgs e) =>
			Interlocked.CompareExchange(ref hashesDoneTimestamp, Stopwatch.GetTimestamp(), 0);

		void OnDone(object? sender, EventArgs e) =>
			Interlocked.CompareExchange(ref doneTimestamp, Stopwatch.GetTimestamp(), 0);

		void OnAborted(object? sender, EventArgs e) {
			aborted = true;
			Interlocked.CompareExchange(ref doneTimestamp, Stopwatch.GetTimestamp(), 0);
		}

		void OnProgress(object? sender, ScanProgressChangedEventArgs e) {
			if (string.IsNullOrWhiteSpace(e.CurrentStage))
				return;
			double nowMs = ElapsedMs(startTimestamp, Stopwatch.GetTimestamp());
			lock (gate) {
				if (!stages.TryGetValue(e.CurrentStage, out ObservedStage? stage)) {
					stage = new ObservedStage { Stage = e.CurrentStage, FirstSeenMs = nowMs };
					stages.Add(stage.Stage, stage);
				}
				stage.Events++;
				stage.LastSeenMs = nowMs;
				stage.MaxStageCurrent = Math.Max(stage.MaxStageCurrent, e.StageCurrent);
				stage.MaxStageMax = Math.Max(stage.MaxStageMax, e.StageMax);
			}
		}

		void OnLog(LogEntry entry) {
			string? kind = entry.Message switch {
				var text when text.StartsWith("Native FFmpeg batched graybyte extraction completed", StringComparison.Ordinal) => "native-batch",
				var text when text.StartsWith("FFmpeg process graybyte extraction completed", StringComparison.Ordinal) => "process-batch",
				var text when text.StartsWith("Native FFmpeg timing on", StringComparison.Ordinal) => "native-single",
				var text when text.StartsWith("FFmpeg process timing on", StringComparison.Ordinal) => "process-single",
				_ => null,
			};
			if (kind == null)
				return;

			MatchCollection values = TimingFieldRegex().Matches(entry.Message);
			lock (gate) {
				if (!extraction.TryGetValue(kind, out TimingAggregate? aggregate)) {
					aggregate = new TimingAggregate();
					extraction.Add(kind, aggregate);
				}
				aggregate.Count++;
				foreach (Match match in values) {
					string name = match.Groups["name"].Value;
					if (!long.TryParse(match.Groups["value"].Value, out long ms))
						continue;
					aggregate.SumMs[name] = aggregate.SumMs.GetValueOrDefault(name) + ms;
					aggregate.MaxMs[name] = Math.Max(aggregate.MaxMs.GetValueOrDefault(name), ms);
				}
			}
		}

		static ObservedStage CloneStage(ObservedStage stage) => new() {
			Stage = stage.Stage,
			Events = stage.Events,
			FirstSeenMs = stage.FirstSeenMs,
			LastSeenMs = stage.LastSeenMs,
			MaxStageCurrent = stage.MaxStageCurrent,
			MaxStageMax = stage.MaxStageMax,
		};

		public void Dispose() {
			engine.FilesEnumerated -= OnFilesEnumerated;
			engine.BuildingHashesDone -= OnHashesDone;
			engine.ScanDone -= OnDone;
			engine.ScanAborted -= OnAborted;
			engine.Progress -= OnProgress;
			Logger.Instance.LogEntryAdded -= OnLog;
		}
	}

	[GeneratedRegex(@"(?<name>[A-Za-z][A-Za-z0-9]*)=(?<value>[0-9]+)ms", RegexOptions.CultureInvariant)]
	private static partial Regex TimingFieldRegex();

	public static int Run(string[] args) {
		try {
			return RunAsync(ParseOptions(args.Skip(1).ToArray())).GetAwaiter().GetResult();
		}
		catch (Exception ex) {
			Console.Error.WriteLine(ex);
			return 1;
		}
	}

	static async Task<int> RunAsync(Options options) {
		(string corpus, bool ownsCorpus) = ResolveCorpus(options);
		string workDir = Path.Combine(Path.GetTempPath(), $"vdf_scan_probe_{Guid.NewGuid():N}");
		string databaseDir = Path.Combine(workDir, "database");
		Directory.CreateDirectory(databaseDir);

		try {
			if (!ConfigureMode(options.Mode, out string? modeError))
				throw new InvalidOperationException(modeError);
			if (!ScanEngine.FFprobeExists)
				throw new InvalidOperationException("FFprobe is unavailable; the full scan pipeline cannot run.");

			var settings = new Settings {
				IncludeList = new HashSet<string> { Path.GetFullPath(corpus) },
				IncludeSubDirectories = true,
				IncludeImages = false,
				GeneratePreviewThumbnails = false,
				UseNativeFfmpegBinding = !options.Mode.Equals("process", StringComparison.OrdinalIgnoreCase),
				HardwareAccelerationMode = options.Mode.Equals("d3d11", StringComparison.OrdinalIgnoreCase)
					? FFHardwareAccelerationMode.d3d11va
					: FFHardwareAccelerationMode.none,
				ExtendedFFToolsLogging = true,
				ThumbnailCount = options.ThumbnailCount,
				MaxDegreeOfParallelism = options.MediaParallelism,
				HddMaxDegreeOfParallelism = 2,
				MatchingMaxDegreeOfParallelism = options.MatchingParallelism,
				UsePHashing = options.UsePHash,
				EnablePartialClipDetection = options.EnablePartialClipDetection,
				DatabaseCheckpointIntervalMinutes = 0,
				CustomDatabaseFolder = databaseDir,
				Percent = 92f,
				PercentDurationDifference = 20d,
				LanguageCode = "en",
			};

			var engine = new ScanEngine { Settings = settings };
			var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			engine.ScanDone += (_, _) => completion.TrySetResult(true);
			engine.ScanAborted += (_, _) => completion.TrySetResult(false);

			using var observer = new ProbeObserver(engine);
			TimeSpan cpuBefore = Process.GetCurrentProcess().TotalProcessorTime;
			long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);

			engine.StartSearch(searchAndCompare: true);
			bool completed = await completion.Task.WaitAsync(TimeSpan.FromMinutes(options.TimeoutMinutes));

			TimeSpan cpuAfter = Process.GetCurrentProcess().TotalProcessorTime;
			long allocatedAfter = GC.GetTotalAllocatedBytes(precise: true);
			long endTimestamp = observer.DoneTimestamp != 0 ? observer.DoneTimestamp : Stopwatch.GetTimestamp();
			long filesTimestamp = observer.FilesEnumeratedTimestamp;
			long hashesTimestamp = observer.HashesDoneTimestamp;

			var report = new Report {
				TimestampUtc = DateTimeOffset.UtcNow,
				Commit = Environment.GetEnvironmentVariable("GITHUB_SHA") ?? ReadGitCommit(),
				Os = RuntimeInformation.OSDescription,
				Framework = RuntimeInformation.FrameworkDescription,
				Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
				ProcessorCount = Environment.ProcessorCount,
				Mode = options.Mode,
				CorpusPath = Path.GetFullPath(corpus),
				SyntheticCorpus = ownsCorpus,
				InputFiles = CountMediaFiles(corpus),
				DatabaseEntries = ScanEngine.DatabaseEntryCount,
				DuplicateItems = engine.Duplicates.Count,
				DuplicateGroups = engine.Duplicates.Select(item => item.GroupId).Distinct().Count(),
				Status = completed && !observer.Aborted ? "completed" : "aborted",
				TotalWallMs = ElapsedMs(observer.StartTimestamp, endTimestamp),
				FileDiscoveryWallMs = filesTimestamp == 0 ? 0d : ElapsedMs(observer.StartTimestamp, filesTimestamp),
				AnalysisHashingWallMs = filesTimestamp == 0 || hashesTimestamp == 0 ? 0d : ElapsedMs(filesTimestamp, hashesTimestamp),
				CompareFinalizeWallMs = hashesTimestamp == 0 ? 0d : ElapsedMs(hashesTimestamp, endTimestamp),
				ProcessCpuMs = (cpuAfter - cpuBefore).TotalMilliseconds,
				AllocatedBytes = Math.Max(0, allocatedAfter - allocatedBefore),
				ObservedStages = observer.SnapshotStages(),
				ExtractionTelemetry = observer.SnapshotExtraction(),
			};

			string fullOutput = Path.GetFullPath(options.OutputPath);
			Directory.CreateDirectory(Path.GetDirectoryName(fullOutput)!);
			File.WriteAllText(fullOutput, JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));

			Console.WriteLine($"Scan pipeline ({report.Mode}): total={report.TotalWallMs:0}ms, discovery={report.FileDiscoveryWallMs:0}ms, analysis={report.AnalysisHashingWallMs:0}ms, compare/finalize={report.CompareFinalizeWallMs:0}ms");
			Console.WriteLine($"Database={report.DatabaseEntries} files, duplicate items={report.DuplicateItems}, groups={report.DuplicateGroups}, CPU={report.ProcessCpuMs:0}ms, allocations={report.AllocatedBytes:N0} bytes");
			Console.WriteLine($"Report written to {fullOutput}");
			return report.Status == "completed" ? 0 : 2;
		}
		finally {
			if (!options.KeepWorkDir) {
				TryDelete(workDir);
				if (ownsCorpus)
					TryDelete(corpus);
			}
		}
	}

	static (string Path, bool Owns) ResolveCorpus(Options options) {
		if (!string.IsNullOrWhiteSpace(options.CorpusPath)) {
			string path = Path.GetFullPath(options.CorpusPath);
			if (!Directory.Exists(path))
				throw new DirectoryNotFoundException(path);
			return (path, false);
		}

		if (!VideoCorpus.FfmpegAvailable)
			throw new InvalidOperationException("FFmpeg CLI is unavailable; cannot generate the synthetic scan corpus.");
		string root = Path.Combine(Path.GetTempPath(), $"vdf_scan_probe_media_{Guid.NewGuid():N}");
		Directory.CreateDirectory(root);
		var specs = new[] {
			new VideoCorpus.Spec(VideoCorpus.Codec.H264, 1280, 720, 10),
			new VideoCorpus.Spec(VideoCorpus.Codec.HEVC10, 1280, 720, 10),
			new VideoCorpus.Spec(VideoCorpus.Codec.VP9, 1280, 720, 10),
		};
		int written = 0;
		foreach (VideoCorpus.Spec spec in specs) {
			string? source = VideoCorpus.Ensure(spec);
			if (source == null)
				continue;
			for (int i = 0; i < options.CopiesPerCodec; i++) {
				string destination = Path.Combine(root, $"{Path.GetFileNameWithoutExtension(source)}_copy{i + 1}{Path.GetExtension(source)}");
				File.Copy(source, destination, overwrite: true);
				written++;
			}
		}
		if (written < 2) {
			TryDelete(root);
			throw new InvalidOperationException("Synthetic corpus generation produced fewer than two media files.");
		}
		return (root, true);
	}

	static bool ConfigureMode(string mode, out string? error) {
		error = null;
		switch (mode.ToLowerInvariant()) {
			case "process":
				if (!ScanEngine.FFmpegExists) {
					error = "FFmpeg CLI is unavailable.";
					return false;
				}
				FfmpegEngine.UseNativeBinding = false;
				FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
				return true;
			case "native-cpu":
				if (!ScanEngine.NativeFFmpegExists) {
					error = "Native FFmpeg libraries are unavailable.";
					return false;
				}
				FfmpegEngine.UseNativeBinding = true;
				FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
				return true;
			case "d3d11":
				if (!OperatingSystem.IsWindows()) {
					error = "D3D11 profiling is Windows-only.";
					return false;
				}
				if (!ScanEngine.NativeFFmpegExists) {
					error = "Native FFmpeg libraries are unavailable.";
					return false;
				}
				FfmpegEngine.UseNativeBinding = true;
				FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
				return true;
			default:
				error = $"Unknown mode '{mode}'. Use process, native-cpu, or d3d11.";
				return false;
		}
	}

	static int CountMediaFiles(string root) {
		var extensions = new HashSet<string>([".mp4", ".mkv", ".avi", ".mov", ".webm", ".wmv", ".m4v"], StringComparer.OrdinalIgnoreCase);
		return Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories).Count(path => extensions.Contains(Path.GetExtension(path)));
	}

	static Options ParseOptions(string[] args) {
		var options = new Options();
		for (int i = 0; i < args.Length; i++) {
			string Next() {
				if (++i >= args.Length)
					throw new ArgumentException($"Missing value after {args[i - 1]}.");
				return args[i];
			}
			switch (args[i]) {
				case "--corpus": options.CorpusPath = Next(); break;
				case "--mode": options.Mode = Next().ToLowerInvariant(); break;
				case "--output": options.OutputPath = Next(); break;
				case "--copies-per-codec": options.CopiesPerCodec = PositiveInt(Next(), args[i - 1]); break;
				case "--thumbnails": options.ThumbnailCount = PositiveInt(Next(), args[i - 1]); break;
				case "--media-parallelism": options.MediaParallelism = PositiveInt(Next(), args[i - 1]); break;
				case "--matching-parallelism": options.MatchingParallelism = NonNegativeInt(Next(), args[i - 1]); break;
				case "--timeout-minutes": options.TimeoutMinutes = PositiveInt(Next(), args[i - 1]); break;
				case "--phash": options.UsePHash = true; break;
				case "--partial": options.EnablePartialClipDetection = true; break;
				case "--keep-workdir": options.KeepWorkDir = true; break;
				case "--help":
				case "-h": PrintUsage(); Environment.Exit(0); break;
				default: throw new ArgumentException($"Unknown option: {args[i]}");
			}
		}
		return options;
	}

	static int PositiveInt(string value, string option) =>
		int.TryParse(value, out int parsed) && parsed > 0 ? parsed : throw new ArgumentException($"{option} must be positive.");
	static int NonNegativeInt(string value, string option) =>
		int.TryParse(value, out int parsed) && parsed >= 0 ? parsed : throw new ArgumentException($"{option} must be non-negative.");
	static double ElapsedMs(long start, long end) => (end - start) * 1000d / Stopwatch.Frequency;

	static string ReadGitCommit() {
		try {
			var startInfo = new ProcessStartInfo("git", "rev-parse --short HEAD") {
				RedirectStandardOutput = true,
				UseShellExecute = false,
				CreateNoWindow = true,
			};
			using Process? process = Process.Start(startInfo);
			if (process == null) return "unknown";
			string value = process.StandardOutput.ReadToEnd().Trim();
			process.WaitForExit();
			return process.ExitCode == 0 && value.Length > 0 ? value : "unknown";
		}
		catch { return "unknown"; }
	}

	static void TryDelete(string path) {
		try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
		catch { }
	}

	static void PrintUsage() {
		Console.WriteLine("Usage: dotnet run -c Release --project VDF.Benchmarks -- --probe-scan-pipeline [options]");
		Console.WriteLine("  --corpus <directory>          Scan a real media directory (default: generated synthetic corpus).");
		Console.WriteLine("  --mode <mode>                 process, native-cpu, or d3d11 (default: native-cpu).");
		Console.WriteLine("  --output <json>               Output report path.");
		Console.WriteLine("  --copies-per-codec <n>        Synthetic duplicates per available codec (default: 4).");
		Console.WriteLine("  --thumbnails <n>              Sample positions per video (default: 3).");
		Console.WriteLine("  --media-parallelism <n>       Media-read concurrency.");
		Console.WriteLine("  --matching-parallelism <n>    Matching worker cap, 0 = automatic.");
		Console.WriteLine("  --phash                       Use pHash matching in the full scan.");
		Console.WriteLine("  --partial                     Enable audio partial-clip detection.");
		Console.WriteLine("  --timeout-minutes <n>         Abort the probe wait after N minutes (default: 20).");
		Console.WriteLine("  --keep-workdir                Keep temporary database/synthetic corpus for inspection.");
	}
}
