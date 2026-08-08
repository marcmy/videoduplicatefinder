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
using VDF.Core;
using VDF.Core.FFTools;

namespace VDF.Benchmarks.Scenarios;

/// <summary>
/// Runs the real gray-frame extraction paths against a stable corpus and writes
/// machine-readable JSON. A prior report can be supplied as a baseline; the
/// process exits with code 2 when matched cases regress beyond the allowed limit.
/// </summary>
public static class RegressionProbe {
	sealed class Options {
		public int Iterations { get; set; } = 5;
		public int WarmupIterations { get; set; } = 1;
		public double MaxRegressionPercent { get; set; } = 15d;
		public string OutputPath { get; set; } = Path.Combine("artifacts", "vdf-perf-current.json");
		public string? BaselinePath { get; set; }
		public string? CorpusPath { get; set; }
		public List<double> PositionsSeconds { get; set; } = [1d, 3d, 5d];
		public List<string> Modes { get; set; } = ["process", "native-cpu", "d3d11"];
	}

	sealed class Report {
		public DateTimeOffset TimestampUtc { get; set; }
		public string Commit { get; set; } = string.Empty;
		public string Os { get; set; } = string.Empty;
		public string Framework { get; set; } = string.Empty;
		public string Architecture { get; set; } = string.Empty;
		public int ProcessorCount { get; set; }
		public int Iterations { get; set; }
		public int WarmupIterations { get; set; }
		public List<CaseResult> Cases { get; set; } = [];
	}

	sealed class CaseResult {
		public string Key { get; set; } = string.Empty;
		public string Mode { get; set; } = string.Empty;
		public string File { get; set; } = string.Empty;
		public int SamplesPerIteration { get; set; }
		public int SuccessfulIterations { get; set; }
		public int FailedIterations { get; set; }
		public double MeanBatchMs { get; set; }
		public double P50BatchMs { get; set; }
		public double P95BatchMs { get; set; }
		public double SamplesPerSecond { get; set; }
		public long AllocatedBytes { get; set; }
		public string? LastError { get; set; }
	}

	public static int Run(string[] args) {
		Options options;
		try {
			options = ParseOptions(args.Skip(1).ToArray());
		}
		catch (Exception ex) {
			Console.Error.WriteLine(ex.Message);
			PrintUsage();
			return 1;
		}

		List<string> files;
		try {
			files = ResolveCorpus(options);
		}
		catch (Exception ex) {
			Console.Error.WriteLine(ex.Message);
			return 1;
		}
		if (files.Count == 0) {
			Console.Error.WriteLine("No benchmark media files were available.");
			return 1;
		}

		var report = new Report {
			TimestampUtc = DateTimeOffset.UtcNow,
			Commit = Environment.GetEnvironmentVariable("GITHUB_SHA") ?? ReadGitCommit(),
			Os = RuntimeInformation.OSDescription,
			Framework = RuntimeInformation.FrameworkDescription,
			Architecture = RuntimeInformation.ProcessArchitecture.ToString(),
			ProcessorCount = Environment.ProcessorCount,
			Iterations = options.Iterations,
			WarmupIterations = options.WarmupIterations,
		};

		foreach (string mode in options.Modes) {
			if (!ConfigureMode(mode, out string? skipReason)) {
				Console.WriteLine($"Skipping mode '{mode}': {skipReason}");
				continue;
			}

			foreach (string file in files) {
				CaseResult result = RunCase(mode, file, options);
				report.Cases.Add(result);
				Console.WriteLine(
					$"{result.Key}: mean={result.MeanBatchMs:0.00} ms, " +
					$"p50={result.P50BatchMs:0.00} ms, " +
					$"p95={result.P95BatchMs:0.00} ms, " +
					$"throughput={result.SamplesPerSecond:0.00} samples/s, " +
					$"failures={result.FailedIterations}");
			}
		}

		string? outputDirectory = Path.GetDirectoryName(Path.GetFullPath(options.OutputPath));
		if (!string.IsNullOrEmpty(outputDirectory))
			Directory.CreateDirectory(outputDirectory);
		File.WriteAllText(options.OutputPath, JsonSerializer.Serialize(report, JsonOptions()));
		Console.WriteLine($"Report written to {Path.GetFullPath(options.OutputPath)}");

		HashSet<string> executedModes = report.Cases
			.Select(result => result.Mode)
			.ToHashSet(StringComparer.OrdinalIgnoreCase);
		List<string> missingModes = options.Modes
			.Where(mode => !executedModes.Contains(mode))
			.ToList();
		if (missingModes.Count > 0) {
			Console.Error.WriteLine($"Requested benchmark mode(s) did not run: {string.Join(", ", missingModes)}");
			return 1;
		}

		if (report.Cases.Count == 0 || report.Cases.Any(result =>
				result.SuccessfulIterations != options.Iterations || result.FailedIterations != 0)) {
			Console.Error.WriteLine("One or more benchmark cases did not complete every measured iteration.");
			return 1;
		}
		if (options.BaselinePath == null)
			return 0;
		return CompareBaseline(report, options) ? 0 : 2;
	}

	static CaseResult RunCase(string mode, string file, Options options) {
		for (int i = 0; i < options.WarmupIterations; i++)
			_ = ExtractBatch(file, options.PositionsSeconds);

		var elapsed = new List<double>(options.Iterations);
		long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
		int failed = 0;
		string? lastError = null;

		for (int iteration = 0; iteration < options.Iterations; iteration++) {
			try {
				var stopwatch = Stopwatch.StartNew();
				byte[]?[] frames = ExtractBatch(file, options.PositionsSeconds);
				stopwatch.Stop();
				if (frames.Length != options.PositionsSeconds.Count ||
					frames.Any(frame => frame == null || frame.Length != 1024)) {
					throw new InvalidOperationException(
						$"Expected {options.PositionsSeconds.Count} 32x32 gray frames.");
				}
				elapsed.Add(stopwatch.Elapsed.TotalMilliseconds);
			}
			catch (Exception ex) {
				failed++;
				lastError = $"{ex.GetType().Name}: {ex.Message}";
			}
		}

		long allocatedBytes = Math.Max(0, GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore);
		double totalMs = elapsed.Sum();
		double successfulSamples = elapsed.Count * options.PositionsSeconds.Count;

		return new CaseResult {
			Key = $"{mode}|{Path.GetFileName(file)}",
			Mode = mode,
			File = Path.GetFullPath(file),
			SamplesPerIteration = options.PositionsSeconds.Count,
			SuccessfulIterations = elapsed.Count,
			FailedIterations = failed,
			MeanBatchMs = elapsed.Count == 0 ? 0d : elapsed.Average(),
			P50BatchMs = Percentile(elapsed, 0.50d),
			P95BatchMs = Percentile(elapsed, 0.95d),
			SamplesPerSecond = totalMs <= 0d ? 0d : successfulSamples / (totalMs / 1000d),
			AllocatedBytes = allocatedBytes,
			LastError = lastError,
		};
	}

	static byte[]?[] ExtractBatch(string file, IReadOnlyList<double> positionsSeconds) =>
		FfmpegEngine.GetGrayFrames(file, positionsSeconds, extendedLogging: false);

	static bool ConfigureMode(string mode, out string? skipReason) {
		skipReason = null;
		switch (mode) {
			case "process":
				FfmpegEngine.UseNativeBinding = false;
				FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
				return true;
			case "native-cpu":
				if (!ScanEngine.NativeFFmpegExists) {
					skipReason = "native FFmpeg libraries are unavailable";
					return false;
				}
				FfmpegEngine.UseNativeBinding = true;
				FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
				return true;
			case "d3d11":
				if (!OperatingSystem.IsWindows()) {
					skipReason = "D3D11 is Windows-only";
					return false;
				}
				if (!ScanEngine.NativeFFmpegExists) {
					skipReason = "native FFmpeg libraries are unavailable";
					return false;
				}
				FfmpegEngine.UseNativeBinding = true;
				FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
				return true;
			default:
				skipReason = "unknown mode";
				return false;
		}
	}

	static List<string> ResolveCorpus(Options options) {
		if (!string.IsNullOrWhiteSpace(options.CorpusPath)) {
			if (!Directory.Exists(options.CorpusPath))
				throw new DirectoryNotFoundException(options.CorpusPath);
			var extensions = new HashSet<string>(
				[".mp4", ".mkv", ".avi", ".mov", ".webm", ".wmv", ".m4v"],
				StringComparer.OrdinalIgnoreCase);
			return Directory
				.EnumerateFiles(options.CorpusPath, "*", SearchOption.AllDirectories)
				.Where(path => extensions.Contains(Path.GetExtension(path)))
				.OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
				.ToList();
		}

		if (!VideoCorpus.FfmpegAvailable)
			return [];
		var specs = new[] {
			new VideoCorpus.Spec(VideoCorpus.Codec.H264, 1280, 720, 10),
			new VideoCorpus.Spec(VideoCorpus.Codec.HEVC10, 1280, 720, 10),
			new VideoCorpus.Spec(VideoCorpus.Codec.VP9, 1280, 720, 10),
		};
		return specs.Select(VideoCorpus.Ensure).Where(path => path != null).Cast<string>().ToList();
	}

	static bool CompareBaseline(Report current, Options options) {
		if (!File.Exists(options.BaselinePath))
			throw new FileNotFoundException("Baseline report not found.", options.BaselinePath);
		Report? baseline = JsonSerializer.Deserialize<Report>(
			File.ReadAllText(options.BaselinePath), JsonOptions());
		if (baseline == null)
			throw new InvalidDataException("Baseline report could not be parsed.");

		bool passed = true;
		if (!string.Equals(baseline.Os, current.Os, StringComparison.Ordinal) ||
			!string.Equals(baseline.Framework, current.Framework, StringComparison.Ordinal) ||
			!string.Equals(baseline.Architecture, current.Architecture, StringComparison.Ordinal) ||
			baseline.ProcessorCount != current.ProcessorCount) {
			passed = false;
			Console.Error.WriteLine("REGRESSION GATE INVALID: baseline and current reports were produced on different runtime environments.");
			Console.Error.WriteLine($"  baseline: {baseline.Os}; {baseline.Framework}; {baseline.Architecture}; CPUs={baseline.ProcessorCount}");
			Console.Error.WriteLine($"  current:  {current.Os}; {current.Framework}; {current.Architecture}; CPUs={current.ProcessorCount}");
		}

		if (baseline.Iterations != current.Iterations || baseline.WarmupIterations != current.WarmupIterations) {
			passed = false;
			Console.Error.WriteLine(
				$"REGRESSION GATE INVALID: measurement counts differ " +
				$"(baseline {baseline.Iterations}/{baseline.WarmupIterations}, " +
				$"current {current.Iterations}/{current.WarmupIterations}).");
		}

		var baselineByKey = baseline.Cases.ToDictionary(result => result.Key);
		var currentByKey = current.Cases.ToDictionary(result => result.Key);
		List<string> missingCurrent = baselineByKey.Keys
			.Except(currentByKey.Keys, StringComparer.Ordinal)
			.OrderBy(key => key, StringComparer.Ordinal)
			.ToList();
		foreach (string key in missingCurrent) {
			passed = false;
			Console.Error.WriteLine($"REGRESSION GATE INVALID: baseline case is missing from current run: {key}");
		}

		List<string> currentOnly = currentByKey.Keys
			.Except(baselineByKey.Keys, StringComparer.Ordinal)
			.OrderBy(key => key, StringComparer.Ordinal)
			.ToList();
		foreach (string key in currentOnly)
			Console.WriteLine($"Not baseline-gated yet (new case): {key}");

		int matched = 0;
		foreach (CaseResult result in current.Cases) {
			if (!baselineByKey.TryGetValue(result.Key, out CaseResult? previous))
				continue;
			matched++;

			if (previous.SamplesPerIteration != result.SamplesPerIteration ||
				previous.SamplesPerIteration <= 0) {
				passed = false;
				Console.Error.WriteLine(
					$"REGRESSION GATE INVALID: {result.Key} samples-per-iteration changed " +
					$"from {previous.SamplesPerIteration} to {result.SamplesPerIteration}.");
				continue;
			}
			if (previous.P50BatchMs <= 0d || result.P50BatchMs <= 0d ||
				previous.SamplesPerSecond <= 0d || result.SamplesPerSecond <= 0d) {
				passed = false;
				Console.Error.WriteLine($"REGRESSION GATE INVALID: {result.Key} has non-positive timing/throughput data.");
				continue;
			}

			double baselineMedianThroughput =
				previous.SamplesPerIteration / (previous.P50BatchMs / 1000d);
			double currentMedianThroughput =
				result.SamplesPerIteration / (result.P50BatchMs / 1000d);
			double medianRegression =
				(baselineMedianThroughput - currentMedianThroughput) /
				baselineMedianThroughput * 100d;
			double meanRegression =
				(previous.SamplesPerSecond - result.SamplesPerSecond) /
				previous.SamplesPerSecond * 100d;

			Console.WriteLine(
				$"{result.Key}: median throughput delta={-medianRegression:+0.00;-0.00;0.00}%, " +
				$"mean delta={-meanRegression:+0.00;-0.00;0.00}%");
			if (medianRegression > options.MaxRegressionPercent) {
				passed = false;
				Console.Error.WriteLine(
					$"REGRESSION: {result.Key} median throughput slowed by {medianRegression:0.00}% " +
					$"(allowed {options.MaxRegressionPercent:0.00}%).");
			}
		}

		if (matched == 0) {
			Console.Error.WriteLine("REGRESSION GATE INVALID: baseline and current reports have no matching cases.");
			return false;
		}
		return passed;
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
				case "--iterations": options.Iterations = PositiveInt(Next(), "--iterations"); break;
				case "--warmup": options.WarmupIterations = NonNegativeInt(Next(), "--warmup"); break;
				case "--output": options.OutputPath = Next(); break;
				case "--baseline": options.BaselinePath = Next(); break;
				case "--max-regression-percent":
					options.MaxRegressionPercent = NonNegativeDouble(Next(), "--max-regression-percent");
					break;
				case "--corpus": options.CorpusPath = Next(); break;
				case "--positions": options.PositionsSeconds = CsvDoubles(Next(), "--positions"); break;
				case "--modes":
					options.Modes = Next()
						.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
						.Select(value => value.ToLowerInvariant()).Distinct().ToList();
					if (options.Modes.Count == 0)
						throw new ArgumentException("--modes cannot be empty.");
					break;
				case "--help":
				case "-h": PrintUsage(); Environment.Exit(0); break;
				default: throw new ArgumentException($"Unknown option: {args[i]}");
			}
		}
		return options;
	}

	static int PositiveInt(string value, string option) =>
		int.TryParse(value, out int parsed) && parsed > 0
			? parsed : throw new ArgumentException($"{option} must be a positive integer.");

	static int NonNegativeInt(string value, string option) =>
		int.TryParse(value, out int parsed) && parsed >= 0
			? parsed : throw new ArgumentException($"{option} must be a non-negative integer.");

	static double NonNegativeDouble(string value, string option) =>
		double.TryParse(value, System.Globalization.NumberStyles.Float,
			System.Globalization.CultureInfo.InvariantCulture, out double parsed) && parsed >= 0d
				? parsed : throw new ArgumentException($"{option} must be non-negative.");

	static List<double> CsvDoubles(string value, string option) {
		List<double> parsed = value
			.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
			.Select(item => NonNegativeDouble(item, option)).ToList();
		if (parsed.Count == 0)
			throw new ArgumentException($"{option} cannot be empty.");
		return parsed;
	}

	static double Percentile(List<double> values, double percentile) {
		if (values.Count == 0)
			return 0d;
		double[] sorted = values.OrderBy(value => value).ToArray();
		int index = (int)Math.Ceiling(percentile * sorted.Length) - 1;
		return sorted[Math.Clamp(index, 0, sorted.Length - 1)];
	}

	static string ReadGitCommit() {
		try {
			var startInfo = new ProcessStartInfo("git", "rev-parse --short HEAD") {
				RedirectStandardOutput = true,
				UseShellExecute = false,
				CreateNoWindow = true,
			};
			using Process? process = Process.Start(startInfo);
			if (process == null)
				return "unknown";
			string value = process.StandardOutput.ReadToEnd().Trim();
			process.WaitForExit();
			return process.ExitCode == 0 && value.Length > 0 ? value : "unknown";
		}
		catch {
			return "unknown";
		}
	}

	static JsonSerializerOptions JsonOptions() => new() {
		PropertyNameCaseInsensitive = true,
		WriteIndented = true,
	};

	static void PrintUsage() {
		Console.WriteLine("Usage: dotnet run -c Release --project VDF.Benchmarks -- --probe-regression [options]");
		Console.WriteLine("  --corpus <directory>             Use real media files instead of the synthetic corpus.");
		Console.WriteLine("  --modes <csv>                    process,native-cpu,d3d11 (default: all).");
		Console.WriteLine("  --positions <seconds,csv>        Sample positions (default: 1,3,5).");
		Console.WriteLine("  --iterations <n>                 Measured batches per case (default: 5).");
		Console.WriteLine("  --warmup <n>                     Warmup batches per case (default: 1).");
		Console.WriteLine("  --output <json>                  Current report path.");
		Console.WriteLine("  --baseline <json>                Compare median throughput against a prior report.");
		Console.WriteLine("  --max-regression-percent <n>     Allowed median slowdown before exit code 2 (default: 15).");
	}
}
