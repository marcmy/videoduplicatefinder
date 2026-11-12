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
using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;
using VDF.IntegrationTests.Fixtures;
using Xunit.Abstractions;

namespace VDF.IntegrationTests.FFTools;

/// <summary>
/// Opt-in measurement of what opening a D3D11VA decoder costs per file, in wall time and
/// process CPU time, next to a CPU decoder. Set VDF_HWDEVICE_BENCH_CORPUS to a folder of
/// videos to run it; it is skipped otherwise. Run it on its own (dotnet test --filter) so
/// no other test burns CPU in the same process while it measures.
/// </summary>
[Collection("Ffmpeg")]
public class HardwareDeviceOpenCostBenchmark {
	const string CorpusEnvVar = "VDF_HWDEVICE_BENCH_CORPUS";
	readonly FfmpegFixture _fixture;
	readonly ITestOutputHelper _output;

	public HardwareDeviceOpenCostBenchmark(FfmpegFixture fixture, ITestOutputHelper output) {
		_fixture = fixture;
		_output = output;
	}

	readonly record struct Cost(double WallMs, double CpuMs);

	static Cost Measure(Action action) {
		using Process process = Process.GetCurrentProcess();
		process.Refresh();
		TimeSpan cpuBefore = process.TotalProcessorTime;
		var sw = Stopwatch.StartNew();
		action();
		sw.Stop();
		process.Refresh();
		return new Cost(sw.Elapsed.TotalMilliseconds, (process.TotalProcessorTime - cpuBefore).TotalMilliseconds);
	}

	static void OpenOnly(string file, AVHWDeviceType type) {
		using var decoder = new VideoStreamDecoder(file, type);
	}

	static readonly TimeSpan[] SamplePositions = { TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(40), TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(80) };

	/// <summary>
	/// Open, then seek to and decode five spread-out samples (one seek each), scaling them to
	/// 32x32 on the GPU for D3D11VA — the shape of the native gray-byte and AI sample passes.
	/// </summary>
	static void GrayPass(string file, AVHWDeviceType type) {
		using var decoder = new VideoStreamDecoder(file, type);
		using var scaler = type == AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA ? new D3D11VideoProcessorGrayByteScaler() : null;
		foreach (TimeSpan position in SamplePositions) {
			if (!decoder.TryDecodeFrame(out AVFrame frame, position, out _, scaler != null ? FrameTransferMode.KeepHardwareFrame : FrameTransferMode.TransferHardwareFrame))
				continue;
			if (scaler != null && VideoStreamDecoder.IsHardwareFrame(frame))
				scaler.ScaleToGray32(frame, out _);
		}
	}

	void Report(string label, IReadOnlyList<string> files, AVHWDeviceType type, Action<string, AVHWDeviceType> work, int rounds) {
		foreach (string file in files.Take(2))
			work(file, type); // warm-up: library init, driver, file cache
		var costs = new List<Cost>();
		for (int r = 0; r < rounds; r++)
			foreach (string file in files)
				costs.Add(Measure(() => work(file, type)));
		static double Median(IEnumerable<double> values) {
			double[] sorted = values.OrderBy(v => v).ToArray();
			return sorted[sorted.Length / 2];
		}
		_output.WriteLine($"{label,-28} {type,-28} n={costs.Count,3}  wall mean {costs.Average(c => c.WallMs),6:0.0} ms  median {Median(costs.Select(c => c.WallMs)),6:0.0}  |  CPU mean {costs.Average(c => c.CpuMs),6:0.0} ms  median {Median(costs.Select(c => c.CpuMs)),6:0.0}");
	}

	void ReportParallel(IReadOnlyList<string> files, AVHWDeviceType type, int threads, int rounds) {
		var work = Enumerable.Range(0, rounds).SelectMany(_ => files).ToArray();
		Cost cost = Measure(() => Parallel.ForEach(work, new ParallelOptions { MaxDegreeOfParallelism = threads }, file => GrayPass(file, type)));
		_output.WriteLine($"parallel gray pass x{threads,-2}         {type,-28} n={work.Length,3}  wall total {cost.WallMs,7:0} ms ({cost.WallMs / work.Length,5:0.0}/file)  |  CPU total {cost.CpuMs,7:0} ms ({cost.CpuMs / work.Length,5:0.0}/file)");
	}

	[SkippableFact]
	public void D3D11VA_PerFileOpenCost() {
		Skip.If(!OperatingSystem.IsWindows(), "D3D11VA is Windows-only");
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		string? corpus = Environment.GetEnvironmentVariable(CorpusEnvVar);
		Skip.If(string.IsNullOrEmpty(corpus) || !Directory.Exists(corpus), $"Set {CorpusEnvVar} to a folder of videos to run this benchmark");

		string[] files = Directory.GetFiles(corpus!).Where(f => f.EndsWith(".mp4") || f.EndsWith(".mkv")).OrderBy(f => f).ToArray();
		Skip.If(files.Length == 0, "No .mp4/.mkv files in the corpus folder");
		int rounds = int.TryParse(Environment.GetEnvironmentVariable("VDF_HWDEVICE_BENCH_ROUNDS"), out int r) ? r : 3;
		_output.WriteLine($"{files.Length} files, {rounds} round(s), VDF_DISABLE_SHARED_HWDEVICE={Environment.GetEnvironmentVariable("VDF_DISABLE_SHARED_HWDEVICE") ?? "(unset)"}");

		Report("open+dispose", files, AVHWDeviceType.AV_HWDEVICE_TYPE_NONE, OpenOnly, rounds);
		Report("open+dispose", files, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA, OpenOnly, rounds);
		Report("gray pass (5 samples)", files, AVHWDeviceType.AV_HWDEVICE_TYPE_NONE, GrayPass, rounds);
		Report("gray pass (5 samples)", files, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA, GrayPass, rounds);
		foreach (int threads in new[] { 4, 8 }) {
			ReportParallel(files, AVHWDeviceType.AV_HWDEVICE_TYPE_NONE, threads, rounds);
			ReportParallel(files, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA, threads, rounds);
		}
	}
}
