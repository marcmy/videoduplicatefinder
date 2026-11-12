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
using System.Runtime.InteropServices;
using FFmpeg.AutoGen;
using VDF.Core.AI;
using VDF.Core.FFTools;
using VDF.Core.FFTools.FFmpegNative;
using VDF.IntegrationTests.Fixtures;

namespace VDF.IntegrationTests.FFTools;

/// <summary>
/// The seek-based dense sampler must reproduce the CLI sweep's timeline slot for slot
/// (-skip_frame nokey + fps=1/interval:round=up): same slot count, the same keyframe in
/// each slot, duplicated slots across keyframe gaps in the same places, so cached
/// DenseEmbeddings records from either path stay comparable.
/// </summary>
[Collection("Ffmpeg")]
public class DenseNativeSamplingParityTests {
	// Keyframes every 8.32 s (208 frames at 25 fps) against a 5 s sampling interval: every
	// other slot or so falls into a keyframe gap and must repeat the previous keyframe.
	const int Fps = 25;
	const int Gop = 208;
	const double Interval = 5;
	const double DurationSeconds = 47;
	// swscale (CLI) vs swscale (native CPU path) on the same decoded keyframe: near-identical.
	const double CpuMaxMae = 2.0;
	// D3D11 video processor vs swscale: a few levels apart (bicubic vs the driver's filter).
	const double GpuMaxMae = 10.0;
	// Rotated files: the CLI rotates before scaling, the native path scales then rotates the
	// 224x224 result; swscale's horizontal and vertical passes round differently (~3 levels).
	const double RotatedCpuMaxMae = 5.0;

	readonly FfmpegFixture _fixture;
	readonly Xunit.Abstractions.ITestOutputHelper _output;

	public DenseNativeSamplingParityTests(FfmpegFixture fixture, Xunit.Abstractions.ITestOutputHelper output) {
		_fixture = fixture;
		_output = output;
	}

	string? Generate(string name, string extraOutputArgs = "", string? rotateDegrees = null) {
		string dir = Path.Combine(_fixture.TempDir, "dense-native");
		Directory.CreateDirectory(dir);
		string path = Path.Combine(dir, name);
		if (File.Exists(path))
			return path;
		string container = Path.GetExtension(name);
		string source = rotateDegrees == null ? path : Path.Combine(dir, "unrotated-" + name);
		// testsrc2 moves every frame, so neighbouring keyframes look clearly different and a
		// slot holding the wrong keyframe cannot pass the per-slot error check.
		string args =
			$"-hide_banner -loglevel error -y -f lavfi -i testsrc2=duration={DurationSeconds}:size=640x360:rate={Fps} " +
			$"-f lavfi -i sine=frequency=440:duration={DurationSeconds} " +
			$"-c:v libx264 -pix_fmt yuv420p -bf 3 -x264-params keyint={Gop}:min-keyint={Gop}:scenecut=0 " +
			$"-c:a {(container == ".mkv" ? "libopus" : "aac")} -shortest {extraOutputArgs} \"{source}\"";
		if (!RunFfmpeg(args))
			return null;
		if (rotateDegrees != null &&
			!RunFfmpeg($"-hide_banner -loglevel error -y -display_rotation {rotateDegrees} -i \"{source}\" -c copy \"{path}\""))
			return null;
		return path;
	}

	static bool RunFfmpeg(string args) {
		var psi = new ProcessStartInfo(FfmpegEngine.FFmpegPath, args) {
			CreateNoWindow = true,
			UseShellExecute = false,
			RedirectStandardError = true,
		};
		using Process? process = Process.Start(psi);
		if (process == null)
			return false;
		process.StandardError.ReadToEnd();
		return process.WaitForExit(120_000) && process.ExitCode == 0;
	}

	static List<byte[]> CollectCli(string file, double interval = Interval, bool allowFailure = false) {
		var frames = new List<byte[]>();
		int count = FfmpegEngine.StreamDenseAiFrames(file, interval, 400, extendedLogging: true, frame => {
			frames.Add(frame.ToArray());
			FramePool.Shared.Return(frame);
		});
		if (allowFailure && count < 0)
			return frames;
		Assert.Equal(frames.Count, count);
		return frames;
	}

	static List<byte[]> CollectNative(string file, out DenseNativeStats stats) {
		var frames = new List<byte[]>();
		int count = FfmpegEngine.StreamDenseAiFramesNative(file, DurationSeconds, Interval, 400, extendedLogging: true, frame => {
			frames.Add(frame.ToArray());
			FramePool.Shared.Return(frame);
		}, CancellationToken.None, codecName: null, familyKey: null, out stats);
		Assert.True(count >= 0, $"native dense sampling returned {count}");
		Assert.Equal(frames.Count, count);
		return frames;
	}

	static double Mae(byte[] a, byte[] b) {
		long sum = 0;
		for (int i = 0; i < a.Length; i++)
			sum += Math.Abs(a[i] - b[i]);
		return sum / (double)a.Length;
	}

	static bool IsDuplicateOfPrevious(List<byte[]> frames, int k) =>
		k > 0 && frames[k].AsSpan().SequenceEqual(frames[k - 1]);

	void AssertParity(string file, List<byte[]> cli, List<byte[]> native, double maxMae, string label) {
		_output.WriteLine($"{label} {Path.GetFileName(file)}: cli={cli.Count} native={native.Count} slots");
		Assert.Equal(cli.Count, native.Count);
		double worst = 0;
		for (int k = 0; k < cli.Count; k++) {
			double mae = Mae(cli[k], native[k]);
			worst = Math.Max(worst, mae);
			// The slot holds the same keyframe, not merely a similar one: its neighbours (when
			// they are a different keyframe) are far further away.
			string neighbours = string.Empty;
			if (k + 1 < cli.Count && !IsDuplicateOfPrevious(cli, k + 1))
				neighbours += $" next={Mae(cli[k + 1], native[k]):F2}";
			if (k > 0 && !IsDuplicateOfPrevious(cli, k))
				neighbours += $" prev={Mae(cli[k - 1], native[k]):F2}";
			_output.WriteLine($"  slot {k,2} t={k * Interval,5:F1}s mae={mae:F2} dupCli={IsDuplicateOfPrevious(cli, k)} dupNative={IsDuplicateOfPrevious(native, k)}{neighbours}");
			Assert.True(mae <= maxMae, $"{label}: slot {k} differs from the CLI by MAE {mae:F2} (limit {maxMae}).");
			Assert.Equal(IsDuplicateOfPrevious(cli, k), IsDuplicateOfPrevious(native, k));
		}
		_output.WriteLine($"  worst slot MAE {worst:F2}");
		Assert.Contains(Enumerable.Range(0, cli.Count), k => IsDuplicateOfPrevious(cli, k));
	}

	void SkipIfUnavailable() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
	}

	static bool D3D11DecodeAvailable(string file) {
		if (!OperatingSystem.IsWindows())
			return false;
		try {
			using var decoder = new VideoStreamDecoder(file, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
			return decoder.TryDecodeFrame(out AVFrame frame, TimeSpan.Zero, out _, FrameTransferMode.KeepHardwareFrame) &&
				VideoStreamDecoder.IsHardwareFrame(frame);
		}
		catch {
			return false;
		}
	}

	[SkippableTheory]
	[InlineData("gop-gap.mp4", "-movflags +faststart")]
	[InlineData("gop-gap.mkv", "")]
	public void NativeCpu_MatchesCliSweep_SlotForSlot(string name, string extraArgs) {
		SkipIfUnavailable();
		string? file = Generate(name, extraArgs);
		Skip.If(file == null, "could not generate the test video");

		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		List<byte[]> cli = CollectCli(file!);
		List<byte[]> native = CollectNative(file!, out DenseNativeStats stats);
		_output.WriteLine($"stats: seeks={stats.Seeks} decodes={stats.Decodes} duplicates={stats.Duplicates} keyframeBytes={stats.KeyframePacketBytes:N0} file={new FileInfo(file!).Length:N0} hw={stats.HardwareDecode}");
		Assert.False(stats.HardwareDecode);
		AssertParity(file!, cli, native, CpuMaxMae, "cpu");
		Assert.True(stats.Decodes <= (int)Math.Ceiling(DurationSeconds * Fps / Gop) + 1,
			$"decoded {stats.Decodes} keyframes; a file with {Math.Ceiling(DurationSeconds * Fps / Gop)} keyframes needs no more");
	}

	[SkippableFact]
	public void NativeD3D11_MatchesCliSweep_SlotForSlot() {
		SkipIfUnavailable();
		string? file = Generate("gop-gap.mp4", "-movflags +faststart");
		Skip.If(file == null, "could not generate the test video");
		Skip.If(!D3D11DecodeAvailable(file!), "D3D11VA decode unavailable on this machine");

		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
		List<byte[]> cli = CollectCli(file!);
		List<byte[]> native = CollectNative(file!, out DenseNativeStats stats);
		_output.WriteLine($"stats: decodes={stats.Decodes} duplicates={stats.Duplicates} hw={stats.HardwareDecode} gpuScale={stats.GpuScale}");
		Assert.True(stats.HardwareDecode, "expected D3D11VA decode");
		Assert.True(stats.GpuScale, "expected the D3D11 video processor to scale");
		AssertParity(file!, cli, native, GpuMaxMae, "d3d11");
	}

	[SkippableTheory]
	[InlineData("none")]
	[InlineData("d3d11va")]
	public void NativeRotated_MatchesCliAutorotate(string mode) {
		SkipIfUnavailable();
		string? file = Generate("gop-gap-rot90.mp4", rotateDegrees: "90");
		Skip.If(file == null, "could not generate the rotated test video (needs FFmpeg 7+ -display_rotation)");
		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = Enum.Parse<FFHardwareAccelerationMode>(mode);
		if (mode == "d3d11va")
			Skip.If(!D3D11DecodeAvailable(file!), "D3D11VA decode unavailable on this machine");
		List<byte[]> cli = CollectCli(file!);
		List<byte[]> native = CollectNative(file!, out _);
		AssertParity(file!, cli, native, mode == "none" ? RotatedCpuMaxMae : GpuMaxMae, $"rot90-{mode}");
	}

	[SkippableFact]
	public void UnindexedContainer_IsLeftToTheCli() {
		SkipIfUnavailable();
		string? file = Generate("gop-gap.ts");
		Skip.If(file == null, "could not generate the test video");
		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		int delivered = 0;
		int result = FfmpegEngine.StreamDenseAiFramesNative(file!, DurationSeconds, Interval, 400, false,
			frame => { delivered++; FramePool.Shared.Return(frame); }, CancellationToken.None);
		Assert.Equal(FfmpegEngine.DenseNativeUnavailable, result);
		Assert.Equal(0, delivered);
	}

	[SkippableFact]
	public void Vp9_IsLeftToTheCli() {
		SkipIfUnavailable();
		// The VP9 decoder ignores skip_frame, so the CLI holds the exact frame at each slot
		// time - not reproducible by keyframe seeks.
		Skip.If(_fixture.VP9 == null, "VP9 test video not generated");
		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		int result = FfmpegEngine.StreamDenseAiFramesNative(_fixture.VP9!, 2, Interval, 400, false,
			FramePool.Shared.Return, CancellationToken.None);
		Assert.Equal(FfmpegEngine.DenseNativeUnavailable, result);
	}

	[SkippableFact]
	public void OptOutEnvVar_DisablesNativeSampling() {
		SkipIfUnavailable();
		string? file = Generate("gop-gap.mp4", "-movflags +faststart");
		Skip.If(file == null, "could not generate the test video");
		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		string? previous = Environment.GetEnvironmentVariable(FfmpegEngine.DisableNativeDenseAiEnvVar);
		try {
			Environment.SetEnvironmentVariable(FfmpegEngine.DisableNativeDenseAiEnvVar, "1");
			int result = FfmpegEngine.StreamDenseAiFramesNative(file!, DurationSeconds, Interval, 400, false,
				FramePool.Shared.Return, CancellationToken.None);
			Assert.Equal(FfmpegEngine.DenseNativeUnavailable, result);
		}
		finally {
			Environment.SetEnvironmentVariable(FfmpegEngine.DisableNativeDenseAiEnvVar, previous);
		}
	}

	[StructLayout(LayoutKind.Sequential)]
	struct IoCounters {
		public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount;
		public ulong ReadTransferCount, WriteTransferCount, OtherTransferCount;
	}

	[DllImport("kernel32.dll", SetLastError = true)]
	static extern bool GetProcessIoCounters(IntPtr hProcess, out IoCounters counters);

	static ulong ReadBytes(Process process) =>
		OperatingSystem.IsWindows() && GetProcessIoCounters(process.Handle, out IoCounters c) ? c.ReadTransferCount : 0;

	static double ProbeDuration(string file) {
		string probe = Path.Combine(Path.GetDirectoryName(FfmpegEngine.FFmpegPath)!, OperatingSystem.IsWindows() ? "ffprobe.exe" : "ffprobe");
		var psi = new ProcessStartInfo(File.Exists(probe) ? probe : "ffprobe",
			$"-v error -show_entries format=duration -of csv=p=0 \"{file}\"") {
			CreateNoWindow = true, UseShellExecute = false, RedirectStandardOutput = true,
		};
		using Process p = Process.Start(psi)!;
		string text = p.StandardOutput.ReadToEnd();
		p.WaitForExit();
		return double.Parse(text.Trim(), System.Globalization.CultureInfo.InvariantCulture);
	}

	/// <summary>Runs the CLI sweep exactly as StreamDenseAiFrames does, for its process I/O and CPU time.</summary>
	static (double wallMs, double cpuMs, ulong readBytes) MeasureCli(string file, double interval) {
		var psi = new ProcessStartInfo(FfmpegEngine.FFmpegPath) {
			CreateNoWindow = true, UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true,
		};
		foreach (string a in new[] { "-hide_banner", "-loglevel", "error", "-nostdin", "-skip_frame", "nokey", "-an", "-sn", "-dn", "-i", file,
			"-vf", FormattableString.Invariant($"fps=1/{interval:0.###}:round=up,scale=224:224:flags=bicubic,format=rgb24"),
			"-frames:v", "400", "-f", "rawvideo", "-pix_fmt", "rgb24", "pipe:1" })
			psi.ArgumentList.Add(a);
		var sw = Stopwatch.StartNew();
		using Process p = Process.Start(psi)!;
		var err = p.StandardError.ReadToEndAsync();
		p.StandardOutput.BaseStream.CopyTo(Stream.Null);
		p.WaitForExit();
		sw.Stop();
		_ = err.Result;
		return (sw.Elapsed.TotalMilliseconds, p.TotalProcessorTime.TotalMilliseconds, ReadBytes(p));
	}

	(double wallMs, double cpuMs, ulong readBytes, List<byte[]> frames, DenseNativeStats stats) MeasureNative(string file, double duration, double interval) {
		using Process self = Process.GetCurrentProcess();
		var frames = new List<byte[]>();
		ulong readBefore = ReadBytes(self);
		self.Refresh();
		TimeSpan cpuBefore = self.TotalProcessorTime;
		var sw = Stopwatch.StartNew();
		int count = FfmpegEngine.StreamDenseAiFramesNative(file, duration, interval, 400, true, frame => {
			frames.Add(frame.ToArray());
			FramePool.Shared.Return(frame);
		}, CancellationToken.None, null, null, out DenseNativeStats stats);
		sw.Stop();
		self.Refresh();
		Assert.True(count >= 0 || count == FfmpegEngine.DenseNativeUnavailable, $"native returned {count} for {file}");
		return (sw.Elapsed.TotalMilliseconds, (self.TotalProcessorTime - cpuBefore).TotalMilliseconds, ReadBytes(self) - readBefore, frames, stats);
	}

	/// <summary>
	/// Measurement probe, not a regression test: set VDF_DENSE_CORPUS_DIRS (';'-separated) to
	/// compare native (CPU and D3D11) against the CLI sweep on real files: slot parity, bytes
	/// read (process I/O counters, logical reads), wall time and CPU time. Optional
	/// VDF_DENSE_CORPUS_REPORT writes the table as CSV.
	/// </summary>
	[SkippableFact]
	public void Corpus_MeasureNativeVsCli() {
		string? dirs = Environment.GetEnvironmentVariable("VDF_DENSE_CORPUS_DIRS");
		Skip.If(string.IsNullOrWhiteSpace(dirs), "set VDF_DENSE_CORPUS_DIRS to run the corpus measurement");
		SkipIfUnavailable();
		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		var csv = new List<string> { "file,format,sizeBytes,durationS,interval,cliSlots,nativeSlots,decodes,dupMismatchCpu,maeCpu,maeCpuShift1,maeGpu,keyframeBytes,cliReadBytes,cpuReadBytes,gpuReadBytes,cliWallMs,cpuWallMs,gpuWallMs,cliCpuMs,cpuCpuMs,gpuCpuMs,gpuScale" };
		foreach (string dir in dirs!.Split(';', StringSplitOptions.RemoveEmptyEntries))
			foreach (string file in Directory.EnumerateFiles(dir).OrderBy(f => f)) {
				double duration = ProbeDuration(file);
				double interval = VDF.Core.ScanEngine.GetAiPartialIntervalSeconds(duration);
				FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
				List<byte[]> cli = CollectCli(file, interval, allowFailure: true);
				var cliM = MeasureCli(file, interval);
				var cpuM = MeasureNative(file, duration, interval);
				string gpuMae = "n/a";
				(double wallMs, double cpuMs, ulong readBytes, List<byte[]> frames, DenseNativeStats stats)? gpuM = null;
				if (D3D11DecodeAvailable(file)) {
					FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
					gpuM = MeasureNative(file, duration, interval);
					int n = Math.Min(cli.Count, gpuM.Value.frames.Count);
					gpuMae = n == 0 ? "n/a" : Enumerable.Range(0, n).Average(k => Mae(cli[k], gpuM.Value.frames[k])).ToString("F2");
				}
				int m = Math.Min(cli.Count, cpuM.frames.Count);
				int dupMismatch = Enumerable.Range(0, m).Count(k => IsDuplicateOfPrevious(cli, k) != IsDuplicateOfPrevious(cpuM.frames, k));
				double maeCpu = m == 0 ? double.NaN : Enumerable.Range(0, m).Average(k => Mae(cli[k], cpuM.frames[k]));
				if (Environment.GetEnvironmentVariable("VDF_DENSE_CORPUS_VERBOSE") == "1")
					for (int k = 0; k < cpuM.frames.Count; k++) {
						string gpuPart = gpuM == null || k >= gpuM.Value.frames.Count ? string.Empty
							: FormattableString.Invariant($" gpuVsCpu={Mae(gpuM.Value.frames[k], cpuM.frames[k]):F2}");
						if (cli.Count == 0) { _output.WriteLine($"  {Path.GetFileName(file)} slot {k}{gpuPart}"); continue; }
						int best = Enumerable.Range(0, cli.Count).MinBy(j => Mae(cli[j], cpuM.frames[k]));
						_output.WriteLine(FormattableString.Invariant(
							$"  {Path.GetFileName(file)} slot {k}: vsCliSame={(k < cli.Count ? Mae(cli[k], cpuM.frames[k]) : double.NaN):F2} bestCli={best} ({Mae(cli[best], cpuM.frames[k]):F2}) dupNative={IsDuplicateOfPrevious(cpuM.frames, k)} dupCli={(k < cli.Count && IsDuplicateOfPrevious(cli, k))}{gpuPart}"));
					}
				int ms = Math.Min(cli.Count, cpuM.frames.Count - 1);
				double maeShift = ms <= 0 ? double.NaN : Enumerable.Range(0, ms).Average(k => Mae(cli[k], cpuM.frames[k + 1]));
				string format;
				using (var vsd = new VideoStreamDecoder(file))
					format = vsd.FormatName;
				string row = FormattableString.Invariant(
					$"{Path.GetFileName(file)},\"{format}\",{new FileInfo(file).Length},{duration:F1},{interval:F3},{cli.Count},{cpuM.frames.Count},{cpuM.stats.Decodes},{dupMismatch},{maeCpu:F2},{maeShift:F2},{gpuMae},{cpuM.stats.KeyframePacketBytes},{cliM.readBytes},{cpuM.readBytes},{gpuM?.readBytes},{cliM.wallMs:F0},{cpuM.wallMs:F0},{gpuM?.wallMs:F0},{cliM.cpuMs:F0},{cpuM.cpuMs:F0},{gpuM?.cpuMs:F0},{gpuM?.stats.GpuScale}");
				_output.WriteLine(row);
				csv.Add(row);
			}
		string? report = Environment.GetEnvironmentVariable("VDF_DENSE_CORPUS_REPORT");
		if (!string.IsNullOrWhiteSpace(report))
			File.WriteAllLines(report, csv);
	}

	[SkippableFact]
	public void Cancellation_Throws() {
		SkipIfUnavailable();
		string? file = Generate("gop-gap.mp4", "-movflags +faststart");
		Skip.If(file == null, "could not generate the test video");
		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		using var cts = new CancellationTokenSource();
		Assert.Throws<OperationCanceledException>(() =>
			FfmpegEngine.StreamDenseAiFramesNative(file!, DurationSeconds, Interval, 400, false, frame => {
				FramePool.Shared.Return(frame);
				cts.Cancel();
			}, cts.Token));
	}
}
