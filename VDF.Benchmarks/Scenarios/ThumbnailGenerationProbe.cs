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
using VDF.Core;
using VDF.Core.FFTools;
using VDF.Core.FFTools.FFmpegNative;
using VDF.TestSupport;
using Size = System.Drawing.Size;

namespace VDF.Benchmarks.Scenarios;

/// <summary>
/// Direct thumbnail extraction probe. This intentionally bypasses BenchmarkDotNet
/// so it can report process CPU time and child FFmpeg CPU time while mimicking the
/// automatic preview-thumbnail shape: several positions per video and a configurable
/// item-level parallelism.
///
/// Run with:
///
///   dotnet run -c Release --project VDF.Benchmarks -- --probe-thumbnails
///
/// Optional args:
///
///   --hw=none|d3d11va     Hardware mode to compare (default: d3d11va on Windows, none elsewhere)
///   --items=N             Number of synthetic duplicate items per parallel pass
///   --iterations=N        Warm sequential work count
///   --max-width=N         Thumbnail max dimension; 0 means full-size
///   --video=PATH          Existing video to probe instead of generating a corpus file
///   --duration=N          Duration in seconds for --video percentage positions (default: 60)
///   --ffmpeg=PATH         Explicit ffmpeg executable for corpus/process-mode probing
/// </summary>
public static class ThumbnailGenerationProbe {
	const int DefaultThumbnailQuality = 90;

	enum ExtractionMode {
		Native,
		Process
	}

	readonly record struct ProbeCase(ExtractionMode Mode, FFHardwareAccelerationMode HardwareMode, int MaxParallel);

	readonly struct ProbeResult {
		public ProbeResult(long wallMs, long cpuMs, int thumbnails, long bytes, int failures, string? firstError) {
			WallMs = wallMs;
			CpuMs = cpuMs;
			Thumbnails = thumbnails;
			Bytes = bytes;
			Failures = failures;
			FirstError = firstError;
		}

		public long WallMs { get; }
		public long CpuMs { get; }
		public int Thumbnails { get; }
		public long Bytes { get; }
		public int Failures { get; }
		public string? FirstError { get; }
		public double CpuCoresEquivalent => WallMs <= 0 ? 0 : CpuMs / (double)WallMs;
	}

	public static int Run(string[] args) {
		var spec = new VideoCorpus.Spec(VideoCorpus.Codec.H264, 1280, 720, 60);
		string? ffmpegPath = ResolveFfmpegPath(ParseStringArg(args, "--ffmpeg="));
		string? requestedVideo = ParseStringArg(args, "--video=");
		string? path = ResolveVideoPath(requestedVideo, ffmpegPath, spec);
		if (path == null) {
			Console.Error.WriteLine("Could not resolve a video to probe. Supply --video=PATH, or supply/discover ffmpeg so the synthetic corpus can be generated.");
			return 1;
		}

		FFHardwareAccelerationMode hardwareMode = ParseHardwareMode(args);
		int items = ParseIntArg(args, "--items=", Math.Min(Environment.ProcessorCount, 12), min: 1, max: 128);
		int iterations = ParseIntArg(args, "--iterations=", 6, min: 1, max: 64);
		int maxWidth = ParseIntArg(args, "--max-width=", 100, min: 0, max: 4096);
		int durationSeconds = requestedVideo == null
			? spec.Duration
			: ParseIntArg(args, "--duration=", spec.Duration, min: 1, max: 86_400);
		var positions = Enumerable.Range(1, 4)
			.Select(i => TimeSpan.FromSeconds(durationSeconds * i / 5.0))
			.ToArray();

		Console.WriteLine("== Thumbnail generation probe ==");
		Console.WriteLine($"video:        {(requestedVideo == null ? $"{spec.Codec} {spec.Width}x{spec.Height} {spec.Duration}s" : Path.GetFileName(path))}");
		Console.WriteLine($"path:         {path}");
		Console.WriteLine($"ffmpeg:       {(ffmpegPath ?? "(not found; process mode skipped)")}");
		Console.WriteLine($"positions:    {string.Join(", ", positions.Select(p => p.TotalSeconds.ToString("0.0")))}s");
		Console.WriteLine($"max width:    {(maxWidth == 0 ? "full-size" : maxWidth)}");
		Console.WriteLine($"items:        {items}");
		Console.WriteLine($"iterations:   {iterations}");
		Console.WriteLine($"hardware:     {hardwareMode}");
		Console.WriteLine($"native libs:  {(ScanEngine.NativeFFmpegExists ? "yes" : "no")}");
		Console.WriteLine();

		if (!ScanEngine.NativeFFmpegExists)
			Console.WriteLine("Native cases will be skipped because native FFmpeg libraries are missing.");
		if (ffmpegPath == null)
			Console.WriteLine("Process cases will be skipped because ffmpeg CLI was not found.");

		var parallelismLevels = new[] { 1, 2, Math.Min(Environment.ProcessorCount, 12) }
			.Distinct()
			.OrderBy(level => level)
			.ToArray();

		Console.WriteLine("mode      hw        maxPar  wall(ms)  cpu(ms)  cpu/wall  thumbs  bytes      failures");
		Console.WriteLine("-------------------------------------------------------------------------------------");

		foreach (ExtractionMode mode in Enum.GetValues<ExtractionMode>()) {
			if (mode == ExtractionMode.Native && !ScanEngine.NativeFFmpegExists)
				continue;
			if (mode == ExtractionMode.Process && ffmpegPath == null)
				continue;

			foreach (int maxParallel in parallelismLevels) {
				int workItems = maxParallel == 1 ? iterations : items;
				var probeCase = new ProbeCase(mode, hardwareMode, maxParallel);
				_ = RunCase(probeCase, path, ffmpegPath, positions, Math.Min(workItems, 2), maxWidth);
				ProbeResult result = RunCase(probeCase, path, ffmpegPath, positions, workItems, maxWidth);
				Console.WriteLine($"{mode.ToString().ToLowerInvariant(),-9} {hardwareMode,-9} {maxParallel,6}  {result.WallMs,8}  {result.CpuMs,7}  {result.CpuCoresEquivalent,8:0.00}  {result.Thumbnails,6}  {result.Bytes,9}  {result.Failures,8}");
				if (result.FirstError != null)
					Console.WriteLine($"  first error: {result.FirstError}");
			}
		}

		Console.WriteLine();
		Console.WriteLine("cpu/wall approximates how many CPU cores were busy during the measured extraction work.");
		Console.WriteLine("For process mode, cpu(ms) is summed from child FFmpeg processes; for native mode, it is this process.");
		return 0;
	}

	static ProbeResult RunCase(ProbeCase probeCase, string path, string? ffmpegPath, TimeSpan[] positions, int workItems, int maxWidth) {
		long childCpuTicks = 0;
		long bytes = 0;
		int thumbnails = 0;
		int failures = 0;
		string? firstError = null;
		object firstErrorLock = new();

		TimeSpan cpuStart = Process.GetCurrentProcess().TotalProcessorTime;
		var wall = Stopwatch.StartNew();
		Parallel.For(0, workItems, new ParallelOptions { MaxDegreeOfParallelism = probeCase.MaxParallel }, _ => {
			foreach (TimeSpan position in positions) {
				try {
					TimeSpan childCpu = TimeSpan.Zero;
					byte[]? jpeg = probeCase.Mode == ExtractionMode.Native
						? ExtractNativeThumbnail(path, position, maxWidth, probeCase.HardwareMode)
						: ExtractProcessThumbnail(ffmpegPath ?? throw new InvalidOperationException("ffmpeg CLI missing."), path, position, maxWidth, probeCase.HardwareMode, out childCpu);
					Interlocked.Add(ref childCpuTicks, childCpu.Ticks);
					if (jpeg == null || jpeg.Length == 0) {
						Interlocked.Increment(ref failures);
						continue;
					}

					Interlocked.Add(ref bytes, jpeg.Length);
					Interlocked.Increment(ref thumbnails);
				}
				catch (Exception e) {
					Interlocked.Increment(ref failures);
					lock (firstErrorLock)
						firstError ??= e.Message;
				}
			}
		});
		wall.Stop();
		TimeSpan cpuEnd = Process.GetCurrentProcess().TotalProcessorTime;

		long cpuTicks = probeCase.Mode == ExtractionMode.Process
			? childCpuTicks
			: (cpuEnd - cpuStart).Ticks;
		return new ProbeResult(wall.ElapsedMilliseconds, (long)TimeSpan.FromTicks(cpuTicks).TotalMilliseconds, thumbnails, bytes, failures, firstError);
	}

	static unsafe byte[] ExtractNativeThumbnail(string path, TimeSpan position, int maxWidth, FFHardwareAccelerationMode hardwareMode) {
		AVHWDeviceType deviceType = ToDeviceType(hardwareMode);
		using var vsd = new VideoStreamDecoder(path, deviceType);
		if (!vsd.TryDecodeFrame(out var srcFrame, position, out _))
			throw new Exception($"TryDecodeFrame failed at pos={position}.");

		Size sourceSize = new(
			srcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width,
			srcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);
		if (sourceSize.Width <= 0 || sourceSize.Height <= 0)
			throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");

		AVPixelFormat sourcePixelFormat = (AVPixelFormat)srcFrame.format;
		if (!IsValidPixelFormat(sourcePixelFormat))
			sourcePixelFormat = vsd.PixelFormat;
		if (!IsValidPixelFormat(sourcePixelFormat))
			throw new Exception($"Invalid source pixel format {sourcePixelFormat}.");

		Size destinationSize = maxWidth <= 0 ? sourceSize : ScaleToMaxWidth(sourceSize, maxWidth);
		using var converter = new VideoFrameConverter(
			sourceSize,
			sourcePixelFormat,
			destinationSize,
			AVPixelFormat.AV_PIX_FMT_YUVJ420P,
			VideoFrameConverter.ScaleQuality.Bicubic,
			bitExact: false);
		AVFrame convertedFrame = converter.Convert(srcFrame);
		return JpegFrameEncoder.Encode(convertedFrame, DefaultThumbnailQuality);
	}

	static byte[]? ExtractProcessThumbnail(string ffmpegPath, string path, TimeSpan position, int maxWidth, FFHardwareAccelerationMode hardwareMode, out TimeSpan childCpu) {
		childCpu = TimeSpan.Zero;
		var psi = new ProcessStartInfo {
			FileName = ffmpegPath,
			CreateNoWindow = true,
			RedirectStandardInput = false,
			RedirectStandardOutput = true,
			RedirectStandardError = true,
			WorkingDirectory = Path.GetDirectoryName(ffmpegPath) ?? string.Empty,
			WindowStyle = ProcessWindowStyle.Hidden
		};
		psi.ArgumentList.Add("-hide_banner");
		psi.ArgumentList.Add("-loglevel"); psi.ArgumentList.Add("error");
		psi.ArgumentList.Add("-nostdin");
		if (hardwareMode != FFHardwareAccelerationMode.none) {
			psi.ArgumentList.Add("-hwaccel");
			psi.ArgumentList.Add(hardwareMode.ToString());
		}
		psi.ArgumentList.Add("-ss"); psi.ArgumentList.Add(position.ToString(null, System.Globalization.CultureInfo.InvariantCulture));
		psi.ArgumentList.Add("-i"); psi.ArgumentList.Add(path);
		if (maxWidth > 0) {
			psi.ArgumentList.Add("-vf");
			psi.ArgumentList.Add($"scale=min({maxWidth}\\,iw):min({maxWidth}\\,ih):force_original_aspect_ratio=decrease");
		}
		psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("mjpeg");
		psi.ArgumentList.Add("-q:v"); psi.ArgumentList.Add("3");
		psi.ArgumentList.Add("-frames:v"); psi.ArgumentList.Add("1");
		psi.ArgumentList.Add("pipe:1");

		using var process = new Process { StartInfo = psi };
		process.Start();
		Task<string> errTask = process.StandardError.ReadToEndAsync();
		using var ms = new MemoryStream();
		process.StandardOutput.BaseStream.CopyTo(ms);
		process.WaitForExit();
		string err = errTask.GetAwaiter().GetResult();
		try { childCpu = process.TotalProcessorTime; }
		catch { childCpu = TimeSpan.Zero; }

		if (process.ExitCode != 0)
			throw new Exception($"ffmpeg exited with {process.ExitCode}: {err.Trim()}");

		byte[] bytes = ms.ToArray();
		return bytes.Length == 0 ? null : bytes;
	}

	static string? ResolveFfmpegPath(string? requestedPath) {
		if (!string.IsNullOrWhiteSpace(requestedPath)) {
			string expanded = Environment.ExpandEnvironmentVariables(requestedPath.Trim('"'));
			if (File.Exists(expanded))
				return Path.GetFullPath(expanded);
		}

		return VideoCorpus.FfmpegAvailable ? VideoCorpus.FfmpegPath : null;
	}

	static string? ResolveVideoPath(string? requestedVideo, string? ffmpegPath, VideoCorpus.Spec spec) {
		if (!string.IsNullOrWhiteSpace(requestedVideo)) {
			string expanded = Environment.ExpandEnvironmentVariables(requestedVideo.Trim('"'));
			return File.Exists(expanded) ? Path.GetFullPath(expanded) : null;
		}

		if (ffmpegPath == null)
			return null;

		Directory.CreateDirectory(VideoCorpus.CacheDir);
		string path = Path.Combine(VideoCorpus.CacheDir, spec.FileName);
		if (File.Exists(path) && new FileInfo(path).Length > 0)
			return path;

		return TestVideoGenerator.GenerateH264(ffmpegPath, path, spec.Width, spec.Height, spec.Duration)
			? path
			: null;
	}

	static FFHardwareAccelerationMode ParseHardwareMode(string[] args) {
		string? value = args.FirstOrDefault(arg => arg.StartsWith("--hw=", StringComparison.OrdinalIgnoreCase))?["--hw=".Length..];
		if (value != null && Enum.TryParse(value, ignoreCase: true, out FFHardwareAccelerationMode parsed))
			return parsed;
		return OperatingSystem.IsWindows() ? FFHardwareAccelerationMode.d3d11va : FFHardwareAccelerationMode.none;
	}

	static int ParseIntArg(string[] args, string prefix, int defaultValue, int min, int max) {
		string? value = args.FirstOrDefault(arg => arg.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))?[prefix.Length..];
		if (!int.TryParse(value, out int parsed))
			return defaultValue;
		return Math.Clamp(parsed, min, max);
	}

	static string? ParseStringArg(string[] args, string prefix) =>
		args.FirstOrDefault(arg => arg.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))?[prefix.Length..];

	static AVHWDeviceType ToDeviceType(FFHardwareAccelerationMode hardwareMode) =>
		hardwareMode switch {
			FFHardwareAccelerationMode.vdpau => AVHWDeviceType.AV_HWDEVICE_TYPE_VDPAU,
			FFHardwareAccelerationMode.dxva2 => AVHWDeviceType.AV_HWDEVICE_TYPE_DXVA2,
			FFHardwareAccelerationMode.vaapi => AVHWDeviceType.AV_HWDEVICE_TYPE_VAAPI,
			FFHardwareAccelerationMode.qsv => AVHWDeviceType.AV_HWDEVICE_TYPE_QSV,
			FFHardwareAccelerationMode.cuda => AVHWDeviceType.AV_HWDEVICE_TYPE_CUDA,
			FFHardwareAccelerationMode.videotoolbox => AVHWDeviceType.AV_HWDEVICE_TYPE_VIDEOTOOLBOX,
			FFHardwareAccelerationMode.d3d11va => AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA,
			FFHardwareAccelerationMode.drm => AVHWDeviceType.AV_HWDEVICE_TYPE_DRM,
			FFHardwareAccelerationMode.mediacodec => AVHWDeviceType.AV_HWDEVICE_TYPE_MEDIACODEC,
			FFHardwareAccelerationMode.vulkan => AVHWDeviceType.AV_HWDEVICE_TYPE_VULKAN,
			_ => AVHWDeviceType.AV_HWDEVICE_TYPE_NONE
		};

	static unsafe bool IsValidPixelFormat(AVPixelFormat pixelFormat) {
		if (pixelFormat < 0 || pixelFormat >= AVPixelFormat.AV_PIX_FMT_NB)
			return false;
		return ffmpeg.av_pix_fmt_desc_get(pixelFormat) != null;
	}

	static Size ScaleToMaxWidth(Size source, int maxDim) {
		if (source.Width <= maxDim && source.Height <= maxDim)
			return source;
		double factor = Math.Max(source.Width / (double)maxDim, source.Height / (double)maxDim);
		return new Size(
			Math.Max(1, (int)Math.Round(source.Width / factor)),
			Math.Max(1, (int)Math.Round(source.Height / factor)));
	}
}
