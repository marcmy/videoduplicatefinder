using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
		// Compatibility overload for upstream's AI embedding pipeline. The perf branch
		// keeps its optimized gray-byte extraction path and only performs the additional
		// RGB extraction when the sink requests an embedding for a sampled position.
		internal static bool GetGrayBytesFromVideo(
			FileEntry videoFile,
			List<float> positions,
			double maxSamplingDurationSeconds,
			bool extendedLogging,
			Action<int>? onSampleComplete,
			global::VDF.Core.AI.IEmbeddingFrameSink? embeddingSink) {
			bool ok = GetGrayBytesFromVideo(
				videoFile,
				positions,
				maxSamplingDurationSeconds,
				extendedLogging,
				onSampleComplete: null);
			if (!ok)
				return false;

			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (embeddingSink?.WantsEmbedding(videoFile, position) == true) {
					(_, byte[]? rgb) = GetGrayAndRgb224Cli(
						videoFile.Path,
						TimeSpan.FromSeconds(position),
						softwareDecodeOnly: false,
						extendedLogging);
					if (rgb != null)
						embeddingSink.SubmitFrame(videoFile, position, rgb);
				}
				onSampleComplete?.Invoke(i + 1);
			}
			return true;
		}

		// Dense AI partial-match sampling. Decode a bounded raw RGB24 sequence in one
		// FFmpeg process, then split the fixed-size output into individual model frames.
		internal static byte[][]? GetDenseAiFrames(
			string file,
			double intervalSeconds,
			int maxFrames,
			bool extendedLogging,
			CancellationToken cancellationToken) {
			if (maxFrames <= 0)
				return Array.Empty<byte[]>();
			intervalSeconds = Math.Max(0.01, intervalSeconds);
			int side = global::VDF.Core.AI.OnnxEmbedder.InputSide;
			int frameBytes = side * side * 3;

			var psi = new ProcessStartInfo {
				FileName = FFmpegPath,
				CreateNoWindow = true,
				RedirectStandardInput = false,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				WorkingDirectory = Path.GetDirectoryName(FFmpegPath)!,
				WindowStyle = ProcessWindowStyle.Hidden
			};
			psi.ArgumentList.Add("-hide_banner");
			psi.ArgumentList.Add("-loglevel");
			psi.ArgumentList.Add("error");
			psi.ArgumentList.Add("-nostdin");
			psi.ArgumentList.Add("-i");
			psi.ArgumentList.Add(FFToolsUtils.LongPathFix(file));
			psi.ArgumentList.Add("-vf");
			psi.ArgumentList.Add($"fps=1/{intervalSeconds.ToString(CultureInfo.InvariantCulture)},scale={side}:{side}:flags=bicubic,format=rgb24");
			psi.ArgumentList.Add("-frames:v");
			psi.ArgumentList.Add(maxFrames.ToString(CultureInfo.InvariantCulture));
			psi.ArgumentList.Add("-f");
			psi.ArgumentList.Add("rawvideo");
			psi.ArgumentList.Add("-pix_fmt");
			psi.ArgumentList.Add("rgb24");
			psi.ArgumentList.Add("pipe:1");

			using var process = new Process { StartInfo = psi };
			try {
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				using var output = new MemoryStream();
				using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
				timeout.CancelAfter(TimeSpan.FromMinutes(10));
				Task copyTask = process.StandardOutput.BaseStream.CopyToAsync(output, timeout.Token);
				Task<string> errorTask = process.StandardError.ReadToEndAsync(timeout.Token);
				Task waitTask = process.WaitForExitAsync(timeout.Token);
				Task.WhenAll(copyTask, errorTask, waitTask).GetAwaiter().GetResult();
				if (process.ExitCode != 0)
					throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}; {errorTask.Result}");

				byte[] bytes = output.ToArray();
				int count = Math.Min(maxFrames, bytes.Length / frameBytes);
				if (count == 0)
					return null;
				var frames = new byte[count][];
				for (int i = 0; i < count; i++) {
					frames[i] = new byte[frameBytes];
					Buffer.BlockCopy(bytes, i * frameBytes, frames[i], 0, frameBytes);
				}
				return frames;
			}
			catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
				try { if (!process.HasExited) process.Kill(); } catch { }
				throw;
			}
			catch (Exception ex) {
				try { if (!process.HasExited) process.Kill(); } catch { }
				if (extendedLogging)
					Logger.Instance.Info($"Dense AI frame extraction failed for '{file}': {ex}");
				return null;
			}
		}

		// Upstream API used by AI partial matching. This process fallback deliberately
		// remains separate from the perf branch's native gray-byte pipeline so the
		// acceleration code stays untouched and independently regression-tested.
		internal static (byte[]? GrayBytes, byte[]? Rgb224) GetGrayAndRgb224Cli(
			string file,
			TimeSpan position,
			bool softwareDecodeOnly,
			bool extendedLogging) {
			const int N = 32;
			const int grayExpectedBytes = N * N;
			int rgbSide = global::VDF.Core.AI.OnnxEmbedder.InputSide;
			int rgbExpectedBytes = rgbSide * rgbSide * 3;
			string rgbTempPath = Path.Combine(Path.GetTempPath(), $"VDF.AiFrame.{Guid.NewGuid():N}.rgb");

			var psi = new ProcessStartInfo {
				FileName = FFmpegPath,
				CreateNoWindow = true,
				RedirectStandardInput = false,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				WorkingDirectory = Path.GetDirectoryName(FFmpegPath)!,
				WindowStyle = ProcessWindowStyle.Hidden
			};
			psi.ArgumentList.Add("-hide_banner");
			psi.ArgumentList.Add("-loglevel");
			psi.ArgumentList.Add("error");
			psi.ArgumentList.Add("-nostdin");
			if (HardwareAccelerationMode != FFHardwareAccelerationMode.none && !softwareDecodeOnly) {
				psi.ArgumentList.Add("-hwaccel");
				psi.ArgumentList.Add(HardwareAccelerationMode.ToString());
			}
			bool isImage = FileUtils.IsImageFile(file);
			if (!isImage) {
				psi.ArgumentList.Add("-ss");
				psi.ArgumentList.Add(position.ToString(null, CultureInfo.InvariantCulture));
			}
			psi.ArgumentList.Add("-i");
			psi.ArgumentList.Add(FFToolsUtils.LongPathFix(file));
			psi.ArgumentList.Add("-filter_complex");
			psi.ArgumentList.Add(
				$"[0:v]split=2[g][r];" +
				$"[g]scale={N}:{N}:flags=bicubic,format=gray[gout];" +
				$"[r]scale={rgbSide}:{rgbSide}:flags=bicubic,format=rgb24[rout]");
			psi.ArgumentList.Add("-map");
			psi.ArgumentList.Add("[gout]");
			psi.ArgumentList.Add("-frames:v");
			psi.ArgumentList.Add("1");
			psi.ArgumentList.Add("-f");
			psi.ArgumentList.Add("rawvideo");
			psi.ArgumentList.Add("-pix_fmt");
			psi.ArgumentList.Add("gray");
			psi.ArgumentList.Add("pipe:1");
			psi.ArgumentList.Add("-map");
			psi.ArgumentList.Add("[rout]");
			psi.ArgumentList.Add("-frames:v");
			psi.ArgumentList.Add("1");
			psi.ArgumentList.Add("-f");
			psi.ArgumentList.Add("rawvideo");
			psi.ArgumentList.Add("-pix_fmt");
			psi.ArgumentList.Add("rgb24");
			psi.ArgumentList.Add("-y");
			psi.ArgumentList.Add(rgbTempPath);

			using var process = new Process { StartInfo = psi };
			byte[]? gray = null;
			byte[]? rgb = null;
			string error = string.Empty;
			try {
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				using var grayStream = new MemoryStream();
				process.StandardOutput.BaseStream.CopyTo(grayStream);
				error = process.StandardError.ReadToEnd();
				if (!process.WaitForExit(TimeoutDuration))
					throw new TimeoutException($"FFmpeg timed out on file: {file}");
				if (process.ExitCode != 0)
					throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}");

				gray = grayStream.ToArray();
				if (gray.Length != grayExpectedBytes)
					gray = null;
				if (File.Exists(rgbTempPath)) {
					rgb = File.ReadAllBytes(rgbTempPath);
					if (rgb.Length != rgbExpectedBytes)
						rgb = null;
				}
			}
			catch (Exception ex) {
				error = string.IsNullOrWhiteSpace(error) ? ex.Message : $"{error}{Environment.NewLine}{ex.Message}";
				try {
					if (!process.HasExited)
						process.Kill();
				}
				catch { }
				gray = null;
				rgb = null;
			}
			finally {
				try {
					if (File.Exists(rgbTempPath))
						File.Delete(rgbTempPath);
				}
				catch { }
			}

			if ((gray == null || rgb == null || extendedLogging) && !string.IsNullOrWhiteSpace(error))
				Logger.Instance.Info($"FFmpeg gray+AI extraction for '{file}': {error}");
			return (gray, rgb);
		}
	}
}
