using System.Diagnostics;
using System.Globalization;
using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	// Compatibility surface for upstream tests and safety helpers that are merged into the
	// performance branch while its specialized FfmpegEngine implementation is retained.
	internal static partial class FfmpegEngine {
		const int NativeFailureFullDetailLimit = 20;
		const int NativeFailureCompactLimit = 200;
		const int NativeFailureSummaryEvery = 100;

		internal enum NativeFailureLogMode { Full, Compact, Summary, Suppressed }

		internal static NativeFailureLogMode GetNativeFailureLogMode(int totalFailures) {
			if (totalFailures <= NativeFailureFullDetailLimit)
				return NativeFailureLogMode.Full;
			if (totalFailures <= NativeFailureCompactLimit)
				return NativeFailureLogMode.Compact;
			return totalFailures % NativeFailureSummaryEvery == 0
				? NativeFailureLogMode.Summary
				: NativeFailureLogMode.Suppressed;
		}

		internal static AVPixelFormat ResolveSourcePixelFormat(int frameFormat, AVPixelFormat openTimeFormat) =>
			frameFormat >= 0 ? (AVPixelFormat)frameFormat : openTimeFormat;

		// A corrupt/truncated software decode will fail identically through the process fallback,
		// so avoid paying another timeout. Hardware failures may be driver-specific and still
		// deserve the out-of-process retry.
		internal static bool ShouldSkipProcessRetryForCorruptFile(
			FfmpegErrorCategory category,
			AVHWDeviceType hardwareDeviceType) =>
			category == FfmpegErrorCategory.CorruptOrTruncated
			&& hardwareDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;

		internal sealed class TiledHeifRequiresProcessException : Exception {
			public TiledHeifRequiresProcessException(string path)
				: base($"Tiled HEIF needs FFmpeg's grid assembly; using the process fallback for '{path}'") { }
		}

		/// <summary>
		/// The native decoder can select only one coded stream. Apple tiled HEIF/HEIC photos
		/// expose the real picture as an assembled stream group, so native decoding would hash
		/// one tile or an auxiliary depth/gain-map stream instead of the photo (#869).
		/// </summary>
		internal static void ThrowIfTiledHeifRequiresProcess(VideoStreamDecoder decoder, string path) {
			if (decoder.HasStreamGroups && FileUtils.IsHeifImageFile(path))
				throw new TiledHeifRequiresProcessException(path);
		}

		/// <summary>
		/// Process fallback for Apple tiled HEIF/HEIC. FFmpeg 8.1+ assembles the primary
		/// tile-grid through a complex graph; applying a normal -vf to the implicit output
		/// fails with "Simple and complex filtering cannot be used together". Address the
		/// primary stream group explicitly as [0:g:0] and put VDF's filter chain into that
		/// same complex graph, matching upstream's #869 fix.
		/// </summary>
		internal static byte[]? TryGetTiledHeifGridFrame(
			FfmpegSettings settings,
			bool isGrayByte,
			bool isRgbFrame,
			int graySideLength,
			int expectedGrayBytes,
			int expectedRgbBytes,
			int timeoutMilliseconds,
			out string error) {
			error = string.Empty;
			if (!FileUtils.IsHeifImageFile(settings.File))
				return null;

			string? userVfFilter = null;
			var remainingCustomArgs = new List<string>();
			if (!string.IsNullOrWhiteSpace(CustomFFArguments)) {
				List<string> tokens = TokenizeArgs(CustomFFArguments);
				for (int ti = 0; ti < tokens.Count; ti++) {
					if ((tokens[ti] == "-vf" || tokens[ti] == "-filter:v") && ti + 1 < tokens.Count)
						userVfFilter = tokens[++ti];
					else
						remainingCustomArgs.Add(tokens[ti]);
				}
			}

			string filterChain;
			var outputArgs = new List<string>();
			if (isRgbFrame) {
				int side = global::VDF.Core.AI.OnnxEmbedder.InputSide;
				filterChain = $"scale={side}:{side}:flags=bicubic,format=rgb24";
				outputArgs.AddRange(["-f", "rawvideo", "-pix_fmt", "rgb24"]);
			}
			else if (isGrayByte) {
				filterChain = $"scale={graySideLength}:{graySideLength}:flags=bicubic,format=gray";
				if (userVfFilter != null)
					filterChain = $"{userVfFilter},{filterChain}";
				outputArgs.AddRange(["-f", "rawvideo", "-pix_fmt", "gray"]);
			}
			else {
				string? vfChain = BuildSarNormalizationFilter(settings.File);
				if (settings.Fullsize != 1) {
					int maxW = settings.MaxWidth > 0 ? settings.MaxWidth : 100;
					string resizeFilter = $"scale=min({maxW}\\,iw):min({maxW}\\,ih):force_original_aspect_ratio=decrease";
					vfChain = vfChain == null ? resizeFilter : $"{vfChain},{resizeFilter}";
				}
				if (userVfFilter != null)
					vfChain = vfChain == null ? userVfFilter : $"{vfChain},{userVfFilter}";
				filterChain = vfChain ?? "null";
				int quality = settings.JpegQuality > 0 ? settings.JpegQuality : DefaultJpegQuality;
				outputArgs.AddRange([
					"-f", "mjpeg",
					"-q:v", Math.Clamp(2 + (100 - quality) / 10, 2, 31).ToString(CultureInfo.InvariantCulture)
				]);
			}

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
			psi.ArgumentList.Add(FFToolsUtils.LongPathFix(settings.File));
			psi.ArgumentList.Add("-filter_complex");
			psi.ArgumentList.Add($"[0:g:0]{filterChain}[vdf]");
			psi.ArgumentList.Add("-map");
			psi.ArgumentList.Add("[vdf]");
			psi.ArgumentList.Add("-frames:v");
			psi.ArgumentList.Add("1");
			foreach (string argument in outputArgs)
				psi.ArgumentList.Add(argument);
			if (!isRgbFrame)
				foreach (string argument in remainingCustomArgs)
					psi.ArgumentList.Add(argument);
			psi.ArgumentList.Add("pipe:1");

			using var process = new Process { StartInfo = psi };
			try {
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				using var output = new MemoryStream();
				Task copyTask = process.StandardOutput.BaseStream.CopyToAsync(output);
				Task<string> errorTask = process.StandardError.ReadToEndAsync();
				Task waitTask = process.WaitForExitAsync();
				Task allTasks = Task.WhenAll(copyTask, errorTask, waitTask);
				int effectiveTimeoutMilliseconds = Math.Clamp(timeoutMilliseconds, 250, TimeoutDuration);
				if (!allTasks.Wait(effectiveTimeoutMilliseconds)) {
					try { if (!process.HasExited) process.Kill(); } catch { }
					error = $"FFmpeg tiled-HEIF grid retry timed out after {effectiveTimeoutMilliseconds}ms on file: {settings.File}";
					return null;
				}
				allTasks.GetAwaiter().GetResult();
				error = errorTask.Result;
				if (process.ExitCode != 0) {
					error = $"{error}{Environment.NewLine}FFmpeg tiled-HEIF grid retry exited with: {process.ExitCode}";
					return null;
				}

				byte[] data = output.ToArray();
				if (data.Length == 0) {
					error = $"{error}{Environment.NewLine}FFmpeg tiled-HEIF grid retry produced zero bytes";
					return null;
				}
				if (isGrayByte && data.Length != expectedGrayBytes) {
					error = $"{error}{Environment.NewLine}graybytes length != {expectedGrayBytes} (got {data.Length})";
					return null;
				}
				if (isRgbFrame && data.Length != expectedRgbBytes) {
					error = $"{error}{Environment.NewLine}AI frame length != {expectedRgbBytes} (got {data.Length})";
					return null;
				}
				return data;
			}
			catch (Exception ex) {
				try { if (!process.HasExited) process.Kill(); } catch { }
				error = ex.Message;
				return null;
			}
		}

		internal static byte[]? TryGetTiledHeifGridAiRgb224(
			string file,
			int timeoutMilliseconds,
			out string error) {
			int side = global::VDF.Core.AI.OnnxEmbedder.InputSide;
			return TryGetTiledHeifGridFrame(
				new FfmpegSettings {
					File = file,
					Rgb224 = true,
					SoftwareDecodeOnly = true,
				},
				isGrayByte: false,
				isRgbFrame: true,
				graySideLength: 32,
				expectedGrayBytes: 32 * 32,
				expectedRgbBytes: side * side * 3,
				timeoutMilliseconds,
				out error);
		}
	}
}
