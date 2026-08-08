using System.Diagnostics;
using System.Globalization;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
		readonly record struct CombinedAiCliResult(byte[]? GrayBytes, byte[]? Rgb224, string Error);

		/// <summary>
		/// Process fallback for AI scans that derives the 32x32 gray sample and 224x224
		/// RGB embedding input from one seek/decode. Gray is returned on stdout and RGB is
		/// written to a unique temporary rawvideo file because two raw outputs on one pipe
		/// have no stable framing contract.
		///
		/// Unlike the older upstream-compatible helper this path uses bounded stdout/error
		/// handling (#865), understands tiled HEIF stream groups (#869), and retries with
		/// output-side accurate seeking after the H.264 reference-recovery diagnostics that
		/// can make an input-side random seek produce an unreliable embedding frame.
		/// </summary>
		static (byte[]? GrayBytes, byte[]? Rgb224) GetGrayAndRgb224CliBounded(
			string file,
			TimeSpan position,
			bool softwareDecodeOnly,
			bool extendedLogging) {
			bool isImage = FileUtils.IsImageFile(file);
			string[] inputLabels = isImage && FileUtils.IsHeifImageFile(file)
				? ["0:g:0", "0:v"]
				: ["0:v"];

			CombinedAiCliResult bestFailure = default;
			foreach (string inputLabel in inputLabels) {
				CombinedAiCliResult fast = RunGrayAndRgb224CliAttempt(
					file,
					position,
					softwareDecodeOnly,
					accurateSeek: false,
					inputLabel);

				bool referenceRecoveryWarning =
					ContainsH264ReferenceRecoveryWarning(fast.Error);
				if (fast.GrayBytes != null && fast.Rgb224 != null && !referenceRecoveryWarning) {
					LogCombinedAiSuccess(file, fast.Error, extendedLogging, inputLabel, accurateSeek: false);
					return (fast.GrayBytes, fast.Rgb224);
				}

				bestFailure = PreferCombinedAiResult(bestFailure, fast);

				// Still images are not seeked. For videos, recover from broken random-access
				// reference state or a failed fast seek using output-side accurate seeking.
				if (!isImage && (referenceRecoveryWarning || fast.GrayBytes == null || fast.Rgb224 == null)) {
					CombinedAiCliResult accurate = RunGrayAndRgb224CliAttempt(
						file,
						position,
						softwareDecodeOnly,
						accurateSeek: true,
						inputLabel);
					if (accurate.GrayBytes != null && accurate.Rgb224 != null) {
						if (extendedLogging && referenceRecoveryWarning)
							Logger.Instance.Info(
								$"FFmpeg gray+AI extraction for '{file}' recovered with accurate output-side seeking after H.264 reference warnings.");
						else
							LogCombinedAiSuccess(file, accurate.Error, extendedLogging, inputLabel, accurateSeek: true);
						return (accurate.GrayBytes, accurate.Rgb224);
					}
					bestFailure = PreferCombinedAiResult(bestFailure, accurate);
				}

				// A single-stream HEIC does not expose [0:g:0]. In that case the second
				// label ([0:v]) is the intended fallback; tiled HEIF normally succeeds first.
				if (bestFailure.GrayBytes != null && bestFailure.Rgb224 != null)
					break;
			}

			if (bestFailure.GrayBytes == null || bestFailure.Rgb224 == null) {
				string what = bestFailure.GrayBytes == null ? "graybytes+AI frame" : "AI frame";
				if (extendedLogging && !string.IsNullOrWhiteSpace(bestFailure.Error))
					Logger.Instance.Info(
						$"Combined FFmpeg {what} extraction did not fully succeed for '{file}'; falling back to the existing per-output path. {bestFailure.Error}");
			}
			return (bestFailure.GrayBytes, bestFailure.Rgb224);
		}

		static CombinedAiCliResult PreferCombinedAiResult(
			CombinedAiCliResult current,
			CombinedAiCliResult candidate) {
			int CurrentScore() => (current.GrayBytes != null ? 1 : 0) + (current.Rgb224 != null ? 1 : 0);
			int CandidateScore() => (candidate.GrayBytes != null ? 1 : 0) + (candidate.Rgb224 != null ? 1 : 0);
			if (CandidateScore() > CurrentScore())
				return candidate;
			if (CandidateScore() == CurrentScore() &&
				string.IsNullOrWhiteSpace(current.Error) &&
				!string.IsNullOrWhiteSpace(candidate.Error))
				return candidate;
			return current;
		}

		static void LogCombinedAiSuccess(
			string file,
			string error,
			bool extendedLogging,
			string inputLabel,
			bool accurateSeek) {
			if (extendedLogging && !string.IsNullOrWhiteSpace(error))
				Logger.Instance.Info(
					$"FFmpeg gray+AI extraction for '{file}' via [{inputLabel}]" +
					$"{(accurateSeek ? " with accurate seek" : string.Empty)}: {error}");
		}

		static CombinedAiCliResult RunGrayAndRgb224CliAttempt(
			string file,
			TimeSpan position,
			bool softwareDecodeOnly,
			bool accurateSeek,
			string inputLabel) {
			const int graySide = 32;
			const int grayExpectedBytes = graySide * graySide;
			int rgbSide = global::VDF.Core.AI.OnnxEmbedder.InputSide;
			int rgbExpectedBytes = rgbSide * rgbSide * 3;
			string rgbTempPath = Path.Combine(
				Path.GetTempPath(),
				$"VDF.AiFrame.{Guid.NewGuid():N}.rgb");
			bool isImage = FileUtils.IsImageFile(file);

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

			string seek = position.ToString(null, CultureInfo.InvariantCulture);
			if (!isImage && !accurateSeek) {
				psi.ArgumentList.Add("-ss");
				psi.ArgumentList.Add(seek);
			}
			psi.ArgumentList.Add("-i");
			psi.ArgumentList.Add(FFToolsUtils.LongPathFix(file));
			if (!isImage && accurateSeek) {
				psi.ArgumentList.Add("-ss");
				psi.ArgumentList.Add(seek);
			}
			psi.ArgumentList.Add("-filter_complex");
			psi.ArgumentList.Add(
				$"[{inputLabel}]split=2[g][r];" +
				$"[g]scale={graySide}:{graySide}:flags=bicubic,format=gray[gout];" +
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
			string error = string.Empty;
			byte[]? gray = null;
			byte[]? rgb = null;
			try {
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				using var output = new MemoryStream();
				Task copyTask = process.StandardOutput.BaseStream.CopyToAsync(output);
				Task<string> errorTask = process.StandardError.ReadToEndAsync();
				Task waitTask = process.WaitForExitAsync();
				Task allTasks = Task.WhenAll(copyTask, errorTask, waitTask);
				if (!allTasks.Wait(TimeoutDuration)) {
					try { if (!process.HasExited) process.Kill(); } catch { }
					return new CombinedAiCliResult(
						null,
						null,
						$"FFmpeg timed out after {TimeoutDuration}ms on file: {file}");
				}
				allTasks.GetAwaiter().GetResult();
				error = errorTask.Result;
				if (process.ExitCode != 0)
					return new CombinedAiCliResult(
						null,
						null,
						$"{error}{Environment.NewLine}FFmpeg exited with: {process.ExitCode}");

				gray = output.ToArray();
				if (gray.Length != grayExpectedBytes) {
					error = $"{error}{Environment.NewLine}graybytes length != {grayExpectedBytes} (got {gray.Length})";
					gray = null;
				}
				if (File.Exists(rgbTempPath)) {
					rgb = File.ReadAllBytes(rgbTempPath);
					if (rgb.Length != rgbExpectedBytes) {
						error = $"{error}{Environment.NewLine}AI frame length != {rgbExpectedBytes} (got {rgb.Length})";
						rgb = null;
					}
				}
				else {
					error = $"{error}{Environment.NewLine}AI frame output file was not produced";
				}

				if (gray != null && isImage && !string.IsNullOrWhiteSpace(error))
					error = FilterBenignImageDemuxerNoise(error);
				return new CombinedAiCliResult(gray, rgb, error);
			}
			catch (Exception ex) {
				try { if (!process.HasExited) process.Kill(); } catch { }
				error = string.IsNullOrWhiteSpace(error)
					? ex.Message
					: $"{error}{Environment.NewLine}{ex.Message}";
				return new CombinedAiCliResult(null, null, error);
			}
			finally {
				try { if (File.Exists(rgbTempPath)) File.Delete(rgbTempPath); } catch { }
			}
		}
	}
}
