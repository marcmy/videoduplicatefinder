using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
		/// <summary>
		/// Dense AI sampling for the visual partial-duplicate pass: decodes ONLY keyframes
		/// (<c>-skip_frame nokey</c>) and emits one 224x224 RGB24 frame per
		/// <paramref name="intervalSeconds"/> across the whole file in a single FFmpeg pass.
		/// Deliberately always the CLI, even when the native binding is enabled: a
		/// sequential keyframe sweep maps naturally onto one process run, and this pass is
		/// throughput-bound, not seek-bound. Frames stream to <paramref name="onFrame"/>
		/// as they arrive. Each callback receives an exact-size <see cref="AI.FramePool"/>
		/// buffer whose ownership transfers to the callback. Returns the number of frames
		/// delivered, or -1 on failure.
		/// </summary>
		internal static int StreamDenseAiFrames(string filePath, double intervalSeconds, int maxFrames, bool extendedLogging, Action<byte[]> onFrame, CancellationToken cancelToken = default) {
			const int frameBytes = AI.OnnxEmbedder.InputSide * AI.OnnxEmbedder.InputSide * 3;
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
			psi.ArgumentList.Add("-loglevel"); psi.ArgumentList.Add("error");
			psi.ArgumentList.Add("-nostdin");
			psi.ArgumentList.Add("-skip_frame"); psi.ArgumentList.Add("nokey");
			psi.ArgumentList.Add("-an"); psi.ArgumentList.Add("-sn"); psi.ArgumentList.Add("-dn");
			psi.ArgumentList.Add("-i"); psi.ArgumentList.Add(FFToolsUtils.LongPathFix(filePath));
			psi.ArgumentList.Add("-vf");
			psi.ArgumentList.Add(FormattableString.Invariant(
				$"fps=1/{intervalSeconds:0.###}:round=up,scale={AI.OnnxEmbedder.InputSide}:{AI.OnnxEmbedder.InputSide}:flags=bicubic,format=rgb24"));
			psi.ArgumentList.Add("-frames:v"); psi.ArgumentList.Add(maxFrames.ToString(CultureInfo.InvariantCulture));
			psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("rawvideo");
			psi.ArgumentList.Add("-pix_fmt"); psi.ArgumentList.Add("rgb24");
			psi.ArgumentList.Add("pipe:1");

			using var process = new Process { StartInfo = psi };
			string errOut = string.Empty;
			Task<int>? pendingRead = null;
			int frameCount = 0;
			try {
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				process.ErrorDataReceived += (_, e) => {
					if (e.Data?.Length > 0)
						errOut += Environment.NewLine + e.Data;
				};
				process.BeginErrorReadLine();
				Stream stdout = process.StandardOutput.BaseStream;
				int readTimeoutMs = (int)TimeSpan.FromMinutes(15).TotalMilliseconds;
				while (frameCount < maxFrames) {
					byte[] frame = AI.FramePool.Shared.Rent();
					int filled = 0;
					try {
						while (filled < frameBytes) {
							pendingRead = stdout.ReadAsync(frame, filled, frameBytes - filled, cancelToken);
							if (!pendingRead.Wait(readTimeoutMs, cancelToken))
								throw new TimeoutException($"FFmpeg timed out on file: {filePath}");
							int bytesRead = pendingRead.Result;
							pendingRead = null;
							if (bytesRead == 0)
								break;
							filled += bytesRead;
						}
					}
					catch {
						AI.FramePool.Shared.Return(frame);
						throw;
					}
					if (filled < frameBytes) {
						AI.FramePool.Shared.Return(frame);
						break;
					}
					frameCount++;
					onFrame(frame);
				}
				if (!process.WaitForExit(30_000))
					throw new TimeoutException($"FFmpeg did not exit after closing its output: {filePath}");
				process.WaitForExit();

				if (process.ExitCode != 0)
					throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}");
				if (frameCount == 0)
					throw new Exception("FFmpeg produced no frames");
				if (extendedLogging && errOut.Length > 0)
					Logger.Instance.Warn($"WARNING: Problems while dense-sampling AI frames from: {filePath}{errOut}");
				return frameCount;
			}
			catch (OperationCanceledException) {
				FFToolsUtils.KillAndDrain(process, pendingRead);
				throw;
			}
			catch (Exception e) {
				FFToolsUtils.KillAndDrain(process, pendingRead);
				string? hint = FfmpegErrorClassifier.Classify(errOut);
				Logger.Instance.Warn($"ERROR: Failed dense-sampling AI frames from: {filePath}{errOut}{Environment.NewLine}{e.Message}" +
					(hint != null ? $"{Environment.NewLine}Hint: {hint}" : string.Empty));
				return -1;
			}
		}
	}
}
