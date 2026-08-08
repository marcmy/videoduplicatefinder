using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static partial class FfmpegEngine {
		// AI-enabled scans need both a 32x32 gray frame and a 224x224 RGB frame at
		// the same sample positions. Decode each position once through the native
		// binding and derive both outputs from that same source frame. This avoids
		// the old second FFmpeg process and its fresh random seek into open-GOP H.264.
		internal static bool GetGrayBytesFromVideo(
			FileEntry videoFile,
			List<float> positions,
			double maxSamplingDurationSeconds,
			bool extendedLogging,
			Action<int>? onSampleComplete,
			global::VDF.Core.AI.IEmbeddingFrameSink? embeddingSink) {
			if (embeddingSink == null)
				return GetGrayBytesFromVideo(
					videoFile,
					positions,
					maxSamplingDurationSeconds,
					extendedLogging,
					onSampleComplete);

			List<double> initiallyMissingGray = new();
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (!videoFile.grayBytes.TryGetValue(position, out byte[]? bytes) || bytes == null)
					initiallyMissingGray.Add(position);
			}

			bool skipRemainingAiProcessRetries = false;
			if (ShouldUseNativeBinding) {
				bool nativeSucceeded = TryExtractGrayAndAiNativeBatch(
					videoFile,
					positions,
					maxSamplingDurationSeconds,
					embeddingSink,
					extendedLogging,
					out FfmpegErrorCategory nativeFailureCategory,
					out AVHWDeviceType nativeFailureHardwareDeviceType);
				if (!nativeSucceeded &&
					ShouldSkipProcessRetryForCorruptFile(
						nativeFailureCategory,
						nativeFailureHardwareDeviceType)) {
					bool allGrayAvailable = positions.All(position => {
						double key = videoFile.GetGrayBytesIndex(
							position, maxSamplingDurationSeconds);
						return videoFile.grayBytes.TryGetValue(key, out byte[]? bytes) &&
							bytes != null;
					});
					if (!allGrayAvailable) {
						videoFile.Flags.Set(EntryFlags.ThumbnailError);
						Logger.Instance.Info(
							$"Skipping process-mode retry for '{videoFile.Path}': the native software decode failure indicates a truncated or corrupt file, and required gray samples are still missing.");
						return false;
					}
					// Gray matching remains valid; abstain from missing AI embeddings rather
					// than grinding the same damaged bitstream through one process per sample.
					skipRemainingAiProcessRetries = true;
				}
			}

			string? hardwareFamilyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);
			string? hardwareCodecName = GetPrimaryVideoCodecName(videoFile);
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				bool needGray =
					!videoFile.grayBytes.TryGetValue(position, out byte[]? currentGray) ||
					currentGray == null;
				bool needRgb =
					!skipRemainingAiProcessRetries &&
					embeddingSink.WantsEmbedding(videoFile, position);

				// Upstream optimization: when both outputs are missing, split one decoded
				// frame into the 32x32 gray sample and 224x224 RGB embedding instead of
				// launching FFmpeg twice. The perf implementation keeps bounded process I/O,
				// tiled-HEIF handling and H.264 accurate-seek recovery. User -vf arguments
				// intentionally stay on the existing separate path so embedding inputs remain
				// uniformly unfiltered.
				if (needGray && needRgb && string.IsNullOrWhiteSpace(CustomFFArguments)) {
					(byte[]? combinedGray, byte[]? combinedRgb) =
						GetGrayAndRgb224CliBounded(
							videoFile.Path,
							TimeSpan.FromSeconds(position),
							softwareDecodeOnly: false,
							extendedLogging);
					if (combinedGray != null) {
						videoFile.grayBytes[position] = combinedGray;
						videoFile.PHashes[position] =
							pHash.PerceptualHash.ComputePHashFromGray32x32(combinedGray);

						if (combinedRgb != null) {
							embeddingSink.SubmitFrame(videoFile, position, combinedRgb);
						}
						else {
							// Preserve the old safe RGB-only fallback if the split output could
							// not produce an embedding frame. Gray work is still retained.
							byte[]? rgbRetry = GetAiRgb224Cli(
								videoFile.Path,
								TimeSpan.FromSeconds(position),
								softwareDecodeOnly: false,
								extendedLogging);
							if (rgbRetry != null)
								embeddingSink.SubmitFrame(videoFile, position, rgbRetry);
						}

						onSampleComplete?.Invoke(i + 1);
						continue;
					}
				}

				if (needGray) {
					byte[]? gray = GetThumbnail(new FfmpegSettings {
						File = videoFile.Path,
						Position = TimeSpan.FromSeconds(position),
						GrayScale = 1,
						HardwareFamilyKey = hardwareFamilyKey,
						HardwareCodecName = hardwareCodecName,
					}, extendedLogging);
					if (gray == null) {
						videoFile.Flags.Set(EntryFlags.ThumbnailError);
						return false;
					}
					videoFile.grayBytes[position] = gray;
					videoFile.PHashes[position] =
						pHash.PerceptualHash.ComputePHashFromGray32x32(gray);
				}

				if (needRgb) {
					byte[]? rgb = GetAiRgb224Cli(
						videoFile.Path,
						TimeSpan.FromSeconds(position),
						softwareDecodeOnly: false,
						extendedLogging);
					if (rgb != null)
						embeddingSink.SubmitFrame(videoFile, position, rgb);
				}

				onSampleComplete?.Invoke(i + 1);
			}

			if (initiallyMissingGray.Count > 0 &&
				initiallyMissingGray.All(position =>
					videoFile.grayBytes.TryGetValue(position, out byte[]? bytes) &&
					bytes != null &&
					!GrayBytesUtils.VerifyGrayScaleValues(bytes))) {
				videoFile.Flags.Set(EntryFlags.TooDark);
				Logger.Instance.Info($"ERROR: Graybytes too dark of: {videoFile.Path}");
				return false;
			}

			return true;
		}

		static List<GrayByteRequest> GetPendingAiNativeRequests(
			FileEntry videoFile,
			List<float> positions,
			double maxSamplingDurationSeconds,
			global::VDF.Core.AI.IEmbeddingFrameSink embeddingSink) {
			var requests = new Dictionary<double, GrayByteRequest>();
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(
					positions[i],
					maxSamplingDurationSeconds);
				bool needGray =
					!videoFile.grayBytes.TryGetValue(position, out byte[]? bytes) ||
					bytes == null;
				bool needEmbedding =
					embeddingSink.WantsEmbedding(videoFile, position);
				if (needGray || needEmbedding)
					requests[position] =
						new GrayByteRequest(
							position,
							TimeSpan.FromSeconds(position));
			}
			return requests.Values
				.OrderBy(request => request.Position)
				.ToList();
		}

		static unsafe bool TryExtractGrayAndAiNativeBatch(
			FileEntry videoFile,
			List<float> positions,
			double maxSamplingDurationSeconds,
			global::VDF.Core.AI.IEmbeddingFrameSink embeddingSink,
			bool extendedLogging,
			out FfmpegErrorCategory failureCategory,
			out AVHWDeviceType failureHardwareDeviceType,
			bool forceCpuDecode = false) {
			failureCategory = FfmpegErrorCategory.Unknown;
			failureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			List<GrayByteRequest> requests =
				GetPendingAiNativeRequests(
					videoFile,
					positions,
					maxSamplingDurationSeconds,
					embeddingSink);
			if (requests.Count == 0)
				return true;

			string? familyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);
			string? codecName = GetPrimaryVideoCodecName(videoFile);
			AVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;
			try {
				if (!forceCpuDecode &&
					!ShouldBypassHardwareDecodeForCodec(
						codecName,
						out _,
						familyKey))
					hardwareDeviceType = GetConfiguredHardwareDeviceType();

				FfmpegLogCapture.Reset();
				using var vsd =
					new VideoStreamDecoder(videoFile.Path, hardwareDeviceType);
				ThrowIfTiledHeifRequiresProcess(vsd, videoFile.Path);
				VideoFrameConverter? grayConverter = null;
				VideoFrameConverter? rgbConverter = null;
				Size converterSourceSize = default;
				AVPixelFormat converterSourcePixelFormat =
					AVPixelFormat.AV_PIX_FMT_NONE;
				try {
					void ConsumeFrame(GrayByteRequest request, AVFrame frame) {
						bool needGray =
							!videoFile.grayBytes.TryGetValue(
								request.Index,
								out byte[]? currentGray) ||
							currentGray == null;
						bool needEmbedding =
							embeddingSink.WantsEmbedding(
								videoFile,
								request.Index);
						if (!needGray && !needEmbedding)
							return;

						Size sourceSize = new(
							frame.width > 0
								? frame.width
								: vsd.FrameSize.Width,
							frame.height > 0
								? frame.height
								: vsd.FrameSize.Height);
						if (sourceSize.Width <= 0 ||
							sourceSize.Height <= 0)
							throw new Exception(
								$"Invalid source frame dimensions " +
								$"{sourceSize.Width}x{sourceSize.Height}.");

						AVPixelFormat sourcePixelFormat =
							GetConvertiblePixelFormat(vsd, frame);
						if (!IsValidPixelFormat(sourcePixelFormat))
							throw new Exception(
								$"Invalid source pixel format " +
								$"{sourcePixelFormat}");

						if (sourceSize != converterSourceSize ||
							sourcePixelFormat !=
								converterSourcePixelFormat) {
							grayConverter?.Dispose();
							rgbConverter?.Dispose();
							grayConverter = null;
							rgbConverter = null;
							converterSourceSize = sourceSize;
							converterSourcePixelFormat =
								sourcePixelFormat;
						}

						if (needGray) {
							grayConverter ??= new VideoFrameConverter(
								sourceSize,
								sourcePixelFormat,
								new Size(32, 32),
								AVPixelFormat.AV_PIX_FMT_GRAY8,
								VideoFrameConverter.ScaleQuality.FastBilinear,
								false);
							byte[] gray =
								ExtractGray32FromFrame(
									grayConverter.Convert(frame));
							videoFile.grayBytes[request.Index] = gray;
							videoFile.PHashes[request.Index] =
								pHash.PerceptualHash
									.ComputePHashFromGray32x32(gray);
						}

						if (needEmbedding) {
							int side =
								global::VDF.Core.AI.OnnxEmbedder.InputSide;
							rgbConverter ??= new VideoFrameConverter(
								sourceSize,
								sourcePixelFormat,
								new Size(side, side),
								AVPixelFormat.AV_PIX_FMT_RGB24,
								VideoFrameConverter.ScaleQuality.Bicubic,
								false);
							byte[] rgb =
								ExtractPackedRgbFrame(
									rgbConverter.Convert(frame),
									side);
							embeddingSink.SubmitFrame(
								videoFile,
								request.Index,
								rgb);
						}
					}

					List<GrayByteRequestCluster> clusters =
						BuildGrayByteRequestClusters(requests);
					foreach (GrayByteRequestCluster cluster in clusters) {
						if (cluster.Requests.Count > 1) {
							int clusterIndex = 0;
							var decodePositions =
								cluster.Requests
									.Select(request => request.Position)
									.ToList();
							bool decoded =
								vsd.TryDecodeFrames(
									decodePositions,
									(position, frame, timing) => {
										GrayByteRequest request =
											cluster.Requests[clusterIndex++];
										ConsumeFrame(request, frame);
									},
									out _,
									FrameTransferMode
										.TransferHardwareFrame);
							if (!decoded ||
								clusterIndex != cluster.Requests.Count)
								throw new Exception(
									$"Native AI batch decoded " +
									$"{clusterIndex} of " +
									$"{cluster.Requests.Count} " +
									$"requested sample(s).");
						}
						else {
							GrayByteRequest request =
								cluster.Requests[0];
							if (!vsd.TryDecodeFrame(
								out AVFrame frame,
								request.Position,
								out _,
								FrameTransferMode
									.TransferHardwareFrame))
								throw new Exception(
									$"TryDecodeFrame failed at " +
									$"pos={request.Position} for " +
									$"'{videoFile.Path}'.");
							ConsumeFrame(request, frame);
						}
					}
				}
				finally {
					grayConverter?.Dispose();
					rgbConverter?.Dispose();
				}

				if (hardwareDeviceType !=
						AVHWDeviceType.AV_HWDEVICE_TYPE_NONE &&
					vsd.IsHardwareDecode)
					RecordHardwareDecodeSuccessForCodec(
						codecName,
						familyKey);
				RecordNativeSuccess();
				return true;
			}
			catch (Exception ex) {
				string diagnostics = FfmpegLogCapture.GetRecent();
				failureCategory = FfmpegErrorClassifier.Categorize(
					diagnostics.Length > 0 ? $"{diagnostics} {ex.Message}" : ex.Message);
				failureHardwareDeviceType = hardwareDeviceType;
				if (ex is TiledHeifRequiresProcessException) {
					Logger.Instance.Info(ex.Message);
					return false;
				}
				if (IsNativeBindingLoadFailure(ex)) {
					RecordNativeFailure(videoFile.Path, ex);
					return false;
				}

				string failureText =
					$"{hardwareDeviceType} {ex}";
				if (!forceCpuDecode &&
					hardwareDeviceType !=
						AVHWDeviceType.AV_HWDEVICE_TYPE_NONE &&
					IsHardwareDecodeFailure(failureText)) {
					MarkConfiguredHardwareDecodeFailure(failureText);
					RecordHardwareDecodeFailureForCodec(
						codecName,
						failureText,
						familyKey);
					Logger.Instance.Info(
						$"Native FFmpeg gray+AI extraction hit a " +
						$"hardware decode failure on " +
						$"'{videoFile.Path}', retrying remaining " +
						$"samples with CPU decode. Reason: " +
						$"{NormalizeLogReason(ex.Message, 240)}");
					return TryExtractGrayAndAiNativeBatch(
						videoFile,
						positions,
						maxSamplingDurationSeconds,
						embeddingSink,
						extendedLogging,
						out failureCategory,
						out failureHardwareDeviceType,
						forceCpuDecode: true);
				}

				RecordNativeFailure(videoFile.Path, ex);
				if (extendedLogging)
					Logger.Instance.Info(
						$"Native FFmpeg gray+AI batch failed on " +
						$"'{videoFile.Path}', falling back for " +
						$"remaining samples. Reason: {ex}");
				return false;
			}
		}

		static unsafe byte[] ExtractPackedRgbFrame(
			AVFrame convertedFrame,
			int sideLength) {
			if (convertedFrame.width != sideLength ||
				convertedFrame.height != sideLength)
				throw new Exception(
					$"Unexpected RGB size " +
					$"{convertedFrame.width}x{convertedFrame.height}, " +
					$"expected {sideLength}x{sideLength}.");
			if (convertedFrame.data[0] == null)
				throw new Exception(
					"Converted RGB frame has no data[0] (null).");

			int rowBytes = sideLength * 3;
			if (convertedFrame.linesize[0] < rowBytes)
				throw new Exception(
					$"Invalid RGB linesize " +
					$"({convertedFrame.linesize[0]}) for " +
					$"{rowBytes}-byte rows.");

			byte[] output = new byte[rowBytes * sideLength];
			fixed (byte* destination = output) {
				byte* source = convertedFrame.data[0];
				for (int y = 0; y < sideLength; y++)
					Buffer.MemoryCopy(
						source + y * convertedFrame.linesize[0],
						destination + y * rowBytes,
						rowBytes,
						rowBytes);
			}
			return output;
		}

		internal static bool ContainsH264ReferenceRecoveryWarning(
			string error) =>
			error.Contains(
				"co located POCs unavailable",
				StringComparison.OrdinalIgnoreCase) ||
			error.Contains(
				"mmco: unref short failure",
				StringComparison.OrdinalIgnoreCase);

		internal static List<string> BuildAiRgb224CliArguments(
			string file,
			TimeSpan position,
			bool softwareDecodeOnly,
			bool accurateSeek) {
			int side = global::VDF.Core.AI.OnnxEmbedder.InputSide;
			var arguments = new List<string> {
				"-hide_banner",
				"-loglevel",
				"error",
				"-nostdin",
			};
			if (HardwareAccelerationMode !=
					FFHardwareAccelerationMode.none &&
				!softwareDecodeOnly) {
				arguments.Add("-hwaccel");
				arguments.Add(HardwareAccelerationMode.ToString());
			}

			bool isImage = FileUtils.IsImageFile(file);
			string seek =
				position.ToString(null, CultureInfo.InvariantCulture);
			if (!isImage && !accurateSeek) {
				arguments.Add("-ss");
				arguments.Add(seek);
			}
			arguments.Add("-i");
			arguments.Add(FFToolsUtils.LongPathFix(file));
			if (!isImage && accurateSeek) {
				arguments.Add("-ss");
				arguments.Add(seek);
			}
			arguments.Add("-vf");
			arguments.Add(
				$"scale={side}:{side}:flags=bicubic,format=rgb24");
			arguments.Add("-frames:v");
			arguments.Add("1");
			arguments.Add("-f");
			arguments.Add("rawvideo");
			arguments.Add("-pix_fmt");
			arguments.Add("rgb24");
			arguments.Add("pipe:1");
			return arguments;
		}

		static (byte[]? Data, string Error) RunAiRgb224CliAttempt(
			string file,
			TimeSpan position,
			bool softwareDecodeOnly,
			bool accurateSeek) {
			int side = global::VDF.Core.AI.OnnxEmbedder.InputSide;
			int expectedBytes = side * side * 3;
			var psi = new ProcessStartInfo {
				FileName = FFmpegPath,
				CreateNoWindow = true,
				RedirectStandardInput = false,
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				WorkingDirectory = Path.GetDirectoryName(FFmpegPath)!,
				WindowStyle = ProcessWindowStyle.Hidden
			};
			foreach (string argument in BuildAiRgb224CliArguments(
				file,
				position,
				softwareDecodeOnly,
				accurateSeek))
				psi.ArgumentList.Add(argument);

			using var process = new Process { StartInfo = psi };
			try {
				process.Start();
				FFToolsUtils.LowerChildPriority(process);
				using var output = new MemoryStream();
				Task copyTask =
					process.StandardOutput.BaseStream.CopyToAsync(output);
				Task<string> errorTask =
					process.StandardError.ReadToEndAsync();
				Task waitTask = process.WaitForExitAsync();
				Task allTasks =
					Task.WhenAll(copyTask, errorTask, waitTask);
				if (!allTasks.Wait(TimeoutDuration)) {
					try {
						if (!process.HasExited)
							process.Kill();
					}
					catch { }
					return (
						null,
						$"FFmpeg timed out on file: {file}");
				}
				allTasks.GetAwaiter().GetResult();
				string error = errorTask.Result;
				if (process.ExitCode != 0)
					return (
						null,
						$"{error}{Environment.NewLine}" +
						$"FFmpeg exited with: {process.ExitCode}");
				byte[] data = output.ToArray();
				if (data.Length != expectedBytes)
					return (
						null,
						$"{error}{Environment.NewLine}" +
						$"AI frame length != {expectedBytes} " +
						$"(got {data.Length})");
				return (data, error);
			}
			catch (Exception ex) {
				try {
					if (!process.HasExited)
						process.Kill();
				}
				catch { }
				return (null, ex.Message);
			}
		}

		static byte[]? GetAiRgb224Cli(
			string file,
			TimeSpan position,
			bool softwareDecodeOnly,
			bool extendedLogging) {
			(byte[]? Data, string Error) fast =
				RunAiRgb224CliAttempt(
					file,
					position,
					softwareDecodeOnly,
					accurateSeek: false);
			bool referenceRecoveryWarning =
				ContainsH264ReferenceRecoveryWarning(fast.Error);
			if (fast.Data != null && !referenceRecoveryWarning) {
				if (extendedLogging &&
					!string.IsNullOrWhiteSpace(fast.Error))
					Logger.Instance.Info(
						$"FFmpeg AI extraction for '{file}': " +
						$"{fast.Error}");
				return fast.Data;
			}

			(byte[]? Data, string Error) accurate =
				RunAiRgb224CliAttempt(
					file,
					position,
					softwareDecodeOnly,
					accurateSeek: true);
			if (accurate.Data != null) {
				if (extendedLogging && referenceRecoveryWarning)
					Logger.Instance.Info(
						$"FFmpeg AI extraction for '{file}' " +
						$"recovered with accurate output-side " +
						$"seeking after H.264 reference warnings.");
				else if (extendedLogging &&
					!string.IsNullOrWhiteSpace(accurate.Error))
					Logger.Instance.Info(
						$"FFmpeg AI extraction for '{file}': " +
						$"{accurate.Error}");
				return accurate.Data;
			}

			string tiledHeifError = string.Empty;
			if (FileUtils.IsHeifImageFile(file)) {
				byte[]? tiledHeif = TryGetTiledHeifGridAiRgb224(
					file,
					TimeoutDuration,
					out tiledHeifError);
				if (tiledHeif != null) {
					if (extendedLogging && !string.IsNullOrWhiteSpace(tiledHeifError))
						Logger.Instance.Info($"FFmpeg tiled-HEIF AI extraction for '{file}': {tiledHeifError}");
					return tiledHeif;
				}
			}

			string error =
				string.Join(
					Environment.NewLine,
					new[] { fast.Error, accurate.Error, tiledHeifError }
						.Where(value =>
							!string.IsNullOrWhiteSpace(value)));
			if (!string.IsNullOrWhiteSpace(error))
				Logger.Instance.Info(
					$"FFmpeg AI extraction failed for " +
					$"'{file}': {error}");
			return null;
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

		// Retained for API/test compatibility. Production AI scans now use the native
		// combined-frame path above and the RGB-only safe-seek fallback.
		internal static (byte[]? GrayBytes, byte[]? Rgb224) GetGrayAndRgb224Cli(
			string file,
			TimeSpan position,
			bool softwareDecodeOnly,
			bool extendedLogging) =>
			GetGrayAndRgb224CliBounded(
				file,
				position,
				softwareDecodeOnly,
				extendedLogging);
	}
}
