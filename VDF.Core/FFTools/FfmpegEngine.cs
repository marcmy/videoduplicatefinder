using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using FFmpeg.AutoGen;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Processing;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	internal static class FfmpegEngine {
		public static readonly string FFmpegPath;
		const int TimeoutDuration = 15_000;
		public static FFHardwareAccelerationMode HardwareAccelerationMode;
		public static string CustomFFArguments = string.Empty;
		public static bool UseNativeBinding;
		private static readonly SixLabors.ImageSharp.Formats.Jpeg.JpegEncoder jpegEncoder = new();
		static FfmpegEngine() => FFmpegPath = FFToolsUtils.GetPath(FFToolsUtils.FFTool.FFmpeg) ?? string.Empty;

		static void LogNativeTiming(string file, TimeSpan position, bool isGrayByte, bool hwDecode, long openMs, long decodeMs, long convertMs, long copyMs, long totalMs) {
			Logger.Instance.Info($"Native FFmpeg timing on '{file}' @ {position}: mode={(isGrayByte ? "gray32" : "thumb")}, hw={(hwDecode ? "on" : "off")}, open={openMs}ms, decode={decodeMs}ms, convert={convertMs}ms, copy={copyMs}ms, total={totalMs}ms");
		}

		static AVHWDeviceType GetConfiguredHardwareDeviceType(bool enableHardwareAcceleration = true) {
			if (!enableHardwareAcceleration)
				return AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;

			return HardwareAccelerationMode switch {
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
		}

		static unsafe byte[] ExtractGray32FromFrame(AVFrame convertedFrame) {
			const int N = 32;
			int width = convertedFrame.width;
			int height = convertedFrame.height;
			if (width != N || height != N) throw new Exception($"Unexpected size {width}x{height}, expected {N}.");
			if (convertedFrame.data[0] == null) throw new Exception("Converted frame has no data[0] (null).");
			if (convertedFrame.linesize[0] < width) throw new Exception($"Invalid linesize ({convertedFrame.linesize[0]}) for width {width}.");
			int srcStride = convertedFrame.linesize[0];
			byte[] outBuf = new byte[width * height];
			fixed (byte* destPtr = outBuf) {
				byte* sourcePtr = convertedFrame.data[0];
				for (int y = 0; y < height; y++)
					Buffer.MemoryCopy(sourcePtr + (y * srcStride), destPtr + (y * width), width, width);
			}
			return outBuf;
		}

		static int CountMissingGrayBytePositions(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds) {
			int missing = 0;
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (!videoFile.grayBytes.ContainsKey(position)) missing++;
			}
			return missing;
		}

		static unsafe byte[] ExtractGrayBytesWithDecoder(VideoStreamDecoder vsd, string filePath, TimeSpan position, ref VideoFrameConverter? converter, ref Size converterSourceSize, ref AVPixelFormat converterSourcePixelFormat, long openMs) {
			var totalSw = Stopwatch.StartNew();
			var phaseSw = Stopwatch.StartNew();

			if (!vsd.TryDecodeFrame(out var srcFrame, position))
				throw new Exception($"TryDecodeFrame failed at pos={position} for '{filePath}'. size={vsd.FrameSize.Width}x{vsd.FrameSize.Height}");
			long decodeMs = phaseSw.ElapsedMilliseconds;

			Size sourceSize = new(srcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width, srcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);
			if (sourceSize.Width <= 0 || sourceSize.Height <= 0)
				throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");

			AVPixelFormat srcPixFmt = vsd.IsHardwareDecode ? (AVPixelFormat)srcFrame.format : vsd.PixelFormat;
			if (srcPixFmt < 0 || srcPixFmt >= AVPixelFormat.AV_PIX_FMT_NB)
				throw new Exception($"Invalid source pixel format {srcPixFmt}");

			if (converter == null || sourceSize != converterSourceSize || srcPixFmt != converterSourcePixelFormat) {
				converter?.Dispose();
				converter = new VideoFrameConverter(sourceSize, srcPixFmt, new Size(32, 32), AVPixelFormat.AV_PIX_FMT_GRAY8, VideoFrameConverter.ScaleQuality.FastBilinear, false);
				converterSourceSize = sourceSize;
				converterSourcePixelFormat = srcPixFmt;
			}

			phaseSw.Restart();
			AVFrame convertedFrame = converter.Convert(srcFrame);
			long convertMs = phaseSw.ElapsedMilliseconds;

			phaseSw.Restart();
			byte[] outBuf = ExtractGray32FromFrame(convertedFrame);
			long copyMs = phaseSw.ElapsedMilliseconds;

			LogNativeTiming(filePath, position, true, vsd.IsHardwareDecode, openMs, decodeMs, convertMs, copyMs, totalSw.ElapsedMilliseconds);
			return outBuf;
		}

		static unsafe bool TryGetGrayBytesFromVideoNativeBatch(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds, ref int tooDarkCounter) {
			try {
				var openSw = Stopwatch.StartNew();
				using var vsd = new VideoStreamDecoder(videoFile.Path, GetConfiguredHardwareDeviceType(enableHardwareAcceleration: false));
				long openMs = openSw.ElapsedMilliseconds;
				VideoFrameConverter? converter = null;
				Size converterSourceSize = default;
				AVPixelFormat converterSourcePixelFormat = AVPixelFormat.AV_PIX_FMT_NONE;
				try {
					for (int i = 0; i < positions.Count; i++) {
						double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
						if (videoFile.grayBytes.ContainsKey(position)) continue;
						byte[] data = ExtractGrayBytesWithDecoder(vsd, videoFile.Path, TimeSpan.FromSeconds(position), ref converter, ref converterSourceSize, ref converterSourcePixelFormat, openMs);
						if (!GrayBytesUtils.VerifyGrayScaleValues(data)) tooDarkCounter++;
						videoFile.grayBytes.Add(position, data);
						videoFile.PHashes.Add(position, pHash.PerceptualHash.ComputePHashFromGray32x32(data));
					}
				}
				finally {
					converter?.Dispose();
				}
				Logger.Instance.Info($"Native FFmpeg batched graybyte extraction completed for '{videoFile.Path}' using hw={(vsd.IsHardwareDecode ? "on" : "off")}.");
				return true;
			}
			catch (Exception e) {
				Logger.Instance.Info($"Native FFmpeg batched graybyte extraction failed on '{videoFile.Path}', falling back to per-sample path. Exception: {e}");
				return false;
			}
		}

		public static unsafe byte[]? GetThumbnail(FfmpegSettings settings, bool extendedLogging) {
			const int N = 32;
			const int ExpectedBytes = N * N;
			bool isGrayByte = settings.GrayScale == 1;
			bool enableHardwareAcceleration = !isGrayByte;

			try {
				if (UseNativeBinding) {
					var totalSw = Stopwatch.StartNew();
					long openMs = 0, decodeMs = 0, convertMs = 0, copyMs = 0;
					var phaseSw = Stopwatch.StartNew();
					using var vsd = new VideoStreamDecoder(settings.File, GetConfiguredHardwareDeviceType(enableHardwareAcceleration));
					openMs = phaseSw.ElapsedMilliseconds;

					Size sourceSize = vsd.FrameSize;
					phaseSw.Restart();
					if (!vsd.TryDecodeFrame(out var srcFrame, settings.Position))
						throw new Exception($"TryDecodeFrame failed at pos={settings.Position} for '{settings.File}'. size={sourceSize.Width}x{sourceSize.Height}");
					decodeMs = phaseSw.ElapsedMilliseconds;

					AVPixelFormat srcPixFmt = vsd.IsHardwareDecode ? (AVPixelFormat)srcFrame.format : vsd.PixelFormat;
					if (srcPixFmt < 0 || srcPixFmt >= AVPixelFormat.AV_PIX_FMT_NB) throw new Exception($"Invalid source pixel format {srcPixFmt}");
					if (sourceSize.Width <= 0 || sourceSize.Height <= 0) throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");

					Size destinationSize = isGrayByte
						? new Size(N, N)
						: settings.Fullsize == 1
							? sourceSize
							: new Size(100, Convert.ToInt32(sourceSize.Height * (100 / (double)sourceSize.Width)));

					AVPixelFormat destinationPixelFrmt = isGrayByte ? AVPixelFormat.AV_PIX_FMT_GRAY8 : AVPixelFormat.AV_PIX_FMT_BGRA;

					phaseSw.Restart();
					using var vfc = new VideoFrameConverter(sourceSize, srcPixFmt, destinationSize, destinationPixelFrmt, isGrayByte ? VideoFrameConverter.ScaleQuality.FastBilinear : VideoFrameConverter.ScaleQuality.Bicubic, false);
					AVFrame convertedFrame = vfc.Convert(srcFrame);
					convertMs = phaseSw.ElapsedMilliseconds;

					phaseSw.Restart();
					if (isGrayByte) {
						byte[] outBuf = ExtractGray32FromFrame(convertedFrame);
						copyMs = phaseSw.ElapsedMilliseconds;
						LogNativeTiming(settings.File, settings.Position, true, vsd.IsHardwareDecode, openMs, decodeMs, convertMs, copyMs, totalSw.ElapsedMilliseconds);
						return outBuf;
					}
					else {
						int width = convertedFrame.width;
						int height = convertedFrame.height;
						if (convertedFrame.data[0] == null) throw new Exception("Converted frame has no data[0] (null).");
						if (width <= 0 || height <= 0) throw new Exception($"Invalid converted frame dimensions {width}x{height}.");
						long totalBytesLong = (long)width * height * 4;
						if (totalBytesLong > 200_000_000) throw new Exception($"Frame too large: {width}x{height} ({totalBytesLong} bytes).");
						var totalBytes = (int)totalBytesLong;
						var rgbaBytes = new byte[totalBytes];
						int stride = convertedFrame.linesize[0];
						if (stride < width * 4) throw new Exception($"Invalid stride ({stride}) for width {width}.");
						fixed (byte* destPtr = rgbaBytes) {
							byte* sourcePtr = convertedFrame.data[0];
							if (stride == width * 4) Buffer.MemoryCopy(sourcePtr, destPtr, totalBytes, totalBytes);
							else {
								int byteWidth = width * 4;
								for (int y = 0; y < height; y++)
									Buffer.MemoryCopy(sourcePtr + (y * stride), destPtr + (y * byteWidth), byteWidth, byteWidth);
							}
						}
						copyMs = phaseSw.ElapsedMilliseconds;
						LogNativeTiming(settings.File, settings.Position, false, vsd.IsHardwareDecode, openMs, decodeMs, convertMs, copyMs, totalSw.ElapsedMilliseconds);
						var image = Image.LoadPixelData<SixLabors.ImageSharp.PixelFormats.Bgra32>(rgbaBytes, width, height);
						using MemoryStream stream = new();
						image.Save(stream, jpegEncoder);
						return stream.ToArray();
					}
				}
			}
			catch (Exception e) {
				Logger.Instance.Info($"Failed using native FFmpeg binding on '{settings.File}', try switching to process mode. Exception: {e}");
			}

			var psi = new ProcessStartInfo {
				FileName = FFmpegPath,
				CreateNoWindow = true,
				RedirectStandardInput = false,
				RedirectStandardOutput = true,
				WorkingDirectory = Path.GetDirectoryName(FFmpegPath)!,
				RedirectStandardError = extendedLogging,
				WindowStyle = ProcessWindowStyle.Hidden
			};

			psi.ArgumentList.Add("-hide_banner");
			psi.ArgumentList.Add("-loglevel"); psi.ArgumentList.Add(extendedLogging ? "error" : "quiet");
			psi.ArgumentList.Add("-nostdin");

			if (enableHardwareAcceleration && HardwareAccelerationMode != FFHardwareAccelerationMode.none) {
				psi.ArgumentList.Add("-hwaccel");
				psi.ArgumentList.Add(HardwareAccelerationMode.ToString());
			}

			psi.ArgumentList.Add("-ss"); psi.ArgumentList.Add(settings.Position.ToString(null, CultureInfo.InvariantCulture));
			psi.ArgumentList.Add("-i"); psi.ArgumentList.Add(FFToolsUtils.LongPathFix(settings.File));

			string? userVfFilter = null;
			var remainingCustomArgs = new List<string>();
			if (!string.IsNullOrWhiteSpace(CustomFFArguments)) {
				var tokens = TokenizeArgs(CustomFFArguments);
				for (int ti = 0; ti < tokens.Count; ti++) {
					if ((tokens[ti] == "-vf" || tokens[ti] == "-filter:v") && ti + 1 < tokens.Count) userVfFilter = tokens[++ti];
					else remainingCustomArgs.Add(tokens[ti]);
				}
			}

			if (isGrayByte) {
				string vfChain = $"scale={N}:{N}:flags=bicubic,format=gray";
				if (userVfFilter != null) vfChain = $"{userVfFilter},{vfChain}";
				psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(vfChain);
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("rawvideo");
				psi.ArgumentList.Add("-pix_fmt"); psi.ArgumentList.Add("gray");
			}
			else {
				if (settings.Fullsize != 1) {
					string vfChain = "scale=100:-1";
					if (userVfFilter != null) vfChain = $"{vfChain},{userVfFilter}";
					psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(vfChain);
				}
				else if (userVfFilter != null) {
					psi.ArgumentList.Add("-vf"); psi.ArgumentList.Add(userVfFilter);
				}
				psi.ArgumentList.Add("-f"); psi.ArgumentList.Add("mjpeg");
			}

			psi.ArgumentList.Add("-frames:v"); psi.ArgumentList.Add("1");
			foreach (var item in remainingCustomArgs) psi.ArgumentList.Add(item);
			psi.ArgumentList.Add("pipe:1");

			using var process = new Process { StartInfo = psi };
			string errOut = string.Empty;
			byte[]? bytes = null;
			try {
				process.EnableRaisingEvents = true;
				process.Start();
				if (extendedLogging) {
					process.ErrorDataReceived += (sender, e) => {
						if (e.Data?.Length > 0) errOut += Environment.NewLine + e.Data;
					};
					process.BeginErrorReadLine();
				}
				using var ms = new MemoryStream();
				process.StandardOutput.BaseStream.CopyTo(ms);

				if (!process.WaitForExit(TimeoutDuration)) throw new TimeoutException($"FFmpeg timed out on file: {settings.File}");
				else if (extendedLogging) process.WaitForExit();

				if (process.ExitCode != 0) throw new FFInvalidExitCodeException($"FFmpeg exited with: {process.ExitCode}");

				bytes = ms.ToArray();
				if (bytes.Length == 0) bytes = null;
				else if (isGrayByte && bytes.Length != ExpectedBytes) {
					errOut += $"{Environment.NewLine}graybytes length != {ExpectedBytes} (got {bytes.Length})";
					bytes = null;
				}
			}
			catch (Exception e) {
				errOut += $"{Environment.NewLine}{e.Message}";
				try {
					if (!process.HasExited) process.Kill();
				}
				catch { }
				bytes = null;
			}
			if (bytes == null || errOut.Length > 0) {
				string message = $"{(bytes == null ? "ERROR: Failed to retrieve" : "WARNING: Problems while retrieving")} {(isGrayByte ? "graybytes" : "thumbnail")} from: {settings.File}";
				if (extendedLogging) {
					var args = string.Join(" ", psi.ArgumentList);
					message += $":{Environment.NewLine}{FFmpegPath} {args}";
				}
				Logger.Instance.Info($"{message}{errOut}");
			}
			return bytes;
		}

		internal static bool GetGrayBytesFromVideo(FileEntry videoFile, List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging) {
			int missingPositions = CountMissingGrayBytePositions(videoFile, positions, maxSamplingDurationSeconds);
			if (missingPositions == 0) return true;

			int tooDarkCounter = 0;
			if (UseNativeBinding && TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, ref tooDarkCounter)) {
				if (tooDarkCounter == missingPositions) {
					videoFile.Flags.Set(EntryFlags.TooDark);
					Logger.Instance.Info($"ERROR: Graybytes too dark of: {videoFile.Path}");
					return false;
				}
				return true;
			}

			missingPositions = CountMissingGrayBytePositions(videoFile, positions, maxSamplingDurationSeconds);
			if (missingPositions == 0) return true;

			tooDarkCounter = 0;
			for (int i = 0; i < positions.Count; i++) {
				double position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
				if (videoFile.grayBytes.ContainsKey(position)) continue;

				var data = GetThumbnail(new FfmpegSettings {
					File = videoFile.Path,
					Position = TimeSpan.FromSeconds(position),
					GrayScale = 1,
				}, extendedLogging);

				if (data == null) {
					videoFile.Flags.Set(EntryFlags.ThumbnailError);
					return false;
				}
				if (!GrayBytesUtils.VerifyGrayScaleValues(data)) tooDarkCounter++;
				videoFile.grayBytes.Add(position, data);
				videoFile.PHashes.Add(position, pHash.PerceptualHash.ComputePHashFromGray32x32(data));
			}
			if (tooDarkCounter == missingPositions) {
				videoFile.Flags.Set(EntryFlags.TooDark);
				Logger.Instance.Info($"ERROR: Graybytes too dark of: {videoFile.Path}");
				return false;
			}
			return true;
		}

		private static List<string> TokenizeArgs(string args) {
			var tokens = new List<string>();
			var current = new System.Text.StringBuilder();
			bool inQuotes = false;
			foreach (char c in args) {
				if (c == '"') inQuotes = !inQuotes;
				else if (c == ' ' && !inQuotes) {
					if (current.Length > 0) {
						tokens.Add(current.ToString());
						current.Clear();
					}
				}
				else current.Append(c);
			}
			if (current.Length > 0) tokens.Add(current.ToString());
			return tokens;
		}

		public static byte[]? ExtractThumbnailJpeg(string filePath, TimeSpan position, int maxWidth = 0, bool extendedLogging = false) {
			var settings = new FfmpegSettings {
				File = filePath,
				Position = position,
				GrayScale = 0,
				Fullsize = (byte)(maxWidth == 0 ? 1 : 0),
			};
			var raw = GetThumbnail(maxWidth == 0 ? settings : settings with { Fullsize = 1 }, extendedLogging);
			if (raw == null || raw.Length == 0) return null;

			if (maxWidth > 0) {
				using var ms = new MemoryStream(raw);
				using var image = Image.Load(ms);
				if (image.Width > maxWidth) {
					int h = (int)(image.Height * ((double)maxWidth / image.Width));
					image.Mutate(x => x.Resize(maxWidth, h));
				}
				using var outMs = new MemoryStream();
				image.Save(outMs, new SixLabors.ImageSharp.Formats.Jpeg.JpegEncoder { Quality = 90 });
				return outMs.ToArray();
			}
			return raw;
		}
	}

	internal struct FfmpegSettings {
		public byte GrayScale;
		public byte Fullsize;
		public string File;
		public TimeSpan Position;
	}
}
