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

using System;
using System.Globalization;
using System.Threading;
using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;

namespace VDF.Core.FFTools {
	/// <summary>What one native dense sweep did (diagnostics and measurements).</summary>
	internal sealed class DenseNativeStats {
		public int Slots;
		public int Seeks;
		public int Decodes;
		public int Duplicates;
		public long KeyframePacketBytes;
		public bool HardwareDecode;
		public bool GpuScale;
	}

	internal static partial class FfmpegEngine {
		/// <summary>Returned by <see cref="StreamDenseAiFramesNative(string, double, double, int, bool, Action{byte[]}, CancellationToken)"/>
		/// when the native sampler cannot handle a file and the caller should fall back to
		/// <see cref="StreamDenseAiFrames"/>. Frames already delivered must be discarded.</summary>
		internal const int DenseNativeUnavailable = -2;

		internal const string DisableNativeDenseAiEnvVar = "VDF_DISABLE_NATIVE_DENSE_AI";

		/// <summary>
		/// Demuxers whose backward seek is index-based (sample table, cues, idx1, ASF index), so
		/// it lands on a keyframe after reading only that keyframe's bytes. Byte-position seekers
		/// (mpegts, mpeg-ps, flv, rm, raw streams) bisect or scan instead: slower and not reliably
		/// the latest keyframe at or before the target, so those stay on the CLI sweep.
		/// </summary>
		static readonly string[] DenseNativeSeekableFormats = { "mov", "mp4", "matroska", "webm", "avi", "asf" };

		internal static bool IsDenseNativeSeekableFormat(string? formatName) {
			if (string.IsNullOrWhiteSpace(formatName))
				return false;
			foreach (string name in formatName.Split(',')) {
				string trimmed = name.Trim();
				foreach (string allowed in DenseNativeSeekableFormats)
					if (string.Equals(trimmed, allowed, StringComparison.OrdinalIgnoreCase))
						return true;
			}
			return false;
		}

		/// <summary>
		/// Decoders that honour <c>skip_frame nokey</c>, so the CLI sweep really holds the latest
		/// keyframe in each slot (checked with showinfo; mjpeg is all-intra). The VP9 decoder
		/// ignores skip_frame: the CLI decodes every frame there and each slot holds the exact
		/// frame at its time, which a keyframe seek cannot reproduce. Codecs not listed (VP9,
		/// VC-1/WMV3, anything untested) stay on the CLI so cached records stay comparable.
		/// MPEG-4 with packed B-frames in AVI is a known, accepted divergence: the CLI has no pts
		/// there and labels each keyframe with the next keyframe's time, so its grid starts one
		/// slot late; native uses the true times. The offset is consistent, so matching holds.
		/// </summary>
		static readonly string[] DenseNativeKeyframeOnlyCodecs = {
			"h264", "hevc", "mpeg4", "mpeg1video", "mpeg2video", "msmpeg4v1", "msmpeg4v2", "msmpeg4v3",
			"wmv1", "wmv2", "vp8", "av1", "mjpeg",
		};

		internal static bool IsDenseNativeKeyframeOnlyCodec(string? codecName) =>
			!string.IsNullOrWhiteSpace(codecName) &&
			Array.Exists(DenseNativeKeyframeOnlyCodecs, c => string.Equals(c, codecName.Trim(), StringComparison.OrdinalIgnoreCase));

		internal static bool ShouldUseNativeDenseAi =>
			ShouldUseNativeBinding && !IsEnvFlagEnabled(DisableNativeDenseAiEnvVar);

		/// <summary>The interval exactly as the CLI sweep hands it to the fps filter ("0.###").</summary>
		internal static double GetDenseCliInterval(double intervalSeconds) =>
			double.Parse(intervalSeconds.ToString("0.###", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

		internal static int StreamDenseAiFramesNative(string filePath, double durationSeconds, double intervalSeconds, int maxFrames,
			bool extendedLogging, Action<byte[]> onFrame, CancellationToken token) =>
			StreamDenseAiFramesNative(filePath, durationSeconds, intervalSeconds, maxFrames, extendedLogging, onFrame, token,
				codecName: null, familyKey: null, out _);

		internal static int StreamDenseAiFramesNative(FileEntry entry, double intervalSeconds, int maxFrames,
			bool extendedLogging, Action<byte[]> onFrame, CancellationToken token) =>
			StreamDenseAiFramesNative(entry.Path, entry.mediaInfo?.Duration.TotalSeconds ?? 0, intervalSeconds, maxFrames,
				extendedLogging, onFrame, token, GetPrimaryVideoCodecName(entry), GetD3D11GrayByteAdaptiveFamilyKey(entry), out _);

		/// <summary>
		/// Seek-based twin of <see cref="StreamDenseAiFrames"/>: reproduces the CLI sweep's
		/// timeline slot for slot (<c>-skip_frame nokey</c> + <c>fps=1/interval:round=up</c> puts
		/// the latest keyframe at or before each slot time into that slot, duplicating it across
		/// keyframe gaps), but reads only the sampled keyframes off disk instead of the whole
		/// file, and decodes and scales them on the GPU when D3D11VA is configured.
		/// Each frame handed to <paramref name="onFrame"/> is an exact-size <see cref="AI.FramePool"/>
		/// buffer whose ownership transfers to the callback.
		/// Returns the number of frames delivered; -1 when the callback failed (same contract as
		/// the CLI sweep); <see cref="DenseNativeUnavailable"/> when the file needs the CLI sweep
		/// (native off, unsupported container, open/seek/decode failure, no frames) - frames
		/// already delivered must then be discarded by the caller.
		/// </summary>
		internal static int StreamDenseAiFramesNative(string filePath, double durationSeconds, double intervalSeconds, int maxFrames,
			bool extendedLogging, Action<byte[]> onFrame, CancellationToken token, string? codecName, string? familyKey,
			out DenseNativeStats stats) {
			stats = new DenseNativeStats();
			if (!ShouldUseNativeDenseAi || maxFrames <= 0 || intervalSeconds <= 0)
				return DenseNativeUnavailable;

			bool hardwareAllowed = !ShouldBypassHardwareDecodeForCodec(codecName, out _, familyKey);
			DenseNativeOutcome outcome = TryStreamDenseNative(filePath, durationSeconds, intervalSeconds, maxFrames,
				extendedLogging, onFrame, token, codecName, familyKey, forceCpuDecode: !hardwareAllowed, stats);
			if (outcome == DenseNativeOutcome.RetryOnCpu) {
				stats = new DenseNativeStats();
				outcome = TryStreamDenseNative(filePath, durationSeconds, intervalSeconds, maxFrames,
					extendedLogging, onFrame, token, codecName, familyKey, forceCpuDecode: true, stats);
			}
			return outcome switch {
				DenseNativeOutcome.Success => stats.Slots,
				DenseNativeOutcome.CallbackFailed => -1,
				_ => DenseNativeUnavailable,
			};
		}

		enum DenseNativeOutcome { Success, Unavailable, RetryOnCpu, CallbackFailed }

		static unsafe DenseNativeOutcome TryStreamDenseNative(string filePath, double durationSeconds, double intervalSeconds,
			int maxFrames, bool extendedLogging, Action<byte[]> onFrame, CancellationToken token, string? codecName,
			string? familyKey, bool forceCpuDecode, DenseNativeStats stats) {
			int side = AI.OnnxEmbedder.InputSide;
			int frameBytes = side * side * 3;
			AVHWDeviceType hardwareDeviceType = forceCpuDecode
				? AVHWDeviceType.AV_HWDEVICE_TYPE_NONE
				: GetConfiguredHardwareDeviceType();
			bool callbackFailed = false;
			try {
				using IDisposable? d3d11Lease = EnterD3D11GrayByteConcurrencyLimiter(hardwareDeviceType);
				FfmpegLogCapture.Reset();
				using var vsd = new VideoStreamDecoder(filePath, hardwareDeviceType);
				if (!IsDenseNativeSeekableFormat(vsd.FormatName)) {
					if (extendedLogging)
						Logger.Instance.Info($"AI partial detection: '{filePath}' is a '{vsd.FormatName}' file, which has no index to seek keyframes by; using the full FFmpeg sweep.");
					return DenseNativeOutcome.Unavailable;
				}
				if (!IsDenseNativeKeyframeOnlyCodec(vsd.CodecName)) {
					if (extendedLogging)
						Logger.Instance.Info($"AI partial detection: '{filePath}' is {vsd.CodecName}, whose decoder does not sample keyframes only; using the full FFmpeg sweep.");
					return DenseNativeOutcome.Unavailable;
				}
				// Plain-Baseline H.264 only reaches the GPU through the profile-mismatch override, and
				// D3D11VA decodes some of it wrongly (a 68-slice phone-style stream came out garbled,
				// MAE ~38 against the CPU). This pass decodes keyframes only, cheap on the CPU, and the
				// CLI sweep it replaces always decoded on the CPU - so stay there.
				if (vsd.IsHardwareDecode && vsd.CodecName == "h264" && vsd.CodecProfile == ffmpeg.AV_PROFILE_H264_BASELINE)
					return DenseNativeOutcome.RetryOnCpu;
				stats.HardwareDecode = vsd.IsHardwareDecode;

				string gpuScalePolicy = string.Empty;
				bool useGpuScale = vsd.IsHardwareDecode && ShouldUseD3D11GrayByteGpuScale(hardwareDeviceType, ref gpuScalePolicy, out _);
				D3D11VideoProcessorGrayByteScaler? gpuScaler = null;
				VideoFrameConverter? rgbConverter = null;
				Size converterSourceSize = default;
				AVPixelFormat converterSourcePixelFormat = AVPixelFormat.AV_PIX_FMT_NONE;
				try {
					AVRational tb = vsd.StreamTimeBase;
					if (tb.num <= 0 || tb.den <= 0)
						return DenseNativeOutcome.Unavailable;
					double secondsPerTick = tb.num / (double)tb.den;
					// The CLI shifts every timestamp by the container start (all streams) and the
					// fps filter starts its grid at the first decoded frame, rounded up to the grid.
					long formatStartTicks = (long)Math.Round(vsd.FormatStartSeconds / secondsPerTick);
					double interval = GetDenseCliInterval(intervalSeconds);
					if (interval <= 0)
						return DenseNativeOutcome.Unavailable;

					long SeekKeyframe(long target, out long pts, out long dts, out long pos) {
						stats.Seeks++;
						if (!vsd.TrySeekKeyframePacket(target, out pts, out dts, out pos))
							return long.MinValue;
						stats.KeyframePacketBytes += vsd.LastKeyframePacketSize;
						return pts;
					}

					if (SeekKeyframe(vsd.StreamStartPts, out long firstPts, out _, out _) == long.MinValue ||
						firstPts == ffmpeg.AV_NOPTS_VALUE)
						return DenseNativeOutcome.Unavailable;
					vsd.DiscardPendingKeyframe();
					double firstRel = (firstPts - formatStartTicks) * secondsPerTick;
					long gridStart = Math.Max(0, (long)Math.Ceiling(firstRel / interval - 1e-9));

					// The CLI's sweep ends at its EOF timestamp: the last keyframe plus one frame.
					// Finding that keyframe costs one more seek; without it, fall back to the duration.
					double endRel = durationSeconds;
					// Well past any plausible end (the probed duration can be missing or short).
					long farTarget = formatStartTicks + (long)((Math.Max(durationSeconds, 0) * 2 + 7 * 86400) / secondsPerTick);
					if (SeekKeyframe(farTarget, out long lastPts, out _, out _) != long.MinValue && lastPts != ffmpeg.AV_NOPTS_VALUE)
						endRel = (lastPts - formatStartTicks) * secondsPerTick + vsd.NominalFrameDurationSeconds;
					vsd.DiscardPendingKeyframe();
					if (endRel <= firstRel)
						endRel = firstRel + 1e-6; // a single keyframe still yields slot 0, as in the CLI

					byte[]? lastRgb = null;
					long lastPos = long.MinValue, lastKeyPts = long.MinValue;
					for (int k = 0; k < maxFrames; k++) {
						token.ThrowIfCancellationRequested();
						double slotRel = (gridStart + k) * interval;
						if (slotRel >= endRel)
							break;
						long target = formatStartTicks + (long)Math.Floor(slotRel / secondsPerTick + 1e-6);
						if (SeekKeyframe(target, out long pts, out long dts, out long pos) == long.MinValue)
							return DenseNativeOutcome.Unavailable;
						// Index entries of B-frame streams are dts-ordered: the seek can land on a
						// keyframe that is displayed just after the target. Step back to the previous
						// keyframe, which is what the CLI's slot holds.
						for (int retry = 0; retry < 3 && pts != ffmpeg.AV_NOPTS_VALUE && pts > target; retry++) {
							long earlier = (dts != ffmpeg.AV_NOPTS_VALUE ? Math.Min(dts, pts) : pts) - 1;
							if (SeekKeyframe(earlier, out long p2, out long d2, out long pos2) == long.MinValue || p2 >= pts) {
								// No earlier keyframe: the slot keeps the one already found.
								if (SeekKeyframe(target, out pts, out dts, out pos) == long.MinValue)
									return DenseNativeOutcome.Unavailable;
								break;
							}
							pts = p2; dts = d2; pos = pos2;
						}

						bool sameKeyframe = lastRgb != null &&
							(pos >= 0 ? pos == lastPos : pts == lastKeyPts);
						byte[] rgb;
						if (sameKeyframe) {
							vsd.DiscardPendingKeyframe();
							stats.Duplicates++;
							rgb = lastRgb!;
						}
						else {
							if (!vsd.TryDecodePendingKeyframe(out AVFrame frame, out _))
								throw new FrameNotReachedException($"Keyframe at pts {pts} did not decode.");
							stats.Decodes++;
							rgb = ScaleDenseKeyframe(vsd, frame, side, ref useGpuScale, ref gpuScaler,
								ref rgbConverter, ref converterSourceSize, ref converterSourcePixelFormat, filePath);
							stats.GpuScale |= useGpuScale && gpuScaler != null;
							lastRgb = rgb;
							lastPos = pos;
							lastKeyPts = pts;
						}

						byte[] pooled = AI.FramePool.Shared.Rent();
						Buffer.BlockCopy(rgb, 0, pooled, 0, frameBytes);
						stats.Slots++;
						try {
							onFrame(pooled);
						}
						catch (OperationCanceledException) {
							throw;
						}
						catch {
							callbackFailed = true;
							throw;
						}
					}
				}
				finally {
					gpuScaler?.Dispose();
					rgbConverter?.Dispose();
				}

				if (stats.Slots == 0)
					return DenseNativeOutcome.Unavailable;
				if (hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE && vsd.IsHardwareDecode)
					RecordHardwareDecodeSuccessForCodec(codecName, familyKey);
				return DenseNativeOutcome.Success;
			}
			catch (OperationCanceledException) {
				throw;
			}
			catch (Exception ex) when (callbackFailed) {
				Logger.Instance.Warn($"ERROR: Failed dense-sampling AI frames from: {filePath}{Environment.NewLine}{ex.Message}");
				return DenseNativeOutcome.CallbackFailed;
			}
			catch (Exception ex) {
				string diagnostics = FfmpegLogCapture.GetRecent();
				if (IsNativeBindingLoadFailure(ex)) {
					RecordNativeFailure(filePath, ex);
					return DenseNativeOutcome.Unavailable;
				}
				string failureText = $"{hardwareDeviceType} {ex}";
				bool hardwareImplicated = hardwareDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE &&
					IsHardwareImplicatedNativeFailure(ex, diagnostics, failureText);
				if (hardwareImplicated) {
					RecordHardwareDecodeFailureForCodec(codecName, failureText, familyKey);
					// Frames already delivered cannot be taken back; the caller restarts on the CLI.
					if (stats.Slots == 0) {
						Logger.Instance.Info($"AI partial detection: hardware keyframe decode failed on '{filePath}', retrying on the CPU. Reason: {NormalizeLogReason(ex.Message, 240)}{FormatFfmpegDiagnostics(diagnostics)}");
						return DenseNativeOutcome.RetryOnCpu;
					}
				}
				RecordNativeFailure(filePath, ex);
				Logger.Instance.Info($"AI partial detection: native keyframe sampling failed on '{filePath}', using the full FFmpeg sweep. Reason: {NormalizeLogReason(ex.Message, 240)}{FormatFfmpegDiagnostics(diagnostics)}");
				return DenseNativeOutcome.Unavailable;
			}
		}

		/// <summary>224x224 RGB24, oriented like the CLI's autorotate, from one decoded keyframe.</summary>
		static byte[] ScaleDenseKeyframe(VideoStreamDecoder vsd, AVFrame frame, int side, ref bool useGpuScale,
			ref D3D11VideoProcessorGrayByteScaler? gpuScaler, ref VideoFrameConverter? rgbConverter,
			ref Size converterSourceSize, ref AVPixelFormat converterSourcePixelFormat, string filePath) {
			FrameOrientation orientation = vsd.GetOrientation(frame);
			if (useGpuScale && VideoStreamDecoder.IsHardwareFrame(frame)) {
				try {
					gpuScaler ??= new D3D11VideoProcessorGrayByteScaler();
					gpuScaler.ScaleToGrayAndRgb(frame, gray: false, rgb: true, out _, out byte[]? rgb, out _);
					if (rgb != null && rgb.Length == side * side * 3)
						return orientation.Apply(rgb, side, side, 3);
					throw new NotSupportedException("D3D11 video processor returned no RGB frame.");
				}
				catch (Exception gpuEx) when (
					gpuEx is NotSupportedException or FFInvalidExitCodeException or SharpGen.Runtime.SharpGenException) {
					// Keep decoding on the GPU but scale the rest of this file on the CPU.
					useGpuScale = false;
					gpuScaler?.Dispose();
					gpuScaler = null;
					Logger.Instance.Info($"D3D11 video processor scaling unavailable for '{filePath}', using CPU scaling for this file. Reason: {NormalizeLogReason(gpuEx.Message, 240)}");
				}
			}
			if (VideoStreamDecoder.IsHardwareFrame(frame))
				frame = vsd.TransferCurrentFrame();

			Size sourceSize = new(frame.width > 0 ? frame.width : vsd.FrameSize.Width,
				frame.height > 0 ? frame.height : vsd.FrameSize.Height);
			if (sourceSize.Width <= 0 || sourceSize.Height <= 0)
				throw new Exception($"Invalid source frame dimensions {sourceSize.Width}x{sourceSize.Height}.");
			AVPixelFormat sourcePixelFormat = GetConvertiblePixelFormat(vsd, frame);
			if (!IsValidPixelFormat(sourcePixelFormat))
				throw new Exception($"Invalid source pixel format {sourcePixelFormat}");
			if (rgbConverter == null || sourceSize != converterSourceSize || sourcePixelFormat != converterSourcePixelFormat) {
				rgbConverter?.Dispose();
				rgbConverter = new VideoFrameConverter(sourceSize, sourcePixelFormat, new Size(side, side),
					AVPixelFormat.AV_PIX_FMT_RGB24, VideoFrameConverter.ScaleQuality.Bicubic, false);
				converterSourceSize = sourceSize;
				converterSourcePixelFormat = sourcePixelFormat;
			}
			return orientation.Apply(ExtractPackedRgbFrame(rgbConverter.Convert(frame), side), side, side, 3);
		}
	}
}
