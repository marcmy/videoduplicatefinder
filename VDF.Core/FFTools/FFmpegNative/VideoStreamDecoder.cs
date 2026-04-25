// /*
//     Copyright (C) 2025 0x90d
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

using System.Collections.Generic;
using System.Diagnostics;
using FFmpeg.AutoGen;

namespace VDF.Core.FFTools.FFmpegNative {
	readonly struct DecodedFrameTiming {
		public DecodedFrameTiming(long seekMs, long decodeMs, long transferMs, int hardwareTransfers) {
			SeekMs = seekMs;
			DecodeMs = decodeMs;
			TransferMs = transferMs;
			HardwareTransfers = hardwareTransfers;
		}

		public long SeekMs { get; }
		public long DecodeMs { get; }
		public long TransferMs { get; }
		public int HardwareTransfers { get; }
	}

	enum FrameTransferMode {
		TransferHardwareFrame,
		KeepHardwareFrame
	}

	unsafe class VideoStreamDecoder : IDisposable {
		private readonly AVCodecContext* _pCodecContext;
		private readonly AVFormatContext* _pFormatContext;
		private readonly AVFrame* _pFrame;
		private readonly AVPacket* _pPacket;
		private readonly AVFrame* _pReceivedFrame;
		private readonly int _streamIndex;
		private readonly AVIOInterruptCB_callback _interruptCbDelegate;
		private readonly int _timeoutMs;
		private long _deadlineTicks;

		public VideoStreamDecoder(string url, AVHWDeviceType HWDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE, int timeoutMs = 15_000) {
			_pFormatContext = ffmpeg.avformat_alloc_context();
			if (_pFormatContext == null)
				throw new FFInvalidExitCodeException("Failed to allocate AVFormatContext.");

			// Set up an interrupt callback so FFmpeg aborts blocking I/O when the
			// timeout expires.  This lets Dispose() run normally and release the
			// file handle — unlike killing a thread, which would leak it.
			_timeoutMs = timeoutMs;
			_interruptCbDelegate = _ => Stopwatch.GetTimestamp() > _deadlineTicks ? 1 : 0;
			_pFormatContext->interrupt_callback = new AVIOInterruptCB { callback = _interruptCbDelegate };

			_pReceivedFrame = ffmpeg.av_frame_alloc();
			if (_pReceivedFrame == null)
				throw new FFInvalidExitCodeException("Failed to allocate AVFrame for received frame.");
			var pFormatContext = _pFormatContext;
			ResetTimeout();
			ffmpeg.avformat_open_input(&pFormatContext, url, null, null).ThrowExceptionIfError("avformat_open_input");
			ResetTimeout();
			ffmpeg.avformat_find_stream_info(_pFormatContext, null).ThrowExceptionIfError("avformat_find_stream_info");
			AVCodec* codec = null;

			_streamIndex = ffmpeg.av_find_best_stream(_pFormatContext,
				AVMediaType.AVMEDIA_TYPE_VIDEO, -1, -1, &codec, 0).ThrowExceptionIfError("av_find_best_stream(video)");
			_pCodecContext = ffmpeg.avcodec_alloc_context3(codec);
			if (_pCodecContext == null)
				throw new FFInvalidExitCodeException("Failed to allocate AVCodecContext.");
			if (HWDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE) {
				ResetTimeout();
				ffmpeg.av_hwdevice_ctx_create(&_pCodecContext->hw_device_ctx, HWDeviceType, null, null, 0).ThrowExceptionIfError($"av_hwdevice_ctx_create({HWDeviceType})");
			}
			ffmpeg.avcodec_parameters_to_context(_pCodecContext, _pFormatContext->streams[_streamIndex]->codecpar).ThrowExceptionIfError("avcodec_parameters_to_context");
			ResetTimeout();
			ffmpeg.avcodec_open2(_pCodecContext, codec, null).ThrowExceptionIfError("avcodec_open2");

			CodecName = ffmpeg.avcodec_get_name(codec->id);
			FrameSize = new Size(_pCodecContext->width, _pCodecContext->height);
			if (FrameSize.Width <= 0 || FrameSize.Height <= 0)
				throw new FFInvalidExitCodeException($"Invalid frame dimensions {FrameSize.Width}x{FrameSize.Height}.");
			// For HW decode we intentionally defer the source pixel format until the
			// first frame has been downloaded with av_hwframe_transfer_data — only then
			// do we know the real sw_format (e.g. P010LE for 10-bit HEVC vs NV12 for
			// 8-bit). Guessing before decode breaks 10-bit content.
			PixelFormat = HWDeviceType == AVHWDeviceType.AV_HWDEVICE_TYPE_NONE
				? _pCodecContext->pix_fmt
				: AVPixelFormat.AV_PIX_FMT_NONE;
			IsHardwareDecode = HWDeviceType != AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;

			_pPacket = ffmpeg.av_packet_alloc();
			if (_pPacket == null)
				throw new FFInvalidExitCodeException("Failed to allocate AVPacket.");
			_pFrame = ffmpeg.av_frame_alloc();
			if (_pFrame == null)
				throw new FFInvalidExitCodeException("Failed to allocate AVFrame.");
		}

		public string CodecName { get; }
		public Size FrameSize { get; }
		public AVPixelFormat PixelFormat { get; }
		public bool IsHardwareDecode { get; }
		internal AVRational StreamTimeBase => _pFormatContext->streams[_streamIndex]->time_base;
		internal AVRational StreamSampleAspectRatio => _pFormatContext->streams[_streamIndex]->sample_aspect_ratio;

		void ResetTimeout() {
			_deadlineTicks = Stopwatch.GetTimestamp() + (long)(_timeoutMs / 1000.0 * Stopwatch.Frequency);
		}

		long ToStreamPts(TimeSpan position) {
			AVRational timebase = _pFormatContext->streams[_streamIndex]->time_base;
			double ptsPerSecond = (double)timebase.den / timebase.num;
			return Convert.ToInt64(position.TotalSeconds * ptsPerSecond);
		}

		static long GetFrameTimestamp(AVFrame* frame) {
			return frame->best_effort_timestamp != ffmpeg.AV_NOPTS_VALUE
				? frame->best_effort_timestamp
				: frame->pts;
		}

		static bool FrameSatisfiesTarget(long framePts, long targetPts) {
			return framePts == ffmpeg.AV_NOPTS_VALUE || framePts >= targetPts;
		}

		static bool IsHardwarePixelFormat(AVPixelFormat pixelFormat) {
			if (pixelFormat < 0 || pixelFormat >= AVPixelFormat.AV_PIX_FMT_NB)
				return false;

			AVPixFmtDescriptor* descriptor = ffmpeg.av_pix_fmt_desc_get(pixelFormat);
			return descriptor != null && (descriptor->flags & ffmpeg.AV_PIX_FMT_FLAG_HWACCEL) != 0;
		}

		internal static bool IsHardwareFrame(AVFrame frame) => IsHardwarePixelFormat((AVPixelFormat)frame.format);

		AVFrame TransferFrameIfNeeded(out long transferMs, out int hardwareTransfers) {
			transferMs = 0;
			hardwareTransfers = 0;
			if (_pCodecContext->hw_device_ctx == null || !IsHardwarePixelFormat((AVPixelFormat)_pFrame->format))
				return *_pFrame;

			ffmpeg.av_frame_unref(_pReceivedFrame);
			var transferSw = Stopwatch.StartNew();
			ResetTimeout();
			ffmpeg.av_hwframe_transfer_data(_pReceivedFrame, _pFrame, 0).ThrowExceptionIfError($"av_hwframe_transfer_data({(AVPixelFormat)_pFrame->format})");
			transferMs = transferSw.ElapsedMilliseconds;
			hardwareTransfers = 1;
			return *_pReceivedFrame;
		}

		void SeekTo(long targetPts, out long seekMs) {
			var seekSw = Stopwatch.StartNew();
			ResetTimeout();
			if (ffmpeg.av_seek_frame(_pFormatContext, _streamIndex, targetPts, ffmpeg.AVSEEK_FLAG_BACKWARD) < 0) {
				ResetTimeout();
				ffmpeg.av_seek_frame(_pFormatContext, _streamIndex, targetPts, ffmpeg.AVSEEK_FLAG_ANY).ThrowExceptionIfError("av_seek_frame(any fallback)");
			}
			ffmpeg.avcodec_flush_buffers(_pCodecContext);
			seekMs = seekSw.ElapsedMilliseconds;
		}

		protected virtual void Dispose(bool disposing) {
			ReleaseUnmanaged();
		}

		~VideoStreamDecoder() {
			Dispose(false);
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public void ReleaseUnmanaged() {
			AVFrame* pFrame = _pFrame;
			ffmpeg.av_frame_free(&pFrame);
			AVFrame* pReceivedFrame = _pReceivedFrame;
			ffmpeg.av_frame_free(&pReceivedFrame);

			AVPacket* pPacket = _pPacket;
			ffmpeg.av_packet_free(&pPacket);

			AVCodecContext* pCodecContext = _pCodecContext;
			ffmpeg.avcodec_free_context(&pCodecContext);

			AVFormatContext* pFormatContext = _pFormatContext;
			ffmpeg.avformat_close_input(&pFormatContext);
		}

		public bool TryDecodeFrame(out AVFrame frame, TimeSpan position) {
			return TryDecodeFrame(out frame, position, out _);
		}

		internal bool TryDecodeFrame(out AVFrame frame, TimeSpan position, out DecodedFrameTiming timing) {
			return TryDecodeFrame(out frame, position, out timing, FrameTransferMode.TransferHardwareFrame);
		}

		internal bool TryDecodeFrame(out AVFrame frame, TimeSpan position, out DecodedFrameTiming timing, FrameTransferMode transferMode) {
			ffmpeg.av_frame_unref(_pFrame);
			ffmpeg.av_frame_unref(_pReceivedFrame);
			timing = new DecodedFrameTiming(0, 0, 0, 0);

			long targetPts = ToStreamPts(position);
			SeekTo(targetPts, out long seekMs);

			// Decode forward from keyframe until we reach the target PTS.
			// Cap iterations to prevent infinite loops on corrupt files.
			const int maxIterations = 10_000;
			var decodeSw = Stopwatch.StartNew();
			bool foundTargetFrame = false;
			for (int iter = 0; iter < maxIterations; iter++) {
				int error;
				do {
					ffmpeg.av_packet_unref(_pPacket);
					ResetTimeout();
					error = ffmpeg.av_read_frame(_pFormatContext, _pPacket);
					if (error == ffmpeg.AVERROR_EOF) {
						frame = *_pFrame;
						timing = new DecodedFrameTiming(seekMs, decodeSw.ElapsedMilliseconds, 0, 0);
						return false;
					}
					error.ThrowExceptionIfError("av_read_frame while decoding single frame");
				} while (_pPacket->stream_index != _streamIndex);

				try {
					ResetTimeout();
					ffmpeg.avcodec_send_packet(_pCodecContext, _pPacket).ThrowExceptionIfError("avcodec_send_packet while decoding single frame");
				}
				finally {
					ffmpeg.av_packet_unref(_pPacket);
				}

				ResetTimeout();
				error = ffmpeg.avcodec_receive_frame(_pCodecContext, _pFrame);
				if (error == ffmpeg.AVERROR(ffmpeg.EAGAIN))
					continue;
				if (error == ffmpeg.AVERROR_EOF) {
					frame = *_pFrame;
					timing = new DecodedFrameTiming(seekMs, decodeSw.ElapsedMilliseconds, 0, 0);
					return false;
				}
				if (error < 0) {
					error.ThrowExceptionIfError("avcodec_receive_frame while decoding single frame");
				}

				// Check if we've reached or passed the target position
				if (FrameSatisfiesTarget(GetFrameTimestamp(_pFrame), targetPts)) {
					foundTargetFrame = true;
					break;
				}

				// Not at target yet - discard this frame and decode the next
				ffmpeg.av_frame_unref(_pFrame);
			}

			long decodeMs = decodeSw.ElapsedMilliseconds;
			if (!foundTargetFrame) {
				frame = *_pFrame;
				timing = new DecodedFrameTiming(seekMs, decodeMs, 0, 0);
				return false;
			}

			if (transferMode == FrameTransferMode.KeepHardwareFrame) {
				frame = *_pFrame;
				timing = new DecodedFrameTiming(seekMs, decodeMs, 0, 0);
				return true;
			}

			frame = TransferFrameIfNeeded(out long transferMs, out int hardwareTransfers);
			timing = new DecodedFrameTiming(seekMs, decodeMs, transferMs, hardwareTransfers);

			return true;
		}

		public bool TryDecodeFrames(IReadOnlyList<TimeSpan> sortedPositions, Action<TimeSpan, AVFrame, DecodedFrameTiming> handleFrame, out long seekMs) {
			return TryDecodeFrames(sortedPositions, handleFrame, out seekMs, FrameTransferMode.TransferHardwareFrame);
		}

		internal bool TryDecodeFrames(IReadOnlyList<TimeSpan> sortedPositions, Action<TimeSpan, AVFrame, DecodedFrameTiming> handleFrame, out long seekMs, FrameTransferMode transferMode) {
			seekMs = 0;
			if (sortedPositions.Count == 0)
				return true;

			ffmpeg.av_frame_unref(_pFrame);
			ffmpeg.av_frame_unref(_pReceivedFrame);

			long[] targetPts = new long[sortedPositions.Count];
			for (int i = 0; i < sortedPositions.Count; i++)
				targetPts[i] = ToStreamPts(sortedPositions[i]);

			SeekTo(targetPts[0], out seekMs);

			int targetIndex = 0;
			var decodeSw = Stopwatch.StartNew();
			while (targetIndex < sortedPositions.Count) {
				int error;
				do {
					ffmpeg.av_packet_unref(_pPacket);
					ResetTimeout();
					error = ffmpeg.av_read_frame(_pFormatContext, _pPacket);
					if (error == ffmpeg.AVERROR_EOF) {
						if (DrainDecoder(sortedPositions, targetPts, handleFrame, ref targetIndex, decodeSw, seekMs, transferMode))
							return true;
						return false;
					}
					error.ThrowExceptionIfError("av_read_frame while decoding graybyte batch");
				} while (_pPacket->stream_index != _streamIndex);

				try {
					ResetTimeout();
					ffmpeg.avcodec_send_packet(_pCodecContext, _pPacket).ThrowExceptionIfError("avcodec_send_packet while decoding graybyte batch");
				}
				finally {
					ffmpeg.av_packet_unref(_pPacket);
				}

				if (DrainReceivedFrames(sortedPositions, targetPts, handleFrame, ref targetIndex, decodeSw, seekMs, transferMode))
					return true;
			}

			return true;
		}

		bool DrainDecoder(IReadOnlyList<TimeSpan> sortedPositions, long[] targetPts, Action<TimeSpan, AVFrame, DecodedFrameTiming> handleFrame, ref int targetIndex, Stopwatch decodeSw, long seekMs, FrameTransferMode transferMode) {
			ResetTimeout();
			int error = ffmpeg.avcodec_send_packet(_pCodecContext, null);
			if (error < 0 && error != ffmpeg.AVERROR_EOF)
				error.ThrowExceptionIfError("avcodec_send_packet(NULL) while draining decoder");
			return DrainReceivedFrames(sortedPositions, targetPts, handleFrame, ref targetIndex, decodeSw, seekMs, transferMode);
		}

		bool DrainReceivedFrames(IReadOnlyList<TimeSpan> sortedPositions, long[] targetPts, Action<TimeSpan, AVFrame, DecodedFrameTiming> handleFrame, ref int targetIndex, Stopwatch decodeSw, long seekMs, FrameTransferMode transferMode) {
			while (targetIndex < sortedPositions.Count) {
				ResetTimeout();
				int error = ffmpeg.avcodec_receive_frame(_pCodecContext, _pFrame);
				if (error == ffmpeg.AVERROR(ffmpeg.EAGAIN) || error == ffmpeg.AVERROR_EOF)
					return false;
				error.ThrowExceptionIfError("avcodec_receive_frame while draining decoder");

				long framePts = GetFrameTimestamp(_pFrame);
				if (FrameSatisfiesTarget(framePts, targetPts[targetIndex])) {
					long decodeMs = decodeSw.ElapsedMilliseconds;
					AVFrame frame;
					long transferMs;
					int hardwareTransfers;
					if (transferMode == FrameTransferMode.KeepHardwareFrame) {
						frame = *_pFrame;
						transferMs = 0;
						hardwareTransfers = 0;
					}
					else {
						frame = TransferFrameIfNeeded(out transferMs, out hardwareTransfers);
					}
					long emittedSeekMs = targetIndex == 0 ? seekMs : 0;
					long emittedDecodeMs = decodeMs;
					long emittedTransferMs = transferMs;
					int emittedHardwareTransfers = hardwareTransfers;
					do {
						handleFrame(sortedPositions[targetIndex], frame, new DecodedFrameTiming(emittedSeekMs, emittedDecodeMs, emittedTransferMs, emittedHardwareTransfers));
						targetIndex++;
						emittedSeekMs = 0;
						emittedDecodeMs = 0;
						emittedTransferMs = 0;
						emittedHardwareTransfers = 0;
					} while (targetIndex < sortedPositions.Count && FrameSatisfiesTarget(framePts, targetPts[targetIndex]));

					decodeSw.Restart();
				}

				ffmpeg.av_frame_unref(_pFrame);
				ffmpeg.av_frame_unref(_pReceivedFrame);
			}

			return true;
		}

	}
}
