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

using System.Diagnostics;
using FFmpeg.AutoGen;

namespace VDF.Core.FFTools.FFmpegNative {
	readonly struct D3D11GrayByteScaleTiming {
		public D3D11GrayByteScaleTiming(long filterMs, long tinyConvertMs, long mapMs, long copyMs, int tinyDownloads) {
			FilterMs = filterMs;
			TinyConvertMs = tinyConvertMs;
			MapMs = mapMs;
			CopyMs = copyMs;
			TinyDownloads = tinyDownloads;
		}

		public long FilterMs { get; }
		public long TinyConvertMs { get; }
		public long MapMs { get; }
		public long CopyMs { get; }
		public int TinyDownloads { get; }
	}

	sealed unsafe class D3D11GrayByteScaler : IDisposable {
		const int GraySize = 32;
		const int AVBuffersrcFlagKeepRef = 8;
		readonly AVRational _streamTimeBase;
		readonly AVRational _streamSampleAspectRatio;
		AVFilterGraph* _graph;
		AVFilterContext* _sourceContext;
		AVFilterContext* _sinkContext;
		AVFrame* _filteredFrame;
		VideoFrameConverter? _tinyConverter;
		Size _tinyConverterSourceSize;
		AVPixelFormat _tinyConverterSourcePixelFormat = AVPixelFormat.AV_PIX_FMT_NONE;
		GraphSignature _signature;

		readonly struct GraphSignature {
			public GraphSignature(int width, int height, AVPixelFormat format, AVPixelFormat downloadPixelFormat, AVBufferRef* hwFramesContext, AVRational sampleAspectRatio) {
				Width = width;
				Height = height;
				Format = format;
				DownloadPixelFormat = downloadPixelFormat;
				HwFramesContext = hwFramesContext;
				SampleAspectRatio = sampleAspectRatio;
			}

			public int Width { get; }
			public int Height { get; }
			public AVPixelFormat Format { get; }
			public AVPixelFormat DownloadPixelFormat { get; }
			public AVBufferRef* HwFramesContext { get; }
			public AVRational SampleAspectRatio { get; }

			public string Describe(string scaleArgs, string downloadFormatName, string bufferArgs) =>
				$"source={Width}x{Height}, sourceFormat={Format}({GetPixelFormatNameOrEnum(Format)}), downloadFormat={DownloadPixelFormat}({downloadFormatName}), " +
				$"sar={SampleAspectRatio.num}/{SampleAspectRatio.den}, hwFramesCtx=0x{((nint)HwFramesContext).ToString("X")}, scaleArgs='{scaleArgs}', bufferArgs='{bufferArgs}'";

			public bool Equals(GraphSignature other) =>
				Width == other.Width
				&& Height == other.Height
				&& Format == other.Format
				&& DownloadPixelFormat == other.DownloadPixelFormat
				&& HwFramesContext == other.HwFramesContext
				&& SampleAspectRatio.num == other.SampleAspectRatio.num
				&& SampleAspectRatio.den == other.SampleAspectRatio.den;
		}

		public D3D11GrayByteScaler(AVRational streamTimeBase, AVRational streamSampleAspectRatio) {
			_streamTimeBase = Normalize(streamTimeBase);
			_streamSampleAspectRatio = Normalize(streamSampleAspectRatio);
			_filteredFrame = ffmpeg.av_frame_alloc();
			if (_filteredFrame == null)
				throw new FFInvalidExitCodeException("Failed to allocate D3D11 graybyte filter output frame.");
		}

		public static bool IsAvailable(out string reason) {
			try {
				if (!FFmpegHelper.DoFFmpegLibraryFilesExist) {
					reason = "native FFmpeg libraries not available";
					return false;
				}
				FFmpegHelper.AddOptionalFilterLibraryVersionMapEntries();

				string[] filters = { "buffer", "buffersink", "scale_d3d11", "hwdownload", "format" };
				foreach (string filter in filters) {
					if (ffmpeg.avfilter_get_by_name(filter) == null) {
						reason = $"FFmpeg filter '{filter}' is unavailable";
						return false;
					}
				}

				reason = string.Empty;
				return true;
			}
			catch (Exception ex) {
				reason = $"FFmpeg filter APIs unavailable: {ex.Message}";
				return false;
			}
		}

		public byte[] ScaleToGray32(AVFrame sourceFrame, out D3D11GrayByteScaleTiming timing) {
			if (!VideoStreamDecoder.IsHardwareFrame(sourceFrame))
				throw new FFInvalidExitCodeException($"D3D11 graybyte scaler requires a hardware frame, got {(AVPixelFormat)sourceFrame.format}.");
			if (sourceFrame.hw_frames_ctx == null)
				throw new FFInvalidExitCodeException("D3D11 graybyte scaler requires frame->hw_frames_ctx.");

			GraphSignature signature = GetSignature(sourceFrame);
			if (_graph == null || !_signature.Equals(signature))
				BuildGraph(signature);

			ffmpeg.av_frame_unref(_filteredFrame);
			AVFrame frameToFilter = sourceFrame;
			var phaseSw = Stopwatch.StartNew();
			ffmpeg.av_buffersrc_add_frame_flags(_sourceContext, &frameToFilter, AVBuffersrcFlagKeepRef).ThrowExceptionIfError("av_buffersrc_add_frame_flags(d3d11 graybytes)");
			int error = ffmpeg.av_buffersink_get_frame(_sinkContext, _filteredFrame);
			if (error == ffmpeg.AVERROR(ffmpeg.EAGAIN))
				throw new FFInvalidExitCodeException("D3D11 graybyte filter graph produced no frame yet.");
			error.ThrowExceptionIfError("av_buffersink_get_frame(d3d11 graybytes)");
			long filterMs = phaseSw.ElapsedMilliseconds;

			AVFrame grayFrame = *_filteredFrame;
			long tinyConvertMs = 0;
			if ((AVPixelFormat)_filteredFrame->format != AVPixelFormat.AV_PIX_FMT_GRAY8 || _filteredFrame->width != GraySize || _filteredFrame->height != GraySize) {
				Size sourceSize = new(_filteredFrame->width, _filteredFrame->height);
				AVPixelFormat sourcePixelFormat = (AVPixelFormat)_filteredFrame->format;
				if (_tinyConverter == null || sourceSize != _tinyConverterSourceSize || sourcePixelFormat != _tinyConverterSourcePixelFormat) {
					_tinyConverter?.Dispose();
					_tinyConverter = new VideoFrameConverter(sourceSize, sourcePixelFormat, new Size(GraySize, GraySize), AVPixelFormat.AV_PIX_FMT_GRAY8, VideoFrameConverter.ScaleQuality.FastBilinear, false);
					_tinyConverterSourceSize = sourceSize;
					_tinyConverterSourcePixelFormat = sourcePixelFormat;
				}

				phaseSw.Restart();
				grayFrame = _tinyConverter.Convert(*_filteredFrame);
				tinyConvertMs = phaseSw.ElapsedMilliseconds;
			}

			phaseSw.Restart();
			byte[] data = Gray32FrameExtractor.Extract(grayFrame);
			long copyMs = phaseSw.ElapsedMilliseconds;
			timing = new D3D11GrayByteScaleTiming(filterMs, tinyConvertMs, 0, copyMs, 1);
			return data;
		}

		void BuildGraph(GraphSignature signature) {
			ReleaseGraph();
			BuildGraphForDownloadFormat(signature);
			_signature = signature;
		}

		void BuildGraphForDownloadFormat(GraphSignature signature) {
			AVFilterGraph* graph = ffmpeg.avfilter_graph_alloc();
			if (graph == null)
				throw new FFInvalidExitCodeException("Failed to allocate D3D11 graybyte filter graph.");

			try {
				AVFilterContext* source = null;
				AVFilterContext* scale = null;
				AVFilterContext* download = null;
				AVFilterContext* format = null;
				AVFilterContext* sink = null;

				CreateBufferSourceFilter(graph, &source, signature);
				string downloadPixelFormatName = GetPixelFormatName(signature.DownloadPixelFormat);
				string scaleArgs = $"width={GraySize}:height={GraySize}:format={downloadPixelFormatName}";
				string bufferArgs = GetBufferSourceArgs(signature);
				string graphDescription = signature.Describe(scaleArgs, downloadPixelFormatName, bufferArgs);

				CreateFilter(graph, &scale, "scale_d3d11", "vdf_d3d11_graybytes_scale", scaleArgs);
				CreateFilter(graph, &download, "hwdownload", "vdf_d3d11_graybytes_download", null);
				CreateFilter(graph, &format, "format", "vdf_d3d11_graybytes_download_format", $"pix_fmts={downloadPixelFormatName}");
				CreateFilter(graph, &sink, "buffersink", "vdf_d3d11_graybytes_sink", null);

				ffmpeg.avfilter_link(source, 0, scale, 0).ThrowExceptionIfError("avfilter_link(buffer -> scale_d3d11)");
				ffmpeg.avfilter_link(scale, 0, download, 0).ThrowExceptionIfError("avfilter_link(scale_d3d11 -> hwdownload)");
				ffmpeg.avfilter_link(download, 0, format, 0).ThrowExceptionIfError("avfilter_link(hwdownload -> format)");
				ffmpeg.avfilter_link(format, 0, sink, 0).ThrowExceptionIfError("avfilter_link(format -> buffersink)");
				int graphConfigError = ffmpeg.avfilter_graph_config(graph, null);
				if (graphConfigError < 0)
					throw new FFInvalidExitCodeException($"avfilter_graph_config(d3d11 graybytes) failed: {FFmpegHelper.Av_strerror(graphConfigError) ?? "Unknown error"} ({graphConfigError}); {graphDescription}");

				_graph = graph;
				_sourceContext = source;
				_sinkContext = sink;
			}
			catch {
				ffmpeg.avfilter_graph_free(&graph);
				throw;
			}
		}

		static void CreateFilter(AVFilterGraph* graph, AVFilterContext** context, string filterName, string instanceName, string? args) {
			AVFilter* filter = ffmpeg.avfilter_get_by_name(filterName);
			if (filter == null)
				throw new FFInvalidExitCodeException($"FFmpeg filter '{filterName}' is unavailable.");
			ffmpeg.avfilter_graph_create_filter(context, filter, instanceName, args, null, graph).ThrowExceptionIfError($"avfilter_graph_create_filter({filterName})");
		}

		void CreateBufferSourceFilter(AVFilterGraph* graph, AVFilterContext** context, GraphSignature signature) {
			AVFilter* filter = ffmpeg.avfilter_get_by_name("buffer");
			if (filter == null)
				throw new FFInvalidExitCodeException("FFmpeg filter 'buffer' is unavailable.");

			*context = ffmpeg.avfilter_graph_alloc_filter(graph, filter, "vdf_d3d11_graybytes_src");
			if (*context == null)
				throw new FFInvalidExitCodeException("Failed to allocate D3D11 graybyte buffer source filter.");

			SetBufferSourceParameters(*context, signature);
			string bufferArgs = GetBufferSourceArgs(signature);
			int initError = ffmpeg.avfilter_init_str(*context, bufferArgs);
			if (initError < 0)
				throw new FFInvalidExitCodeException($"avfilter_init_str(buffer) failed: {FFmpegHelper.Av_strerror(initError) ?? "Unknown error"}; bufferArgs='{bufferArgs}'");
		}

		void SetBufferSourceParameters(AVFilterContext* source, GraphSignature signature) {
			AVBufferSrcParameters* parameters = ffmpeg.av_buffersrc_parameters_alloc();
			if (parameters == null)
				throw new FFInvalidExitCodeException("Failed to allocate AVBufferSrcParameters for D3D11 graybytes.");

			try {
				parameters->format = (int)signature.Format;
				parameters->width = signature.Width;
				parameters->height = signature.Height;
				parameters->time_base = _streamTimeBase;
				parameters->sample_aspect_ratio = signature.SampleAspectRatio;
				parameters->hw_frames_ctx = ffmpeg.av_buffer_ref(signature.HwFramesContext);
				if (parameters->hw_frames_ctx == null)
					throw new FFInvalidExitCodeException("Failed to reference D3D11 hw_frames_ctx for filter graph.");
				ffmpeg.av_buffersrc_parameters_set(source, parameters).ThrowExceptionIfError("av_buffersrc_parameters_set(d3d11 graybytes)");
			}
			finally {
				if (parameters->hw_frames_ctx != null) {
					AVBufferRef* hwFramesContext = parameters->hw_frames_ctx;
					ffmpeg.av_buffer_unref(&hwFramesContext);
					parameters->hw_frames_ctx = null;
				}
				ffmpeg.av_free(parameters);
			}
		}

		GraphSignature GetSignature(AVFrame sourceFrame) {
			AVRational sampleAspectRatio = Normalize(sourceFrame.sample_aspect_ratio);
			if (sampleAspectRatio.num == 1 && sampleAspectRatio.den == 1)
				sampleAspectRatio = _streamSampleAspectRatio;
			return new GraphSignature(sourceFrame.width, sourceFrame.height, (AVPixelFormat)sourceFrame.format, GetDownloadPixelFormat(sourceFrame.hw_frames_ctx), sourceFrame.hw_frames_ctx, sampleAspectRatio);
		}

		string GetBufferSourceArgs(GraphSignature signature) {
			AVRational timeBase = _streamTimeBase;
			AVRational sampleAspectRatio = signature.SampleAspectRatio;
			return $"video_size={signature.Width}x{signature.Height}:pix_fmt={(int)signature.Format}:time_base={timeBase.num}/{timeBase.den}:pixel_aspect={sampleAspectRatio.num}/{sampleAspectRatio.den}";
		}

		static AVPixelFormat GetDownloadPixelFormat(AVBufferRef* hwFramesContextRef) {
			if (hwFramesContextRef == null || hwFramesContextRef->data == null)
				throw new FFInvalidExitCodeException("D3D11 graybyte scaler requires a populated AVHWFramesContext.");

			AVHWFramesContext* hwFramesContext = (AVHWFramesContext*)hwFramesContextRef->data;
			AVPixelFormat swFormat = hwFramesContext->sw_format;
			if (swFormat == AVPixelFormat.AV_PIX_FMT_NONE)
				throw new FFInvalidExitCodeException("D3D11 graybyte scaler could not determine the hardware frame download pixel format.");
			if (swFormat != AVPixelFormat.AV_PIX_FMT_NV12 && swFormat != AVPixelFormat.AV_PIX_FMT_P010LE && swFormat != AVPixelFormat.AV_PIX_FMT_P010BE)
				throw new FFInvalidExitCodeException($"D3D11 graybyte scaler does not support hardware frame download pixel format {swFormat}.");
			return swFormat;
		}

		static string GetPixelFormatName(AVPixelFormat pixelFormat) {
			string pixelFormatName = GetPixelFormatNameOrEnum(pixelFormat);
			if (pixelFormatName == pixelFormat.ToString())
				throw new FFInvalidExitCodeException($"D3D11 graybyte scaler could not resolve pixel format name for {pixelFormat}.");
			return pixelFormatName;
		}

		static string GetPixelFormatNameOrEnum(AVPixelFormat pixelFormat) {
			string? pixelFormatName = ffmpeg.av_get_pix_fmt_name(pixelFormat);
			return string.IsNullOrWhiteSpace(pixelFormatName) ? pixelFormat.ToString() : pixelFormatName;
		}

		static AVRational Normalize(AVRational value) {
			if (value.num <= 0 || value.den <= 0)
				return new AVRational { num = 1, den = 1 };
			return value;
		}

		void ReleaseGraph() {
			if (_graph != null) {
				AVFilterGraph* graph = _graph;
				ffmpeg.avfilter_graph_free(&graph);
				_graph = null;
				_sourceContext = null;
				_sinkContext = null;
			}
			_tinyConverter?.Dispose();
			_tinyConverter = null;
			_tinyConverterSourceSize = default;
			_tinyConverterSourcePixelFormat = AVPixelFormat.AV_PIX_FMT_NONE;
		}

		public void Dispose() {
			ReleaseGraph();
			if (_filteredFrame != null) {
				AVFrame* filteredFrame = _filteredFrame;
				ffmpeg.av_frame_free(&filteredFrame);
				_filteredFrame = null;
			}
		}
	}
}
