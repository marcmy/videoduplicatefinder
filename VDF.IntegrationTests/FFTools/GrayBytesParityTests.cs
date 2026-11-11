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

using VDF.Core.FFTools;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;
using VDF.IntegrationTests.Fixtures;
using FFmpeg.AutoGen;
using VDF.Core;

namespace VDF.IntegrationTests.FFTools;

[Collection("Ffmpeg")]
public class GrayBytesParityTests {
	const float GrayByteParityTolerance = 0.10f;
	readonly FfmpegFixture _fixture;
	readonly Xunit.Abstractions.ITestOutputHelper? _output;

	public GrayBytesParityTests(FfmpegFixture fixture, Xunit.Abstractions.ITestOutputHelper output) {
		_fixture = fixture;
		_output = output;
	}

	byte[]? ExtractGrayBytes(string file, bool native) {
		FfmpegEngine.UseNativeBinding = native;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		return FfmpegEngine.GetThumbnail(new FfmpegSettings {
			File = file,
			Position = TimeSpan.FromSeconds(1),
			GrayScale = 1,
		}, extendedLogging: false);
	}

	static FileEntry CreateVideoEntry(string file) =>
		new(file) {
			mediaInfo = new MediaInfo {
				Duration = TimeSpan.FromSeconds(2),
				Streams = Array.Empty<MediaInfo.StreamInfo>(),
			},
		};

	static void AssertEntryHasGrayBytes(FileEntry entry, List<float> positions) {
		Assert.Equal(positions.Count, entry.grayBytes.Count);
		Assert.Equal(positions.Count, entry.PHashes.Count);
		foreach (float relativePosition in positions) {
			double index = entry.GetGrayBytesIndex(relativePosition, maxSamplingDurationSeconds: 0);
			Assert.True(entry.grayBytes.TryGetValue(index, out byte[]? grayBytes), $"Missing graybytes at {index}s");
			Assert.NotNull(grayBytes);
			Assert.Equal(1024, grayBytes!.Length);
			Assert.True(entry.PHashes.ContainsKey(index), $"Missing pHash at {index}s");
		}
	}

	static void AssertNativeBatchMatchesProcess(string file, List<float> positions) {
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;

		FfmpegEngine.UseNativeBinding = true;
		FileEntry nativeEntry = CreateVideoEntry(file);
		Assert.True(FfmpegEngine.GetGrayBytesFromVideo(nativeEntry, positions, maxSamplingDurationSeconds: 0, extendedLogging: false));

		FfmpegEngine.UseNativeBinding = false;
		FileEntry processEntry = CreateVideoEntry(file);
		Assert.True(FfmpegEngine.GetGrayBytesFromVideo(processEntry, positions, maxSamplingDurationSeconds: 0, extendedLogging: false));

		AssertEntryHasGrayBytes(nativeEntry, positions);
		AssertEntryHasGrayBytes(processEntry, positions);
		foreach (float relativePosition in positions) {
			double index = nativeEntry.GetGrayBytesIndex(relativePosition, maxSamplingDurationSeconds: 0);
			float diff = GrayBytesUtils.PercentageDifference(nativeEntry.grayBytes[index]!, processEntry.grayBytes[index]!);
			Assert.True(diff < GrayByteParityTolerance, $"Native batch vs process graybytes differ by {diff:P2} at {index}s, expected < {GrayByteParityTolerance:P0}");
		}
	}

	static List<byte[]> ExtractD3D11GpuScaledGrayBytes(string file, List<TimeSpan> positions) {
		using var decoder = new VideoStreamDecoder(file, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		using var scaler = new D3D11VideoProcessorGrayByteScaler();
		List<byte[]> results = new();
		bool decodedAll = decoder.TryDecodeFrames(positions, (position, frame, timing) => {
			results.Add(scaler.ScaleToGray32(frame, out _));
		}, out _, FrameTransferMode.KeepHardwareFrame);

		if (!decodedAll)
			throw new Exception($"D3D11 GPU-scaled graybyte decode returned {results.Count} of {positions.Count} sample(s).");
		return results;
	}

	static void SkipIfD3D11GpuScaleUnavailable(FfmpegFixture fixture, string? file) {
		Skip.If(!OperatingSystem.IsWindows(), "D3D11VA is Windows-only");
		Skip.If(!fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(file == null, "H264 test video not generated");
	}

	[SkippableFact]
	public void GrayBytes_NativeVsProcess_H264_ProduceSimilarResults() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");

		using var guard = new FfmpegStaticStateGuard();
		var nativeBytes = ExtractGrayBytes(_fixture.H264_8bit!, native: true);
		var processBytes = ExtractGrayBytes(_fixture.H264_8bit!, native: false);

		Assert.NotNull(nativeBytes);
		Assert.NotNull(processBytes);
		Assert.Equal(1024, nativeBytes.Length);
		Assert.Equal(1024, processBytes.Length);

		float diff = GrayBytesUtils.PercentageDifference(nativeBytes, processBytes);
		Assert.True(diff < GrayByteParityTolerance,
			$"Native vs process graybytes differ by {diff:P2}, expected < {GrayByteParityTolerance:P0}");
	}

	[SkippableFact]
	public void GrayBytes_NativeVsProcess_HEVC10bit_ProduceSimilarResults() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		Skip.If(_fixture.HEVC_10bit == null, "HEVC 10-bit test video not generated (libx265 unavailable?)");

		using var guard = new FfmpegStaticStateGuard();
		var nativeBytes = ExtractGrayBytes(_fixture.HEVC_10bit!, native: true);
		var processBytes = ExtractGrayBytes(_fixture.HEVC_10bit!, native: false);

		Assert.NotNull(nativeBytes);
		Assert.NotNull(processBytes);
		Assert.Equal(1024, nativeBytes.Length);
		Assert.Equal(1024, processBytes.Length);

		float diff = GrayBytesUtils.PercentageDifference(nativeBytes, processBytes);
		Assert.True(diff < GrayByteParityTolerance,
			$"Native vs process graybytes differ by {diff:P2} for 10-bit HEVC, expected < {GrayByteParityTolerance:P0}");
	}

	[SkippableFact]
	public void GrayBytes_ProcessMode_SameInput_ProducesIdenticalOutput() {
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");

		using var guard = new FfmpegStaticStateGuard();
		var run1 = ExtractGrayBytes(_fixture.H264_8bit!, native: false);
		var run2 = ExtractGrayBytes(_fixture.H264_8bit!, native: false);

		Assert.NotNull(run1);
		Assert.NotNull(run2);
		Assert.Equal(run1, run2);
	}

	[SkippableTheory]
	[InlineData(false)] // JPEG (MJPEG): buffers its single frame, requires draining
	[InlineData(true)]  // PNG: emits immediately, guards against a seek regression
	public void GrayBytes_NativeStillImage_DecodesAndMatchesProcess(bool png) {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		string? image = png ? _fixture.SamplePng : _fixture.SampleJpeg;
		Skip.If(image == null, "Still image fixture not generated");

		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;

		// Native still-image fast path. Before the #810 fix this returned false for MJPEG
		// (the lone intra frame was never received), silently falling back to the CLI.
		FfmpegEngine.UseNativeBinding = true;
		bool ok = FfmpegEngine.TryGetImageInfoAndGrayBytes(image!, out var nativeBytes, out int w, out int h, extendedLogging: false);
		Assert.True(ok, "Native still-image decode failed");
		Assert.NotNull(nativeBytes);
		Assert.Equal(1024, nativeBytes!.Length);
		Assert.True(w > 0 && h > 0, $"Native decode reported invalid dimensions {w}x{h}");

		FfmpegEngine.UseNativeBinding = false;
		var processBytes = FfmpegEngine.GetThumbnail(new FfmpegSettings {
			File = image!, Position = TimeSpan.Zero, GrayScale = 1, SoftwareDecodeOnly = true,
		}, extendedLogging: false);
		Assert.NotNull(processBytes);

		float diff = GrayBytesUtils.PercentageDifference(nativeBytes, processBytes!);
		Assert.True(diff < 0.05f, $"Native vs process still-image graybytes differ by {diff:P2}, expected < 5%");
	}

	[SkippableFact]
	public void GrayBytes_NativeMode_SameInput_ProducesIdenticalOutput() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");

		using var guard = new FfmpegStaticStateGuard();
		var run1 = ExtractGrayBytes(_fixture.H264_8bit!, native: true);
		var run2 = ExtractGrayBytes(_fixture.H264_8bit!, native: true);

		Assert.NotNull(run1);
		Assert.NotNull(run2);
		Assert.Equal(run1, run2);
	}

	[SkippableFact]
	public void GrayBytes_NativeBatch_UnsortedPositions_ReturnsAllSamples() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");

		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		FileEntry entry = CreateVideoEntry(_fixture.H264_8bit!);
		List<float> positions = new() { 0.75f, 0.25f, 0.5f };

		Assert.True(FfmpegEngine.GetGrayBytesFromVideo(entry, positions, maxSamplingDurationSeconds: 0, extendedLogging: false));
		AssertEntryHasGrayBytes(entry, positions);
	}

	[SkippableFact]
	public void GrayBytes_NativeBatch_SameInput_ProducesIdenticalOutput() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");

		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		List<float> positions = new() { 0.75f, 0.25f, 0.5f };
		FileEntry first = CreateVideoEntry(_fixture.H264_8bit!);
		FileEntry second = CreateVideoEntry(_fixture.H264_8bit!);

		Assert.True(FfmpegEngine.GetGrayBytesFromVideo(first, positions, maxSamplingDurationSeconds: 0, extendedLogging: false));
		Assert.True(FfmpegEngine.GetGrayBytesFromVideo(second, positions, maxSamplingDurationSeconds: 0, extendedLogging: false));

		foreach (float relativePosition in positions) {
			double index = first.GetGrayBytesIndex(relativePosition, maxSamplingDurationSeconds: 0);
			Assert.Equal(first.grayBytes[index], second.grayBytes[index]);
			Assert.Equal(first.PHashes[index], second.PHashes[index]);
		}
	}

	[SkippableFact]
	public void GrayBytes_NativeBatchVsProcess_H264_ProduceSimilarResults() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");

		using var guard = new FfmpegStaticStateGuard();
		AssertNativeBatchMatchesProcess(_fixture.H264_8bit!, new List<float> { 0.75f, 0.25f, 0.5f });
	}

	[SkippableFact]
	public void GrayBytes_NativeBatchVsProcess_HEVC10bit_ProduceSimilarResults() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		Skip.If(_fixture.HEVC_10bit == null, "HEVC 10-bit test video not generated (libx265 unavailable?)");

		using var guard = new FfmpegStaticStateGuard();
		AssertNativeBatchMatchesProcess(_fixture.HEVC_10bit!, new List<float> { 0.75f, 0.25f, 0.5f });
	}

	[SkippableFact]
	public void VideoStreamDecoder_D3D11VA_BatchedDecode_ReturnsRequestedFrames_WhenAvailable() {
		Skip.If(!OperatingSystem.IsWindows(), "D3D11VA is Windows-only");
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(_fixture.H264_8bit == null, "H264 test video not generated");

		bool decodedAll;
		bool usedHardwareDecode;
		long seekMs;
		List<(int Width, int Height)> dimensions = new();
		List<TimeSpan> positions = new() {
			TimeSpan.FromSeconds(0.5),
			TimeSpan.FromSeconds(1),
			TimeSpan.FromSeconds(1.5),
		};

		try {
			using var decoder = new VideoStreamDecoder(_fixture.H264_8bit!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
			usedHardwareDecode = decoder.IsHardwareDecode;
			decodedAll = decoder.TryDecodeFrames(positions, (position, frame, timing) => {
				dimensions.Add((frame.width, frame.height));
			}, out seekMs);
		}
		catch (Exception ex) {
			Skip.If(true, $"D3D11VA native decode unavailable: {ex.Message}");
			return;
		}

		Assert.True(usedHardwareDecode);
		Assert.True(decodedAll);
		Assert.True(seekMs >= 0);
		Assert.Equal(positions.Count, dimensions.Count);
		Assert.All(dimensions, size => {
			Assert.True(size.Width > 0);
			Assert.True(size.Height > 0);
		});
	}

	[SkippableFact]
	public void D3D11GrayByteScaler_BatchedDecode_ProducesGray32Frames_WhenAvailable() {
		SkipIfD3D11GpuScaleUnavailable(_fixture, _fixture.H264_8bit);

		List<byte[]> grayBytes;
		try {
			grayBytes = ExtractD3D11GpuScaledGrayBytes(_fixture.H264_8bit!, new List<TimeSpan> {
				TimeSpan.FromSeconds(0.5),
				TimeSpan.FromSeconds(1),
				TimeSpan.FromSeconds(1.5),
			});
		}
		catch (Exception ex) {
			Skip.If(true, $"D3D11 GPU-scaled graybytes unavailable: {ex.Message}");
			return;
		}

		Assert.Equal(3, grayBytes.Count);
		Assert.All(grayBytes, bytes => {
			Assert.Equal(1024, bytes.Length);
			Assert.True(GrayBytesUtils.VerifyGrayScaleValues(bytes));
		});
	}

	[SkippableFact]
	public void D3D11GrayByteScaler_SameInput_ProducesIdenticalOutput_WhenAvailable() {
		SkipIfD3D11GpuScaleUnavailable(_fixture, _fixture.H264_8bit);

		List<TimeSpan> positions = new() {
			TimeSpan.FromSeconds(0.5),
			TimeSpan.FromSeconds(1),
			TimeSpan.FromSeconds(1.5),
		};

		List<byte[]> first;
		List<byte[]> second;
		try {
			first = ExtractD3D11GpuScaledGrayBytes(_fixture.H264_8bit!, positions);
			second = ExtractD3D11GpuScaledGrayBytes(_fixture.H264_8bit!, positions);
		}
		catch (Exception ex) {
			Skip.If(true, $"D3D11 GPU-scaled graybytes unavailable: {ex.Message}");
			return;
		}

		Assert.Equal(first.Count, second.Count);
		for (int i = 0; i < first.Count; i++)
			Assert.Equal(first[i], second[i]);
	}

	[SkippableFact]
	public void D3D11GrayByteScalerVsProcess_H264_ProduceSimilarResults_WhenAvailable() {
		SkipIfD3D11GpuScaleUnavailable(_fixture, _fixture.H264_8bit);
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);

		using var guard = new FfmpegStaticStateGuard();
		List<TimeSpan> positions = new() {
			TimeSpan.FromSeconds(0.5),
			TimeSpan.FromSeconds(1),
			TimeSpan.FromSeconds(1.5),
		};

		List<byte[]> d3d11Bytes;
		try {
			d3d11Bytes = ExtractD3D11GpuScaledGrayBytes(_fixture.H264_8bit!, positions);
		}
		catch (Exception ex) {
			Skip.If(true, $"D3D11 GPU-scaled graybytes unavailable: {ex.Message}");
			return;
		}

		FfmpegEngine.UseNativeBinding = false;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		for (int i = 0; i < positions.Count; i++) {
			byte[]? processBytes = FfmpegEngine.GetThumbnail(new FfmpegSettings {
				File = _fixture.H264_8bit!,
				Position = positions[i],
				GrayScale = 1,
			}, extendedLogging: false);
			Assert.NotNull(processBytes);
			float diff = GrayBytesUtils.PercentageDifference(d3d11Bytes[i], processBytes!);
			Assert.True(diff < GrayByteParityTolerance,
				$"D3D11 GPU-scaled vs process graybytes differ by {diff:P2} at {positions[i]}, expected < {GrayByteParityTolerance:P0}");
		}
	}

	[SkippableFact]
	public void GrayBytes_NativeBatch_D3D11GpuScale_ReturnsAllSamples_WhenAvailable() {
		SkipIfD3D11GpuScaleUnavailable(_fixture, _fixture.H264_8bit);

		using var guard = new FfmpegStaticStateGuard();
		Environment.SetEnvironmentVariable("VDF_FORCE_NATIVE_GRAYBYTE_CPU", null);
		Environment.SetEnvironmentVariable("VDF_DISABLE_NATIVE_GRAYBYTE_GPU_SCALE", null);
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
		FileEntry entry = CreateVideoEntry(_fixture.H264_8bit!);
		List<float> positions = new() { 0.75f, 0.25f, 0.5f };

		Assert.True(FfmpegEngine.GetGrayBytesFromVideo(entry, positions, maxSamplingDurationSeconds: 0, extendedLogging: true));
		AssertEntryHasGrayBytes(entry, positions);
	}

	static unsafe (List<byte[]> Gray, List<byte[]> Rgb) ExtractCpuReferenceGrayAndRgb(string file, List<TimeSpan> positions) {
		const int rgbSide = D3D11VideoProcessorGrayByteScaler.RgbSize;
		using var decoder = new VideoStreamDecoder(file, AVHWDeviceType.AV_HWDEVICE_TYPE_NONE);
		List<byte[]> gray = new();
		List<byte[]> rgb = new();
		foreach (TimeSpan position in positions) {
			Assert.True(decoder.TryDecodeFrame(out AVFrame frame, position));
			var size = new System.Drawing.Size(frame.width, frame.height);
			using var grayConverter = new VideoFrameConverter(size, (AVPixelFormat)frame.format, new System.Drawing.Size(32, 32), AVPixelFormat.AV_PIX_FMT_GRAY8);
			using var rgbConverter = new VideoFrameConverter(size, (AVPixelFormat)frame.format, new System.Drawing.Size(rgbSide, rgbSide), AVPixelFormat.AV_PIX_FMT_RGB24);
			gray.Add(CopyPacked(grayConverter.Convert(frame), 32, 1));
			rgb.Add(CopyPacked(rgbConverter.Convert(frame), rgbSide, 3));
		}
		return (gray, rgb);
	}

	static unsafe byte[] CopyPacked(AVFrame frame, int side, int bytesPerPixel) {
		byte[] output = new byte[side * side * bytesPerPixel];
		for (int y = 0; y < side; y++)
			new ReadOnlySpan<byte>(frame.data[0] + y * frame.linesize[0], side * bytesPerPixel)
				.CopyTo(output.AsSpan(y * side * bytesPerPixel));
		return output;
	}

	static (List<byte[]> Gray, List<byte[]> Rgb) ExtractD3D11GpuScaledGrayAndRgb(string file, List<TimeSpan> positions) {
		using var decoder = new VideoStreamDecoder(file, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		using var scaler = new D3D11VideoProcessorGrayByteScaler();
		List<byte[]> gray = new();
		List<byte[]> rgb = new();
		foreach (TimeSpan position in positions) {
			Assert.True(decoder.TryDecodeFrame(out AVFrame frame, position, out _, FrameTransferMode.KeepHardwareFrame));
			scaler.ScaleToGrayAndRgb(frame, gray: true, rgb: true, out byte[]? g, out byte[]? c, out _);
			gray.Add(g!);
			rgb.Add(c!);
		}
		return (gray, rgb);
	}

	static double MeanAbsoluteError(byte[] a, byte[] b) {
		Assert.Equal(a.Length, b.Length);
		long sum = 0;
		for (int i = 0; i < a.Length; i++)
			sum += Math.Abs(a[i] - b[i]);
		return (double)sum / a.Length;
	}

	// The GPU (D3D11 video processor) and CPU (swscale) paths must produce samples that
	// compare as the same frame, so files scanned on either path still match each other
	// (and the AI embedder sees the same picture).
	void AssertD3D11GrayAndRgbMatchCpu(string? file, string label) {
		SkipIfD3D11GpuScaleUnavailable(_fixture, file);
		using var guard = new FfmpegStaticStateGuard();
		List<TimeSpan> positions = new() { TimeSpan.FromSeconds(0.5), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1.5) };

		(List<byte[]> Gray, List<byte[]> Rgb) gpu;
		try {
			gpu = ExtractD3D11GpuScaledGrayAndRgb(file!, positions);
		}
		catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) {
			Skip.If(true, $"D3D11 GPU-scaled gray/RGB unavailable: {ex.Message}");
			return;
		}
		var cpu = ExtractCpuReferenceGrayAndRgb(file!, positions);

		for (int i = 0; i < positions.Count; i++) {
			double grayMae = MeanAbsoluteError(gpu.Gray[i], cpu.Gray[i]);
			double rgbMae = MeanAbsoluteError(gpu.Rgb[i], cpu.Rgb[i]);
			float grayDiff = GrayBytesUtils.PercentageDifference(gpu.Gray[i], cpu.Gray[i]);
			_output?.WriteLine($"{label} @ {positions[i]}: gray MAE={grayMae:0.00} diff={grayDiff:P2}, rgb MAE={rgbMae:0.00}");
			Assert.True(grayDiff < GrayByteParityTolerance, $"{label}: D3D11 vs CPU gray differ by {grayDiff:P2} at {positions[i]}");
			Assert.True(grayMae < 8, $"{label}: D3D11 vs CPU gray mean abs error {grayMae:0.00} at {positions[i]}");
			Assert.True(rgbMae < 10, $"{label}: D3D11 vs CPU RGB224 mean abs error {rgbMae:0.00} at {positions[i]}");
		}
	}

	[SkippableFact]
	public void D3D11GrayAndRgbScaler_H264_MatchesCpuSwscale_WhenAvailable() =>
		AssertD3D11GrayAndRgbMatchCpu(_fixture.H264_8bit, "H264");

	[SkippableFact]
	public void D3D11GrayAndRgbScaler_HEVC10bit_MatchesCpuSwscale_WhenAvailable() =>
		AssertD3D11GrayAndRgbMatchCpu(_fixture.HEVC_10bit, "HEVC10");
}

