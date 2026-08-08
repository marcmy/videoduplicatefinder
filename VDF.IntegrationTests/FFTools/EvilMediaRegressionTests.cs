// /*
 //    Copyright (C) 2026 0x90d
 //    This file is part of VideoDuplicateFinder
 //    VideoDuplicateFinder is free software: you can redistribute it and/or modify
 //    it under the terms of the GNU Affero General Public License as published by
 //    the Free Software Foundation, either version 3 of the License, or
 //    (at your option) any later version.
 //    VideoDuplicateFinder is distributed in the hope that it will be useful,
 //    but WITHOUT ANY WARRANTY without even the implied warranty of
 //    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 //    GNU Affero General Public License for more details.
 //    You should have received a copy of the GNU Affero General Public License
 //    along with VideoDuplicateFinder.  If not, see <http://www.gnu.org/licenses/>.
 // */
 //

using System.Diagnostics;
using VDF.Core;
using VDF.Core.FFTools;
using VDF.Core.Utils;
using VDF.IntegrationTests.Fixtures;

namespace VDF.IntegrationTests.FFTools;

/// <summary>
/// Generated "evil media" coverage for decoder and seek shapes that normal happy-path
/// fixtures do not exercise. Every fixture is deterministic and recreated from an FFmpeg
/// recipe at test time; see TestData/EVIL_MEDIA.md.
/// </summary>
[Collection("Ffmpeg")]
public class EvilMediaRegressionTests : IClassFixture<EvilMediaFixture> {
	const float GrayParityTolerance = 0.12f;
	readonly EvilMediaFixture _evil;

	public EvilMediaRegressionTests(EvilMediaFixture evil) => _evil = evil;

	string? Resolve(string fixtureName) => fixtureName switch {
		"long-gop" => _evil.LongGopH264,
		"vfr" => _evil.VfrH264,
		"odd-dimensions" => _evil.OddDimensions,
		"very-short" => _evil.VeryShortH264,
		"rotation" => _evil.RotatedH264,
		"resolution-change" => _evil.ResolutionChangeH264,
		_ => null,
	};

	static byte[]? ExtractGray(string path, double seconds, bool native) {
		FfmpegEngine.UseNativeBinding = native;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		FfmpegEngine.CustomFFArguments = string.Empty;
		return FfmpegEngine.GetThumbnail(new FfmpegSettings {
			File = path,
			Position = TimeSpan.FromSeconds(seconds),
			GrayScale = 1,
			SoftwareDecodeOnly = true,
		}, extendedLogging: false);
	}

	[SkippableTheory]
	[InlineData("long-gop", 3.55)]
	[InlineData("vfr", 1.45)]
	[InlineData("odd-dimensions", 1.0)]
	[InlineData("very-short", 0.04)]
	[InlineData("rotation", 1.0)]
	[InlineData("resolution-change", 1.55)]
	public void GrayBytes_ProcessMode_PathologicalMedia_ReturnsOneGray32Frame(
		string fixtureName,
		double seconds) {
		Skip.If(!_evil.FfmpegCliAvailable, _evil.FfmpegNotFoundReason);
		string? path = Resolve(fixtureName);
		Skip.If(path == null, $"{fixtureName} fixture was not generated");

		using var guard = new FfmpegStaticStateGuard();
		byte[]? gray = ExtractGray(path!, seconds, native: false);

		Assert.NotNull(gray);
		Assert.Equal(32 * 32, gray!.Length);
	}

	[SkippableTheory]
	[InlineData("long-gop", 3.55)]
	[InlineData("vfr", 1.45)]
	[InlineData("odd-dimensions", 1.0)]
	[InlineData("very-short", 0.04)]
	[InlineData("rotation", 1.0)]
	[InlineData("resolution-change", 1.55)]
	public void GrayBytes_NativeMode_PathologicalMedia_ReturnsOneGray32Frame(
		string fixtureName,
		double seconds) {
		Skip.If(!_evil.NativeBindingAvailable, "FFmpeg native libraries not available");
		string? path = Resolve(fixtureName);
		Skip.If(path == null, $"{fixtureName} fixture was not generated");

		using var guard = new FfmpegStaticStateGuard();
		byte[]? gray = ExtractGray(path!, seconds, native: true);

		Assert.NotNull(gray);
		Assert.Equal(32 * 32, gray!.Length);
	}

	[SkippableTheory]
	[InlineData("long-gop", 3.55)]
	[InlineData("odd-dimensions", 1.0)]
	public void GrayBytes_NativeAndProcess_StablePathologiesRemainVisuallyEquivalent(
		string fixtureName,
		double seconds) {
		Skip.If(!_evil.FfmpegCliAvailable, _evil.FfmpegNotFoundReason);
		Skip.If(!_evil.NativeBindingAvailable, "FFmpeg native libraries not available");
		string? path = Resolve(fixtureName);
		Skip.If(path == null, $"{fixtureName} fixture was not generated");

		using var guard = new FfmpegStaticStateGuard();
		byte[]? native = ExtractGray(path!, seconds, native: true);
		byte[]? process = ExtractGray(path!, seconds, native: false);

		Assert.NotNull(native);
		Assert.NotNull(process);
		float diff = GrayBytesUtils.PercentageDifference(native!, process!);
		Assert.True(diff < GrayParityTolerance,
			$"{fixtureName}: native/process graybytes differ by {diff:P2}, expected < {GrayParityTolerance:P0}");
	}

	[SkippableFact]
	public void NativeBatch_MidStreamResolutionChange_ReconfiguresInsteadOfUsingStaleFrameMetadata() {
		Skip.If(!_evil.FfmpegCliAvailable, _evil.FfmpegNotFoundReason);
		Skip.If(!_evil.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(_evil.ResolutionChangeH264 == null, "resolution-change fixture was not generated");

		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		FfmpegEngine.CustomFFArguments = string.Empty;

		MediaInfo? mediaInfo = FFProbeEngine.GetMediaInfo(_evil.ResolutionChangeH264!, extendedLogging: false);
		Assert.NotNull(mediaInfo);
		Assert.True(mediaInfo!.Duration > TimeSpan.Zero);

		var entry = new FileEntry(_evil.ResolutionChangeH264!) { mediaInfo = mediaInfo };
		var positions = new List<float> { 0.20f, 0.80f };
		Assert.True(FfmpegEngine.GetGrayBytesFromVideo(
			entry,
			positions,
			maxSamplingDurationSeconds: 0,
			extendedLogging: true));

		foreach (float relativePosition in positions) {
			double key = entry.GetGrayBytesIndex(relativePosition, maxSamplingDurationSeconds: 0);
			Assert.True(entry.grayBytes.TryGetValue(key, out byte[]? gray));
			Assert.NotNull(gray);
			Assert.Equal(32 * 32, gray!.Length);
		}
	}

	[SkippableFact]
	public async Task TruncatedFastStartMp4_LateNativeExtraction_CompletesWithinBoundedTime() {
		Skip.If(!_evil.FfmpegCliAvailable, _evil.FfmpegNotFoundReason);
		Skip.If(!_evil.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(_evil.TruncatedH264 == null, "truncated fixture was not generated");

		using var guard = new FfmpegStaticStateGuard();
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.none;
		FfmpegEngine.CustomFFArguments = string.Empty;

		var sw = Stopwatch.StartNew();
		byte[]? result = await Task.Run(() =>
			FfmpegEngine.GetThumbnail(new FfmpegSettings {
				File = _evil.TruncatedH264!,
				Position = TimeSpan.FromSeconds(1.75),
				GrayScale = 1,
				SoftwareDecodeOnly = true,
			}, extendedLogging: true)).WaitAsync(TimeSpan.FromSeconds(12));
		sw.Stop();

		// Recovery of an earlier frame is acceptable; the regression is a hang/crash or a
		// retry ladder that exceeds the bounded corrupt-file handling budget.
		if (result != null)
			Assert.Equal(32 * 32, result.Length);
		Assert.True(sw.Elapsed < TimeSpan.FromSeconds(12),
			$"truncated-media extraction took {sw.Elapsed.TotalSeconds:F2}s");
	}
}
