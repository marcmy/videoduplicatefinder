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

namespace VDF.Core.Tests.FFTools;

public class FfmpegEngineTests {
	[Theory]
	[InlineData("[av1 @ 000002] Your platform doesn't support hardware accelerated AV1 decoding. [AVHWDeviceContext @ 000002] Failed to create Direct3D device. Device setup failed for decoder on input stream #0:0 : Function not implemented")]
	[InlineData("[h264 @ 000002] No device available for decoder: device type d3d11va needed for codec h264. Device setup failed for decoder on input stream #0:0")]
	[InlineData("av_hwdevice_ctx_create(AV_HWDEVICE_TYPE_D3D11VA) failed: Function not implemented")]
	[InlineData("[h264 @ 000002] Failed setup for format d3d11: hwaccel initialisation returned error.")]
	public void IsHardwareDecodeFailure_HardwareFailureText_ReturnsTrue(string text) {
		Assert.True(FfmpegEngine.IsHardwareDecodeFailure(text));
	}

	[Theory]
	[InlineData("FFmpeg exited with: 1")]
	[InlineData("[matroska,webm @ 000002] EBML header parsing failed")]
	[InlineData("Function not implemented")]
	[InlineData("Invalid argument")]
	[InlineData("requested AV_HWDEVICE_TYPE_D3D11VA VDF.Core.FFTools.FFInvalidExitCodeException: Invalid argument")]
	public void IsHardwareDecodeFailure_GenericFailureText_ReturnsFalse(string text) {
		Assert.False(FfmpegEngine.IsHardwareDecodeFailure(text));
	}

	[Theory]
	[InlineData("[av1 @ 000002] Your platform doesn't support hardware accelerated AV1 decoding.")]
	[InlineData("[d3d11va @ 000002] Unsupported hardware codec: av1")]
	[InlineData("[cuda @ 000002] Hardware decoder does not support this codec")]
	[InlineData("[qsv @ 000002] Unsupported profile: main10")]
	[InlineData("[d3d11va @ 000002] Pixel format p010 is not supported by this device")]
	public void IsPersistentHardwareCodecFailure_ExplicitCapabilityText_ReturnsTrue(
		string text) {
		Assert.True(
			FfmpegEngine.IsPersistentHardwareCodecFailure(text));
	}

	[Theory]
	[InlineData("[h264 @ 000002] d3d11 hardware decode error while decoding a corrupted frame.")]
	[InlineData("[h264 @ 000002] d3d11 hardware decode failed after invalid packet data.")]
	[InlineData("[h264 @ 000002] No device available for decoder: device type d3d11va needed for codec h264.")]
	[InlineData("av_hwdevice_ctx_create(AV_HWDEVICE_TYPE_D3D11VA) failed: Function not implemented")]
	[InlineData("[h264 @ 000002] Failed setup for format d3d11: hwaccel initialisation returned error.")]
	public void IsPersistentHardwareCodecFailure_GenericOrDeviceFailure_ReturnsFalse(
		string text) {
		Assert.False(
			FfmpegEngine.IsPersistentHardwareCodecFailure(text));
	}

	[Fact]
	public void FfmpegErrorAccumulator_TruncatesAfterMaximumCapturedLines() {
		var accumulator = new FfmpegEngine.FfmpegErrorAccumulator(maxLines: 3);

		accumulator.AppendLine("line 1");
		accumulator.AppendLine("line 2");
		accumulator.AppendLine("line 3");
		accumulator.AppendLine("line 4");
		accumulator.AppendLine("line 5");

		string text = accumulator.ToString();
		Assert.Contains("line 1", text);
		Assert.Contains("line 2", text);
		Assert.Contains("line 3", text);
		Assert.DoesNotContain("line 4", text);
		Assert.DoesNotContain("line 5", text);
		Assert.Contains("omitted 2 additional FFmpeg stderr line(s)", text);
	}

	[Fact]
	public void FfmpegErrorAccumulator_CollapsesConsecutiveRepeatsBeforeTruncating() {
		var accumulator = new FfmpegEngine.FfmpegErrorAccumulator(maxLines: 3);

		accumulator.AppendLine("same decoder warning");
		accumulator.AppendLine("same decoder warning");
		accumulator.AppendLine("same decoder warning");
		accumulator.AppendLine("different warning");

		string text = accumulator.ToString();
		Assert.Contains("same decoder warning (repeated 2 more times)", text);
		Assert.Contains("different warning", text);
		Assert.DoesNotContain("omitted", text);
	}

	[Fact]
	public void FormatNativeGrayByteBatchSkippedLog_IncludesProcessFallbackContext() {
		string message = FfmpegEngine.FormatNativeGrayByteBatchSkippedLog(
			@"X:\video.mp4",
			"h264|yuv420p|320x224",
			"libraries-unavailable",
			10);

		Assert.Equal(@"Native FFmpeg batched graybyte extraction skipped for 'X:\video.mp4': native=libraries-unavailable, family=h264|yuv420p|320x224, samples=10; using FFmpeg process per-sample path", message);
	}

	[Fact]
	public void FormatProcessGrayByteBatchTimingLog_IncludesNativeFallbackContext() {
		string message = FfmpegEngine.FormatProcessGrayByteBatchTimingLog(
			@"X:\video.mp4",
			"h264|yuv420p|320x224",
			"h264",
			"libraries-unavailable",
			"requested",
			processSamples: 3,
			totalSamples: 10,
			stagedNativeSamples: 7,
			totalMs: 1234);

		Assert.Equal(@"FFmpeg process graybyte extraction completed for 'X:\video.mp4': mode=process-per-sample, family=h264|yuv420p|320x224, codec=h264, native=libraries-unavailable, hwPolicy=requested, processSamples=3/10, stagedNativeSamples=7/10, samples=10, total=1234ms", message);
	}

	[Fact]
	public void FormatCachedGrayByteScanLog_IncludesSampleContext() {
		string message = FfmpegEngine.FormatCachedGrayByteScanLog(
			@"X:\video.mp4",
			"h264|yuv420p|320x224",
			"h264",
			cachedSamples: 10,
			totalSamples: 10);

		Assert.Equal(@"FFmpeg graybyte extraction skipped for 'X:\video.mp4': mode=cached, family=h264|yuv420p|320x224, codec=h264, cachedSamples=10/10, samples=10", message);
	}

	[Fact]
	public void FormatProcessTimingLog_IncludesThumbnailSuccessContext() {
		string message = FfmpegEngine.FormatProcessTimingLog(
			@"X:\video.mp4",
			TimeSpan.FromSeconds(1.5),
			isGrayByte: false,
			hardwareRequested: true,
			hardwarePolicy: "requested",
			bytes: 12345,
			totalMs: 67);

		Assert.Equal(@"FFmpeg process timing on 'X:\video.mp4' @ 00:00:01.5000000: mode=thumb, hw=requested, hwPolicy=requested, bytes=12345, total=67ms", message);
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public void ShouldLogNativeSuccessTiming_DoesNotDependOnExtendedLogging(bool extendedLogging) {
		Assert.True(FfmpegEngine.ShouldLogNativeSuccessTiming(extendedLogging));
	}

	[Theory]
	[InlineData(false)]
	[InlineData(true)]
	public void ShouldLogGrayByteScanTelemetry_DoesNotDependOnExtendedLogging(bool extendedLogging) {
		Assert.True(FfmpegEngine.ShouldLogGrayByteScanTelemetry(extendedLogging));
	}

	[Fact]
	public void ConfiguredHardwareDecodeBypass_DoesNotTripAfterRepeatedSetupFailures() {
		FFHardwareAccelerationMode oldMode = FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
		try {
			FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
			string failure = "[h264 @ 000002] Failed setup for format d3d11: hwaccel initialisation returned error.";

			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);
			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);
			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);

			Assert.False(FfmpegEngine.IsConfiguredHardwareDecodeBypassed(out _));
		}
		finally {
			FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Fact]
	public void NativeFrameExtraction_KeepsDisplayThumbnailsEligibleAfterHardwareFailures() {
		FFHardwareAccelerationMode oldMode = FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
		FfmpegEngine.ResetNativeBindingHealthForTests();
		try {
			FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
			string failure = "[h264 @ 000002] Failed setup for format d3d11: hwaccel initialisation returned error.";
			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);
			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);
			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);

			Assert.False(FfmpegEngine.IsNativeBindingDisabledForSessionForTests);
			Assert.Equal(
				FFmpegHelper.CanLoadNativeLibraries,
				FfmpegEngine.ShouldAttemptNativeSingleFrameExtraction(new FfmpegSettings {
					GrayScale = 0,
					File = "video.mp4",
					Position = TimeSpan.Zero,
				}));
		}
		finally {
			FfmpegEngine.UseNativeBinding = false;
			FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.ResetNativeBindingHealthForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Fact]
	public void HardwareCapabilityFailure_BypassesOnlyExactFamily() {
		FFHardwareAccelerationMode oldMode =
			FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();

		try {
			FfmpegEngine.HardwareAccelerationMode =
				FFHardwareAccelerationMode.d3d11va;

			const string failure =
				"[d3d11va @ 000002] Unsupported profile: high10";
			const string familyA =
				"h264|yuv420p10le|1920x1080";
			const string familyB =
				"h264|yuv420p|1920x1080";

			FfmpegEngine.RecordHardwareDecodeFailureForCodec(
				"h264",
				failure,
				familyA);

			Assert.True(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"h264",
					out string reason,
					familyA));
			Assert.Contains(
				familyA,
				reason,
				StringComparison.OrdinalIgnoreCase);

			Assert.False(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"h264",
					out _,
					familyB));
			Assert.False(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"hevc",
					out _,
					"hevc|yuv420p10le|1920x1080"));
		}
		finally {
			FfmpegEngine
				.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Fact]
	public void HardwareDecodeFailure_RepeatedCorruptFilesNeverBypass() {
		FFHardwareAccelerationMode oldMode =
			FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();

		try {
			FfmpegEngine.HardwareAccelerationMode =
				FFHardwareAccelerationMode.d3d11va;

			const string failure =
				"[h264 @ 000002] d3d11 hardware decode error " +
				"while decoding a corrupted frame.";
			const string family =
				"h264|yuv420p|1920x1080";

			for (int i = 0; i < 20; i++) {
				FfmpegEngine.RecordHardwareDecodeFailureForCodec(
					"h264",
					failure,
					family);
			}

			Assert.False(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"h264",
					out _,
					family));
		}
		finally {
			FfmpegEngine
				.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Fact]
	public void D3D11SoftwareFrameFallback_NeverCreatesPersistentBypass() {
		FFHardwareAccelerationMode oldMode =
			FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();

		try {
			FfmpegEngine.HardwareAccelerationMode =
				FFHardwareAccelerationMode.d3d11va;

			const string reason =
				"D3D11 graybyte decode produced software frames " +
				"(AV_PIX_FMT_YUV420P)";
			const string family =
				"mpeg4|yuv420p|640x480";

			for (int i = 0; i < 20; i++) {
				FfmpegEngine
					.RecordD3D11SoftwareFrameFallbackForCodec(
						"mpeg4",
						reason,
						family);
			}

			Assert.False(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"mpeg4",
					out _,
					family));
		}
		finally {
			FfmpegEngine
				.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Fact]
	public void HardwareCapabilityFailure_WithoutFamilyBypassesOnlyCodec() {
		FFHardwareAccelerationMode oldMode =
			FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();

		try {
			FfmpegEngine.HardwareAccelerationMode =
				FFHardwareAccelerationMode.d3d11va;

			const string failure =
				"[d3d11va @ 000002] Hardware decoder does not " +
				"support this codec";

			FfmpegEngine.RecordHardwareDecodeFailureForCodec(
				"av1",
				failure);

			Assert.True(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"av1",
					out _));
			Assert.True(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"av1",
					out _,
					"av1|yuv420p10le|3840x2160"));
			Assert.False(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"hevc",
					out _));
		}
		finally {
			FfmpegEngine
				.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Fact]
	public void D3D11GrayByteConcurrency_DoesNotIncreaseWhenQueueAndDecodeSpikesAreBothHigh() {
		int result = FfmpegEngine.CalculateNativeGrayByteD3D11AutoConcurrencyForTests(
			oldLimit: 3,
			maxLimit: 8,
			averageQueueMs: 2_500,
			averageDecodeMs: 800,
			decodeSpikes: 2,
			observations: 12);

		Assert.Equal(3, result);
	}

	[Fact]
	public void D3D11GrayByteConcurrency_DoesNotDecreaseForMinorityDecodeSpikesWhenAverageDecodeIsModerate() {
		int result = FfmpegEngine.CalculateNativeGrayByteD3D11AutoConcurrencyForTests(
			oldLimit: 5,
			maxLimit: 8,
			averageQueueMs: 2_500,
			averageDecodeMs: 1_200,
			decodeSpikes: 4,
			observations: 12);

		Assert.Equal(5, result);
	}

	[Fact]
	public void D3D11GrayByteConcurrency_DecreasesForSustainedDecodeSpikes() {
		int result = FfmpegEngine.CalculateNativeGrayByteD3D11AutoConcurrencyForTests(
			oldLimit: 5,
			maxLimit: 8,
			averageQueueMs: 2_500,
			averageDecodeMs: 1_200,
			decodeSpikes: 6,
			observations: 12);

		Assert.Equal(4, result);
	}

	[Fact]
	public void D3D11GrayByteConcurrency_DecreasesForHighAverageDecode() {
		int result = FfmpegEngine.CalculateNativeGrayByteD3D11AutoConcurrencyForTests(
			oldLimit: 5,
			maxLimit: 8,
			averageQueueMs: 2_500,
			averageDecodeMs: 1_600,
			decodeSpikes: 3,
			observations: 12);

		Assert.Equal(4, result);
	}

	[Fact]
	public void D3D11GrayByteConcurrency_IncreasesWhenQueueIsHighAndDecodeIsClean() {
		int result = FfmpegEngine.CalculateNativeGrayByteD3D11AutoConcurrencyForTests(
			oldLimit: 3,
			maxLimit: 8,
			averageQueueMs: 2_500,
			averageDecodeMs: 300,
			decodeSpikes: 0,
			observations: 12);

		Assert.Equal(4, result);
	}

	[Fact]
	public void NativeBindingHealth_DisablesImmediatelyForAutoGenUnsupportedMethod() {
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.ResetNativeBindingHealthForTests();
		try {
			FfmpegEngine.RecordNativeFailure("video.mkv", new NotSupportedException("Specified method is not supported."));

			Assert.True(FfmpegEngine.IsNativeBindingDisabledForSessionForTests);
			Assert.False(FfmpegEngine.ShouldAttemptNativeBinding);
		}
		finally {
			FfmpegEngine.UseNativeBinding = false;
			FfmpegEngine.ResetNativeBindingHealthForTests();
		}
	}

	[Fact]
	public void NativeBindingHealth_KeepsNativeEnabledAfterRepeatedDecodeFailures() {
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.ResetNativeBindingHealthForTests();
		try {
			for (int i = 0; i < 5; i++)
				FfmpegEngine.RecordNativeFailure($"video-{i}.mkv", new InvalidOperationException("TryDecodeFrame failed"));

			Assert.False(FfmpegEngine.IsNativeBindingDisabledForSessionForTests);
			Assert.Equal(FFmpegHelper.CanLoadNativeLibraries, FfmpegEngine.ShouldAttemptNativeBinding);
		}
		finally {
			FfmpegEngine.UseNativeBinding = false;
			FfmpegEngine.ResetNativeBindingHealthForTests();
		}
	}
	[Fact]
	public void HardwareSuccess_ClearsMatchingCapabilityBypass() {
		FFHardwareAccelerationMode oldMode =
			FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();

		try {
			FfmpegEngine.HardwareAccelerationMode =
				FFHardwareAccelerationMode.d3d11va;

			const string family =
				"h264|yuv420p10le|1920x1080";
			const string failure =
				"[d3d11va @ 000002] Unsupported profile: high10";

			FfmpegEngine.RecordHardwareDecodeFailureForCodec(
				"h264",
				failure,
				family);
			Assert.True(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"h264",
					out _,
					family));

			FfmpegEngine.RecordHardwareDecodeSuccessForCodec(
				"h264",
				family);

			Assert.False(
				FfmpegEngine.ShouldBypassHardwareDecodeForCodec(
					"h264",
					out _,
					family));
		}
		finally {
			FfmpegEngine
				.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
	}

	[Fact]
	public void NativeBindingHealth_DisablesImmediatelyForInfrastructureFailure() {
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.ResetNativeBindingHealthForTests();

		try {
			FfmpegEngine.RecordNativeFailure(
				"video.mkv",
				new DllNotFoundException(
					"avcodec shared library was not found"));

			Assert.True(
				FfmpegEngine.IsNativeBindingDisabledForSessionForTests);
			Assert.False(FfmpegEngine.ShouldAttemptNativeBinding);
		}
		finally {
			FfmpegEngine.UseNativeBinding = false;
			FfmpegEngine.ResetNativeBindingHealthForTests();
		}
	}

	[Fact]
	public void NativeBindingHealth_DetectsNestedInfrastructureFailure() {
		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.ResetNativeBindingHealthForTests();

		try {
			FfmpegEngine.RecordNativeFailure(
				"video.mkv",
				new InvalidOperationException(
					"decoder open failed",
					new EntryPointNotFoundException(
						"avcodec_send_packet")));

			Assert.True(
				FfmpegEngine.IsNativeBindingDisabledForSessionForTests);
		}
		finally {
			FfmpegEngine.UseNativeBinding = false;
			FfmpegEngine.ResetNativeBindingHealthForTests();
		}
	}
}
