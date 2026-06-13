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

namespace VDF.Core.Tests.FFTools;

public class FfmpegEngineTests {
	[Theory]
	[InlineData("[av1 @ 000002] Your platform doesn't support hardware accelerated AV1 decoding. [AVHWDeviceContext @ 000002] Failed to create Direct3D device. Device setup failed for decoder on input stream #0:0 : Function not implemented")]
	[InlineData("[h264 @ 000002] No device available for decoder: device type d3d11va needed for codec h264. Device setup failed for decoder on input stream #0:0")]
	[InlineData("av_hwdevice_ctx_create(AV_HWDEVICE_TYPE_D3D11VA) failed: Function not implemented")]
	[InlineData("requested AV_HWDEVICE_TYPE_D3D11VA VDF.Core.FFTools.FFInvalidExitCodeException: Invalid argument")]
	[InlineData("[h264 @ 000002] Failed setup for format d3d11: hwaccel initialisation returned error.")]
	public void IsHardwareDecodeFailure_HardwareFailureText_ReturnsTrue(string text) {
		Assert.True(FfmpegEngine.IsHardwareDecodeFailure(text));
	}

	[Theory]
	[InlineData("FFmpeg exited with: 1")]
	[InlineData("[matroska,webm @ 000002] EBML header parsing failed")]
	[InlineData("Function not implemented")]
	[InlineData("Invalid argument")]
	public void IsHardwareDecodeFailure_GenericFailureText_ReturnsFalse(string text) {
		Assert.False(FfmpegEngine.IsHardwareDecodeFailure(text));
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
	public void ConfiguredHardwareDecodeBypass_TripsAfterRepeatedSetupFailures() {
		FFHardwareAccelerationMode oldMode = FfmpegEngine.HardwareAccelerationMode;
		FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
		try {
			FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
			string failure = "[h264 @ 000002] Failed setup for format d3d11: hwaccel initialisation returned error.";

			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);
			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);
			Assert.False(FfmpegEngine.IsConfiguredHardwareDecodeBypassed(out _));

			FfmpegEngine.MarkConfiguredHardwareDecodeFailure(failure);

			Assert.True(FfmpegEngine.IsConfiguredHardwareDecodeBypassed(out string reason));
			Assert.Contains("Failed setup", reason);
		}
		finally {
			FfmpegEngine.ResetConfiguredHardwareDecodeAdaptiveStateForTests();
			FfmpegEngine.HardwareAccelerationMode = oldMode;
		}
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
			Assert.True(FfmpegEngine.ShouldAttemptNativeBinding);
		}
		finally {
			FfmpegEngine.UseNativeBinding = false;
			FfmpegEngine.ResetNativeBindingHealthForTests();
		}
	}
}
