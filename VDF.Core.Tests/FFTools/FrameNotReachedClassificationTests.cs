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

/// <summary>
/// A requested frame that a damaged/truncated file never delivers used to count as a hardware
/// failure whenever hardware decode was configured: our failure text names the device
/// ("D3D11VA") and contains "failed", which satisfied the hardware heuristic. Every such file
/// was then decoded again on the CPU and once more through the FFmpeg process, failing each
/// time. Only FFmpeg's own diagnostics may implicate the GPU for these, and the
/// "Failed setup for format d3d11" notice (FFmpeg fell back to software decode) does not.
/// </summary>
public class FrameNotReachedClassificationTests {
	const string FailureText =
		"requested AV_HWDEVICE_TYPE_D3D11VA System.Exception: TryDecodeFrame failed at pos=00:01:10 for 'x.mp4'. size=608x1080";

	static Exception NotReached() =>
		new FfmpegEngine.FrameNotReachedException("TryDecodeFrame failed at pos=00:01:10 for 'x.mp4'. size=608x1080");

	[Fact]
	public void OldHeuristic_MisclassifiesFrameNotReachedAsHardware() =>
		// Documents why the dedicated path exists: the generic check matches our own text.
		Assert.True(FfmpegEngine.IsHardwareDecodeFailure(FailureText));

	[Theory]
	[InlineData("")]
	[InlineData("[h264 @ 0x0] Invalid NAL unit size (6830 > 3965).\n[mov,mp4 @ 0x0] stream 0, offset 0x2a17eec: partial file")]
	[InlineData("[h264 @ 0x0] Failed setup for format d3d11: hwaccel initialisation returned error.\n[h264 @ 0x0] Invalid NAL unit size (6830 > 3965).")]
	[InlineData("[mpeg4 @ 0x0] Error at MB: 340\n[mpeg4 @ 0x0] marker does not match f_code")]
	public void FrameNotReached_WithoutGpuErrors_IsNotHardware(string diagnostics) =>
		Assert.False(FfmpegEngine.IsHardwareImplicatedNativeFailure(NotReached(), diagnostics, FailureText));

	[Theory]
	[InlineData("[h264 @ 0x0] hwaccel transfer data failed")]
	[InlineData("[h264 @ 0x0] av_hwframe_transfer_data failed: Generic error in an external library")]
	public void FrameNotReached_WithGpuErrors_IsHardware(string diagnostics) =>
		Assert.True(FfmpegEngine.IsHardwareImplicatedNativeFailure(NotReached(), diagnostics, FailureText));

	[Fact]
	public void OtherNativeFailures_KeepTheExistingHeuristic() {
		var failure = new Exception("av_hwframe_transfer_data(AV_PIX_FMT_D3D11) failed: Generic error in an external library");
		Assert.True(FfmpegEngine.IsHardwareImplicatedNativeFailure(failure, string.Empty, $"requested AV_HWDEVICE_TYPE_D3D11VA {failure}"));
	}

	[Fact]
	public void SoftwareFallbackNotice_DoesNotMaskCorruption() {
		string diagnostics =
			"[h264 @ 0x0] Failed setup for format d3d11: hwaccel initialisation returned error.\n" +
			"[h264 @ 0x0] Invalid NAL unit size (54683 > 28227).";
		// Unstripped, the notice wins (hardware rules are checked first)...
		Assert.Equal(FfmpegErrorCategory.HardwareAcceleration, FfmpegErrorClassifier.Categorize(diagnostics));
		// ...but for a frame that was never reached it is noise, not the cause.
		Assert.Equal(FfmpegErrorCategory.CorruptOrTruncated, FfmpegEngine.CategorizeNativeFailure(NotReached(), diagnostics));
		Assert.True(FfmpegEngine.IsUnreachableFrameInCorruptFile(NotReached(), diagnostics));
	}

	[Theory]
	[InlineData("[mov,mp4 @ 0x0] stream 0, offset 0x2999f31: partial file")]
	[InlineData("[in#0/flv @ 0x0] Packet mismatch -1821966336 37735 37735")]
	[InlineData("[mpeg4 @ 0x0] Error at MB: 351")]
	public void UnreachableFrameInCorruptFile_SkipsProcessRetry(string diagnostics) =>
		Assert.True(FfmpegEngine.IsUnreachableFrameInCorruptFile(NotReached(), diagnostics));

	[Theory]
	// No evidence of damage: the process path (different seek behaviour) may still succeed.
	[InlineData("")]
	[InlineData("[mpeg4 @ 0x0] low_delay flag set incorrectly, clearing it")]
	// GPU implicated: keep the retries.
	[InlineData("[h264 @ 0x0] hwaccel transfer data failed\n[h264 @ 0x0] Invalid NAL unit size (1 > 0).")]
	public void UnreachableFrame_WithoutCorruptionEvidence_KeepsProcessRetry(string diagnostics) =>
		Assert.False(FfmpegEngine.IsUnreachableFrameInCorruptFile(NotReached(), diagnostics));

	[Fact]
	public void OtherExceptions_NeverSkipProcessRetry() =>
		Assert.False(FfmpegEngine.IsUnreachableFrameInCorruptFile(
			new Exception("Invalid data found when processing input"), "[mov,mp4 @ 0x0] partial file"));

	[Fact]
	public void StripHardwareFallbackNotices_RemovesOnlyTheNoticeLines() {
		string stripped = FfmpegEngine.StripHardwareFallbackNotices(
			"[h264 @ 0x0] Failed setup for format d3d11: hwaccel initialisation returned error.\n[aac @ 0x0] Input buffer exhausted");
		Assert.Equal("[aac @ 0x0] Input buffer exhausted", stripped);
		Assert.Equal(string.Empty, FfmpegEngine.StripHardwareFallbackNotices(null));
	}
}
