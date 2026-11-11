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

using FFmpeg.AutoGen;
using VDF.Core.FFTools.FFmpegNative;

namespace VDF.Core.Tests.FFTools;

/// <summary>
/// Only plain H.264 Baseline gets FFmpeg's hwaccel profile check relaxed: DXVA/D3D11VA
/// rejects it although GPUs decode it bit-identically. Everything else keeps the strict
/// check so genuinely unsupported streams still fall back to software cleanly.
/// </summary>
public class HardwareProfileMismatchTests {
	const int H264ConstrainedBaseline = 66 | (1 << 9); // AV_PROFILE_H264_CONSTRAINED_BASELINE
	const int H264Main = 77;
	const int H264Extended = 88;
	const int H264High = 100;
	const int H264High10 = 110;
	const int HevcMain = 1;
	const int Unknown = -99;

	[Fact]
	public void PlainH264Baseline_IsRelaxed() {
		Environment.SetEnvironmentVariable(VideoStreamDecoder.DisableBaselineHardwareDecodeEnvVar, null);
		Assert.True(VideoStreamDecoder.ShouldAllowHardwareProfileMismatch(AVCodecID.AV_CODEC_ID_H264, ffmpeg.AV_PROFILE_H264_BASELINE));
	}

	[Theory]
	[InlineData(H264ConstrainedBaseline)] // already accepted by DXVA
	[InlineData(H264Main)]
	[InlineData(H264High)]
	[InlineData(H264Extended)]            // FMO/ASO/data partitioning are real there
	[InlineData(H264High10)]              // GPU lacks it: must keep falling back
	[InlineData(Unknown)]
	public void OtherH264Profiles_KeepTheStrictCheck(int profile) =>
		Assert.False(VideoStreamDecoder.ShouldAllowHardwareProfileMismatch(AVCodecID.AV_CODEC_ID_H264, profile));

	[Theory]
	[InlineData(AVCodecID.AV_CODEC_ID_HEVC, HevcMain)]
	[InlineData(AVCodecID.AV_CODEC_ID_HEVC, 66)]
	[InlineData(AVCodecID.AV_CODEC_ID_VP9, 0)]
	public void OtherCodecs_KeepTheStrictCheck(AVCodecID codec, int profile) =>
		Assert.False(VideoStreamDecoder.ShouldAllowHardwareProfileMismatch(codec, profile));

	[Fact]
	public void EnvironmentOptOut_DisablesTheRelaxation() {
		string? previous = Environment.GetEnvironmentVariable(VideoStreamDecoder.DisableBaselineHardwareDecodeEnvVar);
		try {
			Environment.SetEnvironmentVariable(VideoStreamDecoder.DisableBaselineHardwareDecodeEnvVar, "1");
			Assert.False(VideoStreamDecoder.ShouldAllowHardwareProfileMismatch(AVCodecID.AV_CODEC_ID_H264, ffmpeg.AV_PROFILE_H264_BASELINE));
		}
		finally {
			Environment.SetEnvironmentVariable(VideoStreamDecoder.DisableBaselineHardwareDecodeEnvVar, previous);
		}
	}
}
