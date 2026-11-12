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
// The seek-based dense AI sampler only takes files whose container seeks by index and whose
// decoder honours skip_frame nokey; everything else stays on the CLI sweep.

using VDF.Core.FFTools;

namespace VDF.Core.Tests.FFTools;

public class DenseNativeEligibilityTests {
	[Theory]
	[InlineData("mov,mp4,m4a,3gp,3g2,mj2", true)]
	[InlineData("matroska,webm", true)]
	[InlineData("avi", true)]
	[InlineData("asf", true)]
	[InlineData("mpegts", false)]
	[InlineData("mpeg", false)]
	[InlineData("flv", false)]
	[InlineData("rm", false)]
	[InlineData("h264", false)]
	[InlineData("", false)]
	[InlineData(null, false)]
	public void Container_NeedsAnIndexToSeekBy(string? formatName, bool expected) =>
		Assert.Equal(expected, FfmpegEngine.IsDenseNativeSeekableFormat(formatName));

	[Theory]
	[InlineData("h264", true)]
	[InlineData("hevc", true)]
	[InlineData("mpeg4", true)]
	[InlineData("mpeg2video", true)]
	[InlineData("av1", true)]
	[InlineData("vp8", true)]
	[InlineData("vp9", false)] // its decoder ignores skip_frame: the CLI samples every frame
	[InlineData("vc1", false)] // untested, stays on the CLI
	[InlineData(null, false)]
	public void Codec_MustDecodeKeyframesOnlyInTheCli(string? codec, bool expected) =>
		Assert.Equal(expected, FfmpegEngine.IsDenseNativeKeyframeOnlyCodec(codec));

	[Theory]
	[InlineData(5.0, 5.0)]
	[InlineData(7.28333333, 7.283)]
	[InlineData(12.0004, 12.0)]
	[InlineData(9.99951, 10.0)]
	public void Interval_IsRoundedLikeTheCliArgument(double interval, double expected) =>
		Assert.Equal(expected, FfmpegEngine.GetDenseCliInterval(interval), 9);
}
