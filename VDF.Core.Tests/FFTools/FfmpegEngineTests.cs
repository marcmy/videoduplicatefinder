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
	public void IsHardwareDecodeFailure_HardwareFailureText_ReturnsTrue(string text) {
		Assert.True(FfmpegEngine.IsHardwareDecodeFailure(text));
	}

	[Theory]
	[InlineData("FFmpeg exited with: 1")]
	[InlineData("[matroska,webm @ 000002] EBML header parsing failed")]
	[InlineData("Function not implemented")]
	public void IsHardwareDecodeFailure_GenericFailureText_ReturnsFalse(string text) {
		Assert.False(FfmpegEngine.IsHardwareDecodeFailure(text));
	}
}
