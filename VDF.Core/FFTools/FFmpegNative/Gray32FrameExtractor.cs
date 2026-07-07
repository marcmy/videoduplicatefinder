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

using FFmpeg.AutoGen;

namespace VDF.Core.FFTools.FFmpegNative {
	static unsafe class Gray32FrameExtractor {
		public static byte[] Extract(AVFrame convertedFrame) {
			const int N = 32;
			int width = convertedFrame.width;
			int height = convertedFrame.height;
			if (width != N || height != N) throw new Exception($"Unexpected size {width}x{height}, expected {N}.");
			if (convertedFrame.data[0] == null) throw new Exception("Converted frame has no data[0] (null).");
			if (convertedFrame.linesize[0] < width) throw new Exception($"Invalid linesize ({convertedFrame.linesize[0]}) for width {width}.");
			int srcStride = convertedFrame.linesize[0];
			byte[] outBuf = new byte[width * height];
			fixed (byte* destPtr = outBuf) {
				byte* sourcePtr = convertedFrame.data[0];
				for (int y = 0; y < height; y++)
					Buffer.MemoryCopy(sourcePtr + (y * srcStride), destPtr + (y * width), width, width);
			}
			return outBuf;
		}
	}
}
