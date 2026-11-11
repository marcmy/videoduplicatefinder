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

using System.Diagnostics;
using FFmpeg.AutoGen;
using VDF.Core.FFTools;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;
using VDF.IntegrationTests.Fixtures;

namespace VDF.IntegrationTests.FFTools;

/// <summary>
/// Phone/hardware encoders commonly write H.264 as plain Baseline (constraint_set1 = 0).
/// FFmpeg's DXVA/D3D11VA hwaccel only lists Constrained Baseline, Main and High, so these
/// streams failed hwaccel setup ("Failed setup for format d3d11") and silently decoded on
/// the CPU. With the profile check relaxed they decode on the GPU, bit-identically.
/// </summary>
[Collection("Ffmpeg")]
public class H264BaselineHardwareDecodeTests {
	readonly FfmpegFixture _fixture;

	public H264BaselineHardwareDecodeTests(FfmpegFixture fixture) => _fixture = fixture;

	/// <summary>
	/// Encodes Constrained Baseline with x264, then clears constraint_set1 in the avcC SPS so
	/// the stream signals plain Baseline (profile_idc 66, constraint flags 0x80), exactly like
	/// the "AVC Coding" phone recordings that triggered the CPU fallback.
	/// </summary>
	string? CreatePlainBaselineMp4() {
		string dir = Path.Combine(_fixture.TempDir, $"baseline-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		string constrained = Path.Combine(dir, "constrained.mp4");
		string plain = Path.Combine(dir, "plain-baseline.mp4");
		var psi = new ProcessStartInfo(FfmpegEngine.FFmpegPath,
			"-hide_banner -loglevel error -y -f lavfi -i testsrc2=duration=3:size=480x912:rate=30 " +
			$"-c:v libx264 -profile:v baseline -pix_fmt yuv420p -movflags +faststart \"{constrained}\"") {
			CreateNoWindow = true,
			UseShellExecute = false,
		};
		using (Process? process = Process.Start(psi)) {
			if (process == null || !process.WaitForExit(60_000) || process.ExitCode != 0)
				return null;
		}
		byte[] data = File.ReadAllBytes(constrained);
		int avcC = data.AsSpan().IndexOf("avcC"u8);
		if (avcC < 0)
			return null;
		int payload = avcC + 4;
		// avcC: version, profile, compat, level, lengthSize, numSps, spsLength(2), SPS NAL...
		if (data[payload + 1] != 0x42 || (data[payload + 8] & 0x1F) != 7 || data[payload + 9] != 0x42)
			return null;
		data[payload + 2] = 0x80;
		data[payload + 10] = 0x80;
		File.WriteAllBytes(plain, data);
		return plain;
	}

	[SkippableFact]
	public void PlainBaselineH264_DecodesOnD3D11_WhenAvailable() {
		Skip.If(!OperatingSystem.IsWindows(), "D3D11VA is Windows-only");
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		string? file = CreatePlainBaselineMp4();
		Skip.If(file == null, "Could not generate the plain Baseline test video");
		Skip.If(_fixture.H264_8bit == null || !D3D11DecodeAvailable(_fixture.H264_8bit!), "D3D11VA decode unavailable on this machine");

		using var guard = new FfmpegStaticStateGuard();
		using var decoder = new VideoStreamDecoder(file!, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
		Assert.Equal(ffmpeg.AV_PROFILE_H264_BASELINE, decoder.CodecProfile);
		Assert.True(decoder.TryDecodeFrame(out AVFrame frame, TimeSpan.FromSeconds(1), out _, FrameTransferMode.KeepHardwareFrame));
		Assert.True(VideoStreamDecoder.IsHardwareFrame(frame),
			$"Plain Baseline H.264 fell back to software decode ({(AVPixelFormat)frame.format}) under D3D11VA.");

		// And the GPU path produces the same picture as the CPU path.
		using var scaler = new D3D11VideoProcessorGrayByteScaler();
		byte[] gpuGray = scaler.ScaleToGray32(frame, out _);
		using var cpuDecoder = new VideoStreamDecoder(file!, AVHWDeviceType.AV_HWDEVICE_TYPE_NONE);
		Assert.True(cpuDecoder.TryDecodeFrame(out AVFrame cpuFrame, TimeSpan.FromSeconds(1)));
		using var converter = new VideoFrameConverter(
			new System.Drawing.Size(cpuFrame.width, cpuFrame.height), (AVPixelFormat)cpuFrame.format,
			new System.Drawing.Size(32, 32), AVPixelFormat.AV_PIX_FMT_GRAY8);
		byte[] cpuGray = CopyGray32(converter.Convert(cpuFrame));
		Assert.True(GrayBytesUtils.PercentageDifference(gpuGray, cpuGray) < 0.10f);
	}

	static unsafe byte[] CopyGray32(AVFrame frame) {
		byte[] output = new byte[32 * 32];
		for (int y = 0; y < 32; y++)
			new ReadOnlySpan<byte>(frame.data[0] + y * frame.linesize[0], 32).CopyTo(output.AsSpan(y * 32));
		return output;
	}

	static bool D3D11DecodeAvailable(string file) {
		try {
			using var decoder = new VideoStreamDecoder(file, AVHWDeviceType.AV_HWDEVICE_TYPE_D3D11VA);
			return decoder.TryDecodeFrame(out _, TimeSpan.Zero);
		}
		catch {
			return false;
		}
	}
}
