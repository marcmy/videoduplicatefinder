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
using VDF.IntegrationTests.Fixtures;

namespace VDF.IntegrationTests.FFTools;

/// <summary>
/// The single-frame decode loop used to receive one frame per sent packet. When a packet
/// leaves output pending — VP9 superframes (alt-ref + shown frame in one packet) after a
/// decode error, for example — the next avcodec_send_packet failed with EAGAIN
/// ("Resource temporarily unavailable") and the sample was abandoned and retried on the CPU
/// and in process mode. FFmpeg's contract is to drain avcodec_receive_frame until EAGAIN
/// before sending more input.
/// </summary>
[Collection("Ffmpeg")]
public class DecoderPendingOutputTests {
	readonly FfmpegFixture _fixture;

	public DecoderPendingOutputTests(FfmpegFixture fixture) => _fixture = fixture;

	string? CreateDamagedVp9WithSuperframes() {
		string dir = Path.Combine(_fixture.TempDir, $"vp9-pending-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		string clean = Path.Combine(dir, "clean.webm");
		string damaged = Path.Combine(dir, "damaged.webm");
		var psi = new ProcessStartInfo(FfmpegEngine.FFmpegPath,
			"-hide_banner -loglevel error -y -f lavfi -i testsrc2=duration=20:size=1280x720:rate=30 " +
			"-c:v libvpx-vp9 -b:v 1M -auto-alt-ref 1 -lag-in-frames 16 -deadline realtime -cpu-used 8 " +
			$"\"{clean}\"") {
			CreateNoWindow = true,
			UseShellExecute = false,
		};
		using (Process? process = Process.Start(psi)) {
			if (process == null || !process.WaitForExit(120_000) || process.ExitCode != 0)
				return null;
		}
		// Damage the middle of the file (headers and cues intact) with a fixed seed.
		byte[] data = File.ReadAllBytes(clean);
		var random = new Random(7);
		for (int spot = 0; spot < 40; spot++) {
			int pos = random.Next(data.Length / 10, data.Length * 9 / 10);
			int length = random.Next(1, 65);
			for (int k = 0; k < length && pos + k < data.Length; k++)
				data[pos + k] = (byte)random.Next(256);
		}
		File.WriteAllBytes(damaged, data);
		return damaged;
	}

	[SkippableFact]
	public void DamagedVp9Superframes_NeverFailWithEagain() {
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);
		Skip.If(!_fixture.HasLibvpxVp9, "libvpx-vp9 encoder not available");
		string? file = CreateDamagedVp9WithSuperframes();
		Skip.If(file == null, "Could not generate the VP9 test video");

		int decoded = 0;
		List<string> eagain = new();
		for (int second = 1; second <= 19; second++) {
			try {
				using var decoder = new VideoStreamDecoder(file!, AVHWDeviceType.AV_HWDEVICE_TYPE_NONE);
				if (decoder.TryDecodeFrame(out _, TimeSpan.FromSeconds(second)))
					decoded++;
			}
			catch (Exception e) when (e.Message.Contains("temporarily unavailable", StringComparison.OrdinalIgnoreCase)) {
				eagain.Add($"{second}s: {e.Message}");
			}
		}

		Assert.True(eagain.Count == 0, "avcodec_send_packet hit EAGAIN:\n" + string.Join("\n", eagain));
		Assert.True(decoded > 0, "No position decoded at all.");
	}
}
