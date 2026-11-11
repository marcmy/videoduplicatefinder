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
using VDF.Core;
using VDF.Core.FFTools;
using VDF.Core.FFTools.FFmpegNative;
using VDF.Core.Utils;
using VDF.IntegrationTests.Fixtures;

namespace VDF.IntegrationTests.FFTools;

/// <summary>
/// A truncated file sampled past the end of its data must fail once, under hardware decode
/// too: no "hardware decode failure" CPU retry and no FFmpeg process retry — both ran the same
/// decoders over the same missing bytes, multiplying CPU work for files that cannot succeed.
/// </summary>
[Collection("Ffmpeg")]
public class TruncatedFileRetryTests {
	readonly FfmpegFixture _fixture;

	public TruncatedFileRetryTests(FfmpegFixture fixture) => _fixture = fixture;

	string? CreateTruncatedMp4() {
		string dir = Path.Combine(_fixture.TempDir, $"truncated-{Guid.NewGuid():N}");
		Directory.CreateDirectory(dir);
		string full = Path.Combine(dir, "full.mp4");
		string cut = Path.Combine(dir, "cut.mp4");
		// +faststart keeps the sample index at the front, so the cut file still opens and
		// advertises samples whose bytes are gone — what a partial download looks like.
		var psi = new ProcessStartInfo(FfmpegEngine.FFmpegPath,
			"-hide_banner -loglevel error -y -f lavfi -i testsrc2=duration=10:size=640x360:rate=25 " +
			$"-c:v libx264 -preset ultrafast -g 25 -pix_fmt yuv420p -movflags +faststart \"{full}\"") {
			CreateNoWindow = true,
			UseShellExecute = false,
		};
		using (Process? process = Process.Start(psi)) {
			if (process == null || !process.WaitForExit(60_000) || process.ExitCode != 0)
				return null;
		}
		byte[] bytes = File.ReadAllBytes(full);
		File.WriteAllBytes(cut, bytes.AsSpan(0, bytes.Length / 2).ToArray());
		return cut;
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

	[SkippableFact]
	public void TruncatedFile_D3D11_FailsWithoutCpuOrProcessRetries() {
		Skip.If(!OperatingSystem.IsWindows(), "D3D11VA is Windows-only");
		Skip.If(!_fixture.NativeBindingAvailable, "FFmpeg native libraries not available");
		Skip.If(!_fixture.FfmpegCliAvailable, _fixture.FfmpegNotFoundReason);

		using var guard = new FfmpegStaticStateGuard();
		string? truncated = CreateTruncatedMp4();
		Skip.If(truncated == null, "Could not generate the truncated test video");
		Skip.If(!D3D11DecodeAvailable(truncated!), "D3D11VA decode unavailable on this machine");

		FfmpegEngine.UseNativeBinding = true;
		FfmpegEngine.HardwareAccelerationMode = FFHardwareAccelerationMode.d3d11va;
		var entry = new FileEntry(truncated!) {
			mediaInfo = new MediaInfo {
				Duration = TimeSpan.FromSeconds(10),
				Streams = Array.Empty<MediaInfo.StreamInfo>(),
			},
		};

		List<string> messages = new();
		void Capture(LogEntry logEntry) {
			if (logEntry.Message.Contains(truncated!, StringComparison.OrdinalIgnoreCase))
				lock (messages)
					messages.Add(logEntry.Message);
		}
		Logger.Instance.LogEntryAdded += Capture;
		bool succeeded;
		try {
			// 90% is well past the half of the data that is still there.
			succeeded = FfmpegEngine.GetGrayBytesFromVideo(entry, new List<float> { 0.9f }, maxSamplingDurationSeconds: 0, extendedLogging: false);
		}
		finally {
			Logger.Instance.LogEntryAdded -= Capture;
		}

		Assert.False(succeeded);
		Assert.True(entry.Flags.Has(EntryFlags.ThumbnailError));
		Assert.DoesNotContain(messages, m => m.Contains("hardware decode failure", StringComparison.OrdinalIgnoreCase));
		Assert.DoesNotContain(messages, m => m.Contains("Failed to retrieve graybytes", StringComparison.OrdinalIgnoreCase));
		Assert.Contains(messages, m => m.Contains("Skipping process-mode retry", StringComparison.Ordinal));
	}
}
