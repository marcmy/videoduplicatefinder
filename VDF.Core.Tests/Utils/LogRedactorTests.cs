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

using VDF.Core.Utils;

namespace VDF.Core.Tests.Utils;

public class LogRedactorTests {
	static LogRedactor NewRedactor(params string[] sensitiveFolders) => new(new LogRedactorOptions {
		UserProfile = @"C:\Users\alice",
		UserName = "alice",
		MachineName = "DESKTOP-ALICE7",
		SensitiveFolders = sensitiveFolders,
		ProfileToken = "%USERPROFILE%",
	});

	[Fact]
	public void WindowsPath_BecomesNumberedToken_KeepingDriveAndExtension() {
		var r = NewRedactor();
		string output = r.Redact(@"10:00:01 => [ERROR] Unhandled error processing 'D:\Movies\Holiday\Beach Day.mkv': System.IO.IOException");
		Assert.Equal(@"10:00:01 => [ERROR] Unhandled error processing 'D:\video_0001.mkv': System.IO.IOException", output);
	}

	[Fact]
	public void SamePath_GetsSameToken_CaseInsensitively_AcrossCalls() {
		var r = NewRedactor();
		string first = r.Redact(@"Native FFmpeg timing on 'D:\Movies\a.mp4' @ 00:01:00: mode=gray32");
		string other = r.Redact(@"Native FFmpeg timing on 'E:\Other\b.mp4' @ 00:01:00: mode=gray32");
		string again = r.Redact(@"Failed using native FFmpeg binding on 'd:\movies\A.MP4', try switching to process mode.");
		Assert.Contains(@"D:\video_0001.mp4", first);
		Assert.Contains(@"E:\video_0002.mp4", other);
		Assert.Contains(@"D:\video_0001.MP4", again);
		Assert.Equal(2, r.RedactedFileCount);
	}

	[Fact]
	public void NumbersFollowOrderOfAppearance_LikeTheScript() {
		string input = string.Join("\n",
			@"Moved file relinked (analysis reused): 'D:\Old\x.avi' -> 'D:\New\x.avi'",
			@"Resampling 'D:\Old\x.avi': 2 cached frame(s) had a legacy size and were discarded.");
		string output = NewRedactor().Redact(input);
		Assert.Equal(string.Join("\n",
			@"Moved file relinked (analysis reused): 'D:\video_0001.avi' -> 'D:\video_0002.avi'",
			@"Resampling 'D:\video_0001.avi': 2 cached frame(s) had a legacy size and were discarded."), output);
	}

	[Fact]
	public void UncPath_HidesHostShareAndName() {
		string output = NewRedactor().Redact(@"Skipped file '\\nas-living\media\TV\Show S01E02.m2ts' because of System.UnauthorizedAccessException");
		Assert.Equal(@"Skipped file '\\server\video_0001.m2ts' because of System.UnauthorizedAccessException", output);
	}

	[Fact]
	public void QuotedPathInsideFfmpegArguments() {
		string output = NewRedactor().Redact(
			@"ffmpeg -hide_banner -loglevel error -hwaccel d3d11va -ss 00:00:12.500 -i ""D:\Videos\My Clip [1080p] (2021).webm"" -frames:v 1 -vf scale=32:32 -f rawvideo -");
		Assert.Equal(
			@"ffmpeg -hide_banner -loglevel error -hwaccel d3d11va -ss 00:00:12.500 -i ""D:\video_0001.webm"" -frames:v 1 -vf scale=32:32 -f rawvideo -",
			output);
	}

	[Fact]
	public void FfmpegStderr_KeepsCodecDetails() {
		string input = @"Input #0, mov,mp4,m4a,3gp,3g2,mj2, from 'F:\Clips\Ünïcödé — 東京 夜景.mov':  Stream #0:0: Video: hevc (Main 10) (hvc1 / 0x31637668), yuv420p10le, 3840x2160";
		string output = NewRedactor().Redact(input);
		Assert.Equal(@"Input #0, mov,mp4,m4a,3gp,3g2,mj2, from 'F:\video_0001.mov':  Stream #0:0: Video: hevc (Main 10) (hvc1 / 0x31637668), yuv420p10le, 3840x2160", output);
	}

	[Fact]
	public void NativeDecodeDiagnostics_WithFfmpegTail() {
		string input = @"Native FFmpeg extraction hit a hardware decode failure on 'G:\Камера\Видео 01.mp4', retrying with CPU decode. hwPolicy=Auto. Reason: Failed to decode frame FFmpeg: [h264 @ 000001c2] Failed setup for format d3d11: hwaccel initialisation returned error.";
		string output = NewRedactor().Redact(input);
		Assert.Equal(@"Native FFmpeg extraction hit a hardware decode failure on 'G:\video_0001.mp4', retrying with CPU decode. hwPolicy=Auto. Reason: Failed to decode frame FFmpeg: [h264 @ 000001c2] Failed setup for format d3d11: hwaccel initialisation returned error.", output);
	}

	[Fact]
	public void AiPartialPassMessage() {
		string output = NewRedactor().Redact(@"AI partial detection: 'D:\Rips\film.vob' is a 'mpeg' file, which has no index to seek keyframes by; using the full FFmpeg sweep.");
		Assert.Equal(@"AI partial detection: 'D:\video_0001.vob' is a 'mpeg' file, which has no index to seek keyframes by; using the full FFmpeg sweep.", output);
	}

	[Fact]
	public void BareFileName_ReusesNumberOfItsFullPath_EvenWhenItComesFirst() {
		string input = string.Join("\n",
			@"[ChromaprintEngine] Concert [Live] 2019.mkv: no audio stream",
			@"[ChromaprintEngine] Failed on 'D:\Music Videos\Concert [Live] 2019.mkv': timeout");
		string output = NewRedactor().Redact(input);
		Assert.Equal(string.Join("\n",
			@"[ChromaprintEngine] video_0001.mkv: no audio stream",
			@"[ChromaprintEngine] Failed on 'D:\video_0001.mkv': timeout"), output);
	}

	[Fact]
	public void BareFileName_WithoutFullPath_TaggedQuotedAndSingleWord() {
		var r = NewRedactor();
		Assert.Equal("[ChromaprintEngine] video_0001.mp4: exit code 1", r.Redact("[ChromaprintEngine] Some Show S01E01.mp4: exit code 1"));
		Assert.Equal("opened 'video_0002.avi' ok", r.Redact("opened 'Another one.avi' ok"));
		Assert.Equal("file=video_0003.ts size=10", r.Redact("file=recording_2024-05-01.ts size=10"));
		Assert.Equal("again [Tag] video_0001.mp4", r.Redact("again [Tag] Some Show S01E01.mp4"));
	}

	[Fact]
	public void ImagesGetImageTokens() {
		string output = NewRedactor().Redact(@"ERROR: Graybytes too dark of: E:\Photos\2023\IMG_0042.HEIC");
		Assert.Equal(@"ERROR: Graybytes too dark of: E:\image_0001.HEIC", output);
	}

	[Fact]
	public void FoldersOfRedactedFiles_AreRedactedElsewhere() {
		string input = string.Join("\n",
			@"Unhandled error processing 'D:\Private\Family\clip.mp4': boom",
			@"Search directory not found or inaccessible, skipping: 'D:\Private\Family'.",
			@"Dense embedding cache unreadable, rebuilding (bad header): D:\Private\notes.bin");
		string output = NewRedactor().Redact(input);
		Assert.DoesNotContain("Private", output);
		Assert.DoesNotContain("Family", output);
		Assert.Contains(@"skipping: 'D:\folder_0001'.", output);
		Assert.Contains(@": D:\folder_0002\notes.bin", output);
	}

	[Fact]
	public void SensitiveFolders_AreRedactedWithoutAnyFileInThem() {
		string output = NewRedactor(@"\\nas\secret share\", @"E:\Stuff").Redact(
			@"Search directory not found or inaccessible, skipping: '\\nas\secret share'. Also E:\Stuff\sub is empty; E:\Stuffing stays.");
		Assert.Equal(@"Search directory not found or inaccessible, skipping: '\\server\folder_0001'. Also E:\folder_0002\sub is empty; E:\Stuffing stays.", output);
	}

	[Fact]
	public void ProfilePathAndNames_AreReplaced() {
		string input = string.Join("\n",
			@"Data folder: C:\Users\alice\scoop\persist\videoduplicatefinder-perf (Scoop persist (videoduplicatefinder-perf))",
			@"ffmpeg path:        C:\Users\Alice\scoop\apps\ffmpeg\current\bin\ffmpeg.exe",
			@"Database saved for alice on DESKTOP-ALICE7");
		string output = NewRedactor().Redact(input);
		Assert.Equal(string.Join("\n",
			@"Data folder: %USERPROFILE%\scoop\persist\videoduplicatefinder-perf (Scoop persist (videoduplicatefinder-perf))",
			@"ffmpeg path:        %USERPROFILE%\scoop\apps\ffmpeg\current\bin\ffmpeg.exe",
			@"Database saved for <user> on <machine>"), output);
	}

	[Fact]
	public void VideoInsideProfile_KeepsProfileFolderOutOfFolderTokens() {
		var r = NewRedactor();
		string output = r.Redact(string.Join("\n",
			@"Skipped file 'C:\Users\alice\Videos\Kids\party.mp4' because of x",
			@"Settings saved to C:\Users\alice\AppData\Roaming\VDF\Settings.json"));
		Assert.Equal(string.Join("\n",
			@"Skipped file 'C:\video_0001.mp4' because of x",
			@"Settings saved to %USERPROFILE%\AppData\Roaming\VDF\Settings.json"), output);
	}

	[Fact]
	public void UnixPaths_AreRedacted_UrlsAreNot() {
		var r = new LogRedactor(new LogRedactorOptions { UserProfile = "/home/bob", ProfileToken = "~" });
		string output = r.Redact("Failed on '/home/bob/Videos/My Film.mkv'; see https://example.com/help.mp4 and /home/bob/.config/VDF/log.txt");
		Assert.Equal("Failed on '/video_0001.mkv'; see https://example.com/help.mp4 and ~/.config/VDF/log.txt", output);
	}

	[Fact]
	public void ForwardSlashDrivePath() {
		string output = NewRedactor().Redact("[mov,mp4 @ 0x1] moov atom not found: D:/Downloads/broken file.mp4");
		Assert.Equal(@"[mov,mp4 @ 0x1] moov atom not found: D:\video_0001.mp4", output);
	}

	[Fact]
	public void NonMediaText_IsLeftAlone() {
		const string line = "10:00:00 => Drive 'D:\\': 1,234 file(s), concurrency 2 (slow, auto) Version 4.1.2; at VDF.Core.ScanEngine.Run() in D:\\a\\vdf\\VDF.Core\\ScanEngine.cs:line 42";
		Assert.Equal(line, NewRedactor().Redact(line));
	}

	[Fact]
	public void ExtensionMustEndTheName() {
		string output = NewRedactor().Redact(@"Copied D:\Media\a.mp4.part and D:\Media\b.mkv.bak");
		Assert.DoesNotContain("video_", output);
	}

	[Fact]
	public void CrLfLineEndingsSurvive() {
		string output = NewRedactor().Redact("a 'D:\\x\\y.mkv'\r\nb\r\n");
		Assert.Equal("a 'D:\\video_0001.mkv'\r\nb\r\n", output);
	}

	[Fact]
	public void RedactFile_WritesRedactedCopy_AndLeavesSourceUntouched() {
		string dir = Path.Combine(Path.GetTempPath(), "vdf-redact-" + Guid.NewGuid().ToString("N"));
		Directory.CreateDirectory(dir);
		try {
			string input = Path.Combine(dir, "log.txt");
			string output = Path.Combine(dir, "log_redacted.txt");
			const string original = "10:00:00 => [WARNING] Failed reading metadata tags from 'D:\\Films\\Secret.mkv': x\n";
			File.WriteAllText(input, original);
			var r = NewRedactor();
			r.RedactFile(input, output);
			Assert.Equal(original, File.ReadAllText(input));
			Assert.Equal("10:00:00 => [WARNING] Failed reading metadata tags from 'D:\\video_0001.mkv': x\n", File.ReadAllText(output));
			Assert.Throws<ArgumentException>(() => r.RedactFile(input, input));
		}
		finally {
			Directory.Delete(dir, true);
		}
	}
}
