// /*
 //    Copyright (C) 2026 0x90d
 //    This file is part of VideoDuplicateFinder
 //    VideoDuplicateFinder is free software: you can redistribute it and/or modify
 //    it under the terms of the GNU Affero General Public License as published by
 //    the Free Software Foundation, either version 3 of the License, or
 //    (at your option) any later version.
 //    VideoDuplicateFinder is distributed in the hope that it will be useful,
 //    but WITHOUT ANY WARRANTY without even the implied warranty of
 //    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 //    GNU Affero General Public License for more details.
 //    You should have received a copy of the GNU Affero General Public License
 //    along with VideoDuplicateFinder.  If not, see <http://www.gnu.org/licenses/>.
 // */
 //

using VDF.Core.FFTools;
using VDF.Core.FFTools.FFmpegNative;
using VDF.TestSupport;

namespace VDF.IntegrationTests.Fixtures;

/// <summary>
/// Per-class generated pathological media corpus. The test class still belongs to the
/// shared "Ffmpeg" collection so FfmpegEngine's process-wide static state is never mutated
/// concurrently with the rest of the FFmpeg integration suite.
/// </summary>
public sealed class EvilMediaFixture : IDisposable {
	public string TempDir { get; }
	public bool FfmpegCliAvailable { get; }
	public bool NativeBindingAvailable { get; }
	public string? FfmpegNotFoundReason { get; }

	public string? BaseH264 { get; }
	public string? LongGopH264 { get; }
	public string? VfrH264 { get; }
	public string? OddDimensions { get; }
	public string? VeryShortH264 { get; }
	public string? RotatedH264 { get; }
	public string? TruncatedH264 { get; }
	public string? ResolutionChangeH264 { get; }

	public EvilMediaFixture() {
		TempDir = Path.Combine(Path.GetTempPath(), $"vdf_evil_media_{Guid.NewGuid():N}");
		Directory.CreateDirectory(TempDir);

		string ffmpegPath = FfmpegEngine.FFmpegPath;
		FfmpegCliAvailable = !string.IsNullOrEmpty(ffmpegPath) && File.Exists(ffmpegPath);
		NativeBindingAvailable = FFmpegHelper.DoFFmpegLibraryFilesExist;
		if (!FfmpegCliAvailable) {
			FfmpegNotFoundReason = "FFmpeg not found; evil-media integration tests require ffmpeg.exe in PATH.";
			if (Environment.GetEnvironmentVariable("CI") != null)
				throw new InvalidOperationException(FfmpegNotFoundReason);
			return;
		}

		string basePath = Path.Combine(TempDir, "base_h264.mp4");
		if (TestVideoGenerator.GenerateH264_8bit(ffmpegPath, basePath))
			BaseH264 = basePath;

		string longGop = Path.Combine(TempDir, "h264_long_gop.mp4");
		if (EvilMediaGenerator.GenerateLongGopH264(ffmpegPath, longGop))
			LongGopH264 = longGop;

		string vfr = Path.Combine(TempDir, "h264_vfr.mp4");
		if (EvilMediaGenerator.GenerateVfrH264(ffmpegPath, vfr))
			VfrH264 = vfr;

		string odd = Path.Combine(TempDir, "odd_321x241_ffv1.mkv");
		if (EvilMediaGenerator.GenerateOddDimensions(ffmpegPath, odd))
			OddDimensions = odd;

		string tiny = Path.Combine(TempDir, "h264_120ms.mp4");
		if (EvilMediaGenerator.GenerateVeryShortH264(ffmpegPath, tiny))
			VeryShortH264 = tiny;

		if (BaseH264 != null) {
			string rotated = Path.Combine(TempDir, "h264_rotation_90.mp4");
			if (EvilMediaGenerator.GenerateRotatedH264(ffmpegPath, BaseH264, rotated))
				RotatedH264 = rotated;

			string truncated = Path.Combine(TempDir, "h264_faststart_truncated.mp4");
			if (EvilMediaGenerator.GenerateTruncatedFastStartH264(ffmpegPath, BaseH264, truncated))
				TruncatedH264 = truncated;
		}

		string resolutionChange = Path.Combine(TempDir, "h264_resolution_change.ts");
		if (EvilMediaGenerator.GenerateResolutionChangingH264(ffmpegPath, resolutionChange))
			ResolutionChangeH264 = resolutionChange;
	}

	public void Dispose() {
		try {
			if (Directory.Exists(TempDir))
				Directory.Delete(TempDir, recursive: true);
		}
		catch { }
	}
}
