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

using System.Diagnostics;

namespace VDF.TestSupport;

/// <summary>
/// Generates tiny deterministic media files that deliberately exercise awkward decoder,
/// seeking and metadata paths. These files are created at test time instead of storing
/// opaque binary fixtures in the repository.
/// </summary>
public static class EvilMediaGenerator {
	static bool RunFfmpeg(string ffmpegPath, IReadOnlyList<string> arguments, int timeoutMs = 60_000) {
		var psi = new ProcessStartInfo {
			FileName = ffmpegPath,
			CreateNoWindow = true,
			RedirectStandardOutput = true,
			RedirectStandardError = true,
			UseShellExecute = false,
			WorkingDirectory = Path.GetDirectoryName(ffmpegPath)!,
		};
		foreach (string argument in arguments)
			psi.ArgumentList.Add(argument);

		try {
			using var process = Process.Start(psi)!;
			Task<string> stderrTask = process.StandardError.ReadToEndAsync();
			Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync();
			if (!process.WaitForExit(timeoutMs)) {
				try { process.Kill(entireProcessTree: true); } catch { }
				return false;
			}
			Task.WaitAll(new Task[] { stderrTask, stdoutTask }, TimeSpan.FromSeconds(5));
			return process.ExitCode == 0;
		}
		catch {
			return false;
		}
	}

	public static bool GenerateLongGopH264(string ffmpegPath, string outputPath) =>
		RunFfmpeg(ffmpegPath, [
			"-y",
			"-f", "lavfi",
			"-i", "testsrc2=duration=4:size=320x240:rate=25",
			"-c:v", "libx264",
			"-preset", "ultrafast",
			"-crf", "23",
			"-pix_fmt", "yuv420p",
			"-x264-params", "keyint=100:min-keyint=100:scenecut=0:open-gop=0",
			outputPath,
		]);

	/// <summary>
	/// Two one-second sections with different source frame rates. The concat filter keeps
	/// their timestamps and -fps_mode vfr prevents FFmpeg from normalizing them back to CFR.
	/// </summary>
	public static bool GenerateVfrH264(string ffmpegPath, string outputPath) =>
		RunFfmpeg(ffmpegPath, [
			"-y",
			"-f", "lavfi",
			"-i", "testsrc2=duration=1:size=320x240:rate=12",
			"-f", "lavfi",
			"-i", "smptebars=duration=1:size=320x240:rate=30",
			"-filter_complex", "[0:v]settb=AVTB[v0];[1:v]settb=AVTB[v1];[v0][v1]concat=n=2:v=1:a=0[v]",
			"-map", "[v]",
			"-fps_mode", "vfr",
			"-c:v", "libx264",
			"-preset", "ultrafast",
			"-crf", "23",
			"-pix_fmt", "yuv420p",
			outputPath,
		]);

	/// <summary>
	/// 321x241 FFV1/yuv444p deliberately avoids the even-dimension assumptions of 4:2:0.
	/// </summary>
	public static bool GenerateOddDimensions(string ffmpegPath, string outputPath) =>
		RunFfmpeg(ffmpegPath, [
			"-y",
			"-f", "lavfi",
			"-i", "testsrc=duration=2:size=321x241:rate=25",
			"-c:v", "ffv1",
			"-pix_fmt", "yuv444p",
			outputPath,
		]);

	public static bool GenerateVeryShortH264(string ffmpegPath, string outputPath) =>
		RunFfmpeg(ffmpegPath, [
			"-y",
			"-f", "lavfi",
			"-i", "testsrc2=duration=0.12:size=320x240:rate=25",
			"-c:v", "libx264",
			"-preset", "ultrafast",
			"-crf", "23",
			"-pix_fmt", "yuv420p",
			outputPath,
		]);

	/// <summary>
	/// Remuxes a clean clip while applying an input display-matrix rotation. Using
	/// -display_rotation instead of a rotate metadata tag guarantees real side data on
	/// current FFmpeg.
	/// </summary>
	public static bool GenerateRotatedH264(string ffmpegPath, string cleanInputPath, string outputPath) =>
		RunFfmpeg(ffmpegPath, [
			"-y",
			"-display_rotation", "90",
			"-i", cleanInputPath,
			"-map", "0",
			"-c", "copy",
			outputPath,
		]);

	/// <summary>
	/// Creates a fast-start MP4 (moov before mdat) and then physically chops the tail.
	/// FFprobe can still read metadata, while late decode encounters missing packet data.
	/// </summary>
	public static bool GenerateTruncatedFastStartH264(
		string ffmpegPath,
		string cleanInputPath,
		string outputPath,
		double keepFraction = 0.72) {
		string staging = outputPath + ".faststart.mp4";
		try {
			if (!RunFfmpeg(ffmpegPath, [
				"-y",
				"-i", cleanInputPath,
				"-c", "copy",
				"-movflags", "+faststart",
				staging,
			]))
				return false;

			byte[] bytes = File.ReadAllBytes(staging);
			if (bytes.Length < 256)
				return false;
			int keep = Math.Clamp((int)(bytes.Length * keepFraction), 128, bytes.Length - 1);
			File.WriteAllBytes(outputPath, bytes[..keep]);
			return true;
		}
		catch {
			return false;
		}
		finally {
			try { File.Delete(staging); } catch { }
		}
	}

	static string FfconcatPath(string path) =>
		Path.GetFullPath(path).Replace('\\', '/').Replace("'", "'\\''");

	/// <summary>
	/// Concatenates two independently encoded H.264 Matroska segments with different SPS
	/// dimensions. The concat demuxer gives the joined stream continuous timestamps while
	/// repeat-headers preserves the second segment's new SPS/PPS. A sequential decoder must
	/// reconfigure from 320x240 to 640x360 without using stale converter metadata.
	/// </summary>
	public static bool GenerateResolutionChangingH264(string ffmpegPath, string outputPath) {
		string directory = Path.GetDirectoryName(outputPath)!;
		string stem = Path.GetFileNameWithoutExtension(outputPath);
		string segmentA = Path.Combine(directory, stem + ".320x240.mkv");
		string segmentB = Path.Combine(directory, stem + ".640x360.mkv");
		string concatList = Path.Combine(directory, stem + ".ffconcat");

		try {
			bool a = RunFfmpeg(ffmpegPath, [
				"-y",
				"-f", "lavfi",
				"-i", "testsrc2=duration=1:size=320x240:rate=25",
				"-c:v", "libx264",
				"-preset", "ultrafast",
				"-crf", "23",
				"-pix_fmt", "yuv420p",
				"-x264-params", "keyint=25:min-keyint=25:scenecut=0:repeat-headers=1",
				segmentA,
			]);
			bool b = RunFfmpeg(ffmpegPath, [
				"-y",
				"-f", "lavfi",
				"-i", "smptebars=duration=1:size=640x360:rate=25",
				"-c:v", "libx264",
				"-preset", "ultrafast",
				"-crf", "23",
				"-pix_fmt", "yuv420p",
				"-x264-params", "keyint=25:min-keyint=25:scenecut=0:repeat-headers=1",
				segmentB,
			]);
			if (!a || !b)
				return false;

			File.WriteAllText(
				concatList,
				$"ffconcat version 1.0{Environment.NewLine}" +
				$"file '{FfconcatPath(segmentA)}'{Environment.NewLine}" +
				$"file '{FfconcatPath(segmentB)}'{Environment.NewLine}");

			return RunFfmpeg(ffmpegPath, [
				"-y",
				"-f", "concat",
				"-safe", "0",
				"-i", concatList,
				"-c", "copy",
				"-f", "matroska",
				outputPath,
			]);
		}
		catch {
			return false;
		}
		finally {
			foreach (string path in new[] { segmentA, segmentB, concatList }) {
				try { File.Delete(path); } catch { }
			}
		}
	}
}
