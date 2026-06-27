using VDF.Core.FFTools;

namespace VDF.Core.Tests.FFTools;

public sealed class FFToolsUtilsTests : IDisposable {
	readonly string tempRoot = Path.Combine(Path.GetTempPath(), "VDF-FFToolsUtilsTests", Guid.NewGuid().ToString("N"));

	public FFToolsUtilsTests() => Directory.CreateDirectory(tempRoot);

	[Fact]
	public void ScanPathDirs_ReturnsFirstMatchingExecutable() {
		string first = Path.Combine(tempRoot, "first");
		string second = Path.Combine(tempRoot, "second");
		Directory.CreateDirectory(first);
		Directory.CreateDirectory(second);

		string executableName = OperatingSystem.IsWindows() ? "ffmpeg.exe" : "ffmpeg";
		string expected = Path.Combine(second, executableName);
		File.WriteAllBytes(expected, [0]);

		string path = string.Join(Path.PathSeparator, first, second);

		Assert.Equal(expected, FFToolsUtils.ScanPathDirs(path, executableName));
	}

	[Fact]
	public void ScanPathDirs_TrimsQuotedPathEntries() {
		string bin = Path.Combine(tempRoot, "quoted bin");
		Directory.CreateDirectory(bin);

		string executableName = OperatingSystem.IsWindows() ? "ffprobe.exe" : "ffprobe";
		string expected = Path.Combine(bin, executableName);
		File.WriteAllBytes(expected, [0]);

		Assert.Equal(expected, FFToolsUtils.ScanPathDirs($"\"{bin}\"", executableName));
	}

	[Fact]
	public void ScanPathDirs_ReturnsNullWhenExecutableIsMissing() {
		string bin = Path.Combine(tempRoot, "empty");
		Directory.CreateDirectory(bin);

		string executableName = OperatingSystem.IsWindows() ? "ffmpeg.exe" : "ffmpeg";

		Assert.Null(FFToolsUtils.ScanPathDirs(bin, executableName));
	}

	public void Dispose() {
		try {
			Directory.Delete(tempRoot, recursive: true);
		}
		catch {
			// Best-effort cleanup only.
		}
	}
}
