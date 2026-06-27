using VDF.Core.FFTools;

namespace VDF.Core.Tests.FFTools;

public sealed class FFToolsUtilsTests : IDisposable {
	readonly string tempRoot = Path.Combine(Path.GetTempPath(), "VDF-FFToolsUtilsTests", Guid.NewGuid().ToString("N"));

	public FFToolsUtilsTests() => Directory.CreateDirectory(tempRoot);

	[Fact]
	public void ResolveExecutableCandidate_ResolvesScoopShimTarget() {
		if (!OperatingSystem.IsWindows())
			return;

		string shims = Path.Combine(tempRoot, "shims");
		string appBin = Path.Combine(tempRoot, "apps", "ffmpeg-marc-shared", "current", "bin");
		Directory.CreateDirectory(shims);
		Directory.CreateDirectory(appBin);

		string shimExe = Path.Combine(shims, "ffmpeg.exe");
		string realExe = Path.Combine(appBin, "ffmpeg.exe");
		File.WriteAllBytes(shimExe, [0]);
		File.WriteAllBytes(realExe, [0]);
		File.WriteAllText(Path.Combine(shims, "ffmpeg.shim"), $"path = \"{realExe}\"\n");

		Assert.Equal(realExe, FFToolsUtils.ResolveExecutableCandidate(shimExe));
	}

	[Fact]
	public void ResolveExecutableCandidate_LeavesNormalExecutableAlone() {
		string executable = Path.Combine(tempRoot, OperatingSystem.IsWindows() ? "ffmpeg.exe" : "ffmpeg");
		File.WriteAllBytes(executable, [0]);

		Assert.Equal(executable, FFToolsUtils.ResolveExecutableCandidate(executable));
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
