using VDF.Core;
using VDF.Core.FFTools.FFmpegNative;
using VDF.IntegrationTests.Fixtures;

namespace VDF.IntegrationTests.FFTools;

[Collection("Ffmpeg")]
public sealed class NativeBindingCompatibilityTests {
	readonly FfmpegFixture fixture;

	public NativeBindingCompatibilityTests(FfmpegFixture fixture) => this.fixture = fixture;

	[SkippableFact]
	public void GeneratedBindings_LoadTheSelectedFfmpegBuild() {
		Skip.If(
			Environment.GetEnvironmentVariable("VDF_REQUIRE_NATIVE_FFMPEG") != "1",
			"Strict native-binding validation is enabled only by the FFmpeg master compatibility workflow.");

		Assert.True(
			fixture.NativeBindingAvailable && ScanEngine.NativeFFmpegExists,
			$"Generated bindings could not load the selected FFmpeg shared build. {FFmpegHelper.DescribeExpectedLibraries()}");
	}
}
