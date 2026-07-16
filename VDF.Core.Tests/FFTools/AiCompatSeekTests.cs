using VDF.Core.FFTools;

namespace VDF.Core.Tests.FFTools;

public class AiCompatSeekTests {
	[Theory]
	[InlineData("[h264 @ 000001] co located POCs unavailable")]
	[InlineData("[h264 @ 000001] mmco: unref short failure")]
	[InlineData("[H264 @ 000001] CO LOCATED POCS UNAVAILABLE")]
	public void ContainsH264ReferenceRecoveryWarning_KnownDiagnostics_ReturnsTrue(
		string error) {
		Assert.True(
			FfmpegEngine.ContainsH264ReferenceRecoveryWarning(error));
	}

	[Theory]
	[InlineData("")]
	[InlineData("[h264 @ 000001] concealing 42 DC, 42 AC, 42 MV errors")]
	[InlineData("[hevc @ 000001] Could not find ref with POC 7")]
	public void ContainsH264ReferenceRecoveryWarning_OtherText_ReturnsFalse(
		string error) {
		Assert.False(
			FfmpegEngine.ContainsH264ReferenceRecoveryWarning(error));
	}

	[Fact]
	public void BuildAiRgb224CliArguments_FastSeekPlacesSsBeforeInput() {
		List<string> arguments =
			FfmpegEngine.BuildAiRgb224CliArguments(
				@"C:\video.mp4",
				TimeSpan.FromSeconds(42),
				softwareDecodeOnly: true,
				accurateSeek: false);

		Assert.True(
			arguments.IndexOf("-ss") < arguments.IndexOf("-i"));
	}

	[Fact]
	public void BuildAiRgb224CliArguments_AccurateSeekPlacesSsAfterInput() {
		List<string> arguments =
			FfmpegEngine.BuildAiRgb224CliArguments(
				@"C:\video.mp4",
				TimeSpan.FromSeconds(42),
				softwareDecodeOnly: true,
				accurateSeek: true);

		Assert.True(
			arguments.IndexOf("-i") < arguments.IndexOf("-ss"));
	}

	[Fact]
	public void BuildAiRgb224CliArguments_StillImageDoesNotSeek() {
		List<string> arguments =
			FfmpegEngine.BuildAiRgb224CliArguments(
				@"C:\image.png",
				TimeSpan.FromSeconds(42),
				softwareDecodeOnly: true,
				accurateSeek: true);

		Assert.DoesNotContain("-ss", arguments);
	}
}
