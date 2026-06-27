using VDF.Core;

namespace VDF.Core.Tests.FFTools;

public sealed class NativeFfmpegStartupPolicyTests {
	[Theory]
	[InlineData(true, false, true)]
	[InlineData(true, true, false)]
	[InlineData(false, false, false)]
	[InlineData(false, true, false)]
	public void ShouldAutoDownloadNativeRuntime_MatchesRequestedUnavailableState(
		bool useNativeBinding,
		bool nativeRuntimeExists,
		bool expected) {
		Assert.Equal(expected, NativeFfmpegStartupPolicy.ShouldAutoDownloadNativeRuntime(
			useNativeBinding,
			nativeRuntimeExists));
	}
}
