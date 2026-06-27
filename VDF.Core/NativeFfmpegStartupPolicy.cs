namespace VDF.Core;

internal static class NativeFfmpegStartupPolicy {
	internal static bool ShouldAutoDownloadNativeRuntime(bool useNativeBinding, bool nativeRuntimeExists) =>
		useNativeBinding && !nativeRuntimeExists;
}
