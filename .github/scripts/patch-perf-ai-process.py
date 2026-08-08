from pathlib import Path


def find_method_end(text: str, start: int) -> int:
    opening = text.find("{", start)
    if opening < 0:
        raise RuntimeError("Method opening brace not found")
    depth = 0
    for index in range(opening, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index + 1
    raise RuntimeError("Method closing brace not found")


def patch_ai_process_fallback() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.AiCompat.cs")
    text = path.read_text(encoding="utf-8")

    method_start = text.find("\t\tinternal static bool GetGrayBytesFromVideo(\n")
    method_end = text.find("\n\t\tstatic List<GrayByteRequest> GetPendingAiNativeRequests(", method_start)
    if method_start < 0 or method_end < 0:
        raise RuntimeError("Unable to locate AI GetGrayBytesFromVideo")
    method = text[method_start:method_end]

    loop_start = method.find("\t\t\tstring? hardwareFamilyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);")
    loop_end = method.find("\n\n\t\t\tif (initiallyMissingGray.Count > 0 &&", loop_start)
    if loop_start < 0 or loop_end < 0:
        raise RuntimeError("Unable to locate AI process fallback loop")

    replacement = '''\t\t\tstring? hardwareFamilyKey = GetD3D11GrayByteAdaptiveFamilyKey(videoFile);
\t\t\tstring? hardwareCodecName = GetPrimaryVideoCodecName(videoFile);
\t\t\tfor (int i = 0; i < positions.Count; i++) {
\t\t\t\tdouble position = videoFile.GetGrayBytesIndex(positions[i], maxSamplingDurationSeconds);
\t\t\t\tbool needGray =
\t\t\t\t\t!videoFile.grayBytes.TryGetValue(position, out byte[]? currentGray) ||
\t\t\t\t\tcurrentGray == null;
\t\t\t\tbool needRgb =
\t\t\t\t\t!skipRemainingAiProcessRetries &&
\t\t\t\t\tembeddingSink.WantsEmbedding(videoFile, position);

\t\t\t\t// Upstream optimization: when both outputs are missing, split one decoded
\t\t\t\t// frame into the 32x32 gray sample and 224x224 RGB embedding instead of
\t\t\t\t// launching FFmpeg twice. The perf implementation keeps bounded process I/O,
\t\t\t\t// tiled-HEIF handling and H.264 accurate-seek recovery. User -vf arguments
\t\t\t\t// intentionally stay on the existing separate path so embedding inputs remain
\t\t\t\t// uniformly unfiltered.
\t\t\t\tif (needGray && needRgb && string.IsNullOrWhiteSpace(CustomFFArguments)) {
\t\t\t\t\t(byte[]? combinedGray, byte[]? combinedRgb) =
\t\t\t\t\t\tGetGrayAndRgb224CliBounded(
\t\t\t\t\t\t\tvideoFile.Path,
\t\t\t\t\t\t\tTimeSpan.FromSeconds(position),
\t\t\t\t\t\t\tsoftwareDecodeOnly: false,
\t\t\t\t\t\t\textendedLogging);
\t\t\t\t\tif (combinedGray != null) {
\t\t\t\t\t\tvideoFile.grayBytes[position] = combinedGray;
\t\t\t\t\t\tvideoFile.PHashes[position] =
\t\t\t\t\t\t\tpHash.PerceptualHash.ComputePHashFromGray32x32(combinedGray);

\t\t\t\t\t\tif (combinedRgb != null) {
\t\t\t\t\t\t\tembeddingSink.SubmitFrame(videoFile, position, combinedRgb);
\t\t\t\t\t\t}
\t\t\t\t\t\telse {
\t\t\t\t\t\t\t// Preserve the old safe RGB-only fallback if the split output could
\t\t\t\t\t\t\t// not produce an embedding frame. Gray work is still retained.
\t\t\t\t\t\t\tbyte[]? rgbRetry = GetAiRgb224Cli(
\t\t\t\t\t\t\t\tvideoFile.Path,
\t\t\t\t\t\t\t\tTimeSpan.FromSeconds(position),
\t\t\t\t\t\t\t\tsoftwareDecodeOnly: false,
\t\t\t\t\t\t\t\textendedLogging);
\t\t\t\t\t\t\tif (rgbRetry != null)
\t\t\t\t\t\t\t\tembeddingSink.SubmitFrame(videoFile, position, rgbRetry);
\t\t\t\t\t\t}

\t\t\t\t\t\tonSampleComplete?.Invoke(i + 1);
\t\t\t\t\t\tcontinue;
\t\t\t\t\t}
\t\t\t\t}

\t\t\t\tif (needGray) {
\t\t\t\t\tbyte[]? gray = GetThumbnail(new FfmpegSettings {
\t\t\t\t\t\tFile = videoFile.Path,
\t\t\t\t\t\tPosition = TimeSpan.FromSeconds(position),
\t\t\t\t\t\tGrayScale = 1,
\t\t\t\t\t\tHardwareFamilyKey = hardwareFamilyKey,
\t\t\t\t\t\tHardwareCodecName = hardwareCodecName,
\t\t\t\t\t}, extendedLogging);
\t\t\t\t\tif (gray == null) {
\t\t\t\t\t\tvideoFile.Flags.Set(EntryFlags.ThumbnailError);
\t\t\t\t\t\treturn false;
\t\t\t\t\t}
\t\t\t\t\tvideoFile.grayBytes[position] = gray;
\t\t\t\t\tvideoFile.PHashes[position] =
\t\t\t\t\t\tpHash.PerceptualHash.ComputePHashFromGray32x32(gray);
\t\t\t\t}

\t\t\t\tif (needRgb) {
\t\t\t\t\tbyte[]? rgb = GetAiRgb224Cli(
\t\t\t\t\t\tvideoFile.Path,
\t\t\t\t\t\tTimeSpan.FromSeconds(position),
\t\t\t\t\t\tsoftwareDecodeOnly: false,
\t\t\t\t\t\textendedLogging);
\t\t\t\t\tif (rgb != null)
\t\t\t\t\t\tembeddingSink.SubmitFrame(videoFile, position, rgb);
\t\t\t\t}

\t\t\t\tonSampleComplete?.Invoke(i + 1);
\t\t\t}'''

    method = method[:loop_start] + replacement + method[loop_end:]
    text = text[:method_start] + method + text[method_end:]

    # Keep the upstream/test-visible API but route it through the hardened perf helper.
    compat_start = text.find(
        "\t\tinternal static (byte[]? GrayBytes, byte[]? Rgb224) GetGrayAndRgb224Cli("
    )
    if compat_start < 0:
        raise RuntimeError("Unable to locate GetGrayAndRgb224Cli compatibility method")

    delegated = "GetGrayAndRgb224CliBounded(" in text[compat_start:compat_start + 700]
    if not delegated:
        compat_end = find_method_end(text, compat_start)
        compat_replacement = '''\t\tinternal static (byte[]? GrayBytes, byte[]? Rgb224) GetGrayAndRgb224Cli(
\t\t\tstring file,
\t\t\tTimeSpan position,
\t\t\tbool softwareDecodeOnly,
\t\t\tbool extendedLogging) =>
\t\t\tGetGrayAndRgb224CliBounded(
\t\t\t\tfile,
\t\t\t\tposition,
\t\t\t\tsoftwareDecodeOnly,
\t\t\t\textendedLogging);'''
        text = text[:compat_start] + compat_replacement + text[compat_end:]

    path.write_text(text, encoding="utf-8")


patch_ai_process_fallback()
