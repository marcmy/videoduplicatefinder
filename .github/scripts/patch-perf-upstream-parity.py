from pathlib import Path


def replace_once(text: str, old: str, new: str, description: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"Expected exactly one {description} anchor, found {count}")
    return text.replace(old, new, 1)


def replace_once_if_missing(
    text: str,
    marker: str,
    old: str,
    new: str,
    description: str,
) -> str:
    if marker in text:
        return text
    return replace_once(text, old, new, description)


def method_slice(text: str, start_marker: str, end_marker: str) -> tuple[int, int, str]:
    start = text.find(start_marker)
    if start < 0:
        raise RuntimeError(f"Unable to locate method start: {start_marker}")
    end = text.find(end_marker, start)
    if end < 0:
        raise RuntimeError(f"Unable to locate method end: {end_marker}")
    return start, end, text[start:end]


def patch_non_ai_corrupt_fast_fail() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
    text = path.read_text(encoding="utf-8")

    start, end, block = method_slice(
        text,
        "\t\tstatic unsafe bool TryGetGrayBytesFromVideoNativeBatch(",
        "\n\t\t/// <summary>\n\t\t/// Extracts one 32x32 grayscale frame per position",
    )

    old_signature = (
        "\t\tstatic unsafe bool TryGetGrayBytesFromVideoNativeBatch(FileEntry videoFile, "
        "List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging, "
        "List<GrayByteResult> results, bool allowD3D11GpuScale = true, "
        "bool forceCpuDecode = false, string? forcedCpuPolicy = null) {"
    )
    new_signature = (
        "\t\tstatic unsafe bool TryGetGrayBytesFromVideoNativeBatch(FileEntry videoFile, "
        "List<float> positions, double maxSamplingDurationSeconds, bool extendedLogging, "
        "List<GrayByteResult> results, out FfmpegErrorCategory failureCategory, "
        "out AVHWDeviceType failureHardwareDeviceType, bool allowD3D11GpuScale = true, "
        "bool forceCpuDecode = false, string? forcedCpuPolicy = null) {"
    )
    block = replace_once_if_missing(
        block,
        "out FfmpegErrorCategory failureCategory",
        old_signature,
        new_signature,
        "non-AI native batch signature",
    )

    init_old = (
        "\t\t\tAVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;\n"
        "\t\t\ttry {"
    )
    init_new = (
        "\t\t\tAVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;\n"
        "\t\t\tfailureCategory = FfmpegErrorCategory.Unknown;\n"
        "\t\t\tfailureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;\n"
        "\t\t\ttry {"
    )
    block = replace_once_if_missing(
        block,
        "failureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;",
        init_old,
        init_new,
        "non-AI native batch failure initialization",
    )

    probe_old = (
        "bool cpuProbeSucceeded = TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, "
        "maxSamplingDurationSeconds, extendedLogging, results, allowD3D11GpuScale: false, "
        "forceCpuDecode: true, forcedCpuPolicy: \"d3d11-adaptive-cpu-probe\");"
    )
    probe_new = (
        "bool cpuProbeSucceeded = TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, "
        "maxSamplingDurationSeconds, extendedLogging, results, out _, out _, "
        "allowD3D11GpuScale: false, forceCpuDecode: true, "
        "forcedCpuPolicy: \"d3d11-adaptive-cpu-probe\");"
    )
    if probe_old in block:
        block = block.replace(probe_old, probe_new, 1)

    d3d11_old = (
        "return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, "
        "maxSamplingDurationSeconds, extendedLogging, results, allowD3D11GpuScale: false, "
        "forceCpuDecode: true);"
    )
    d3d11_new = (
        "return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, "
        "maxSamplingDurationSeconds, extendedLogging, results, out failureCategory, "
        "out failureHardwareDeviceType, allowD3D11GpuScale: false, forceCpuDecode: true);"
    )
    if d3d11_old in block:
        block = block.replace(d3d11_old, d3d11_new, 1)

    hardware_old = (
        "return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, "
        "maxSamplingDurationSeconds, extendedLogging, results, allowD3D11GpuScale: false, "
        "forceCpuDecode: true, forcedCpuPolicy: \"hardware-decode-failure-cpu-retry\");"
    )
    hardware_new = (
        "return TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, "
        "maxSamplingDurationSeconds, extendedLogging, results, out failureCategory, "
        "out failureHardwareDeviceType, allowD3D11GpuScale: false, forceCpuDecode: true, "
        "forcedCpuPolicy: \"hardware-decode-failure-cpu-retry\");"
    )
    if hardware_old in block:
        block = block.replace(hardware_old, hardware_new, 1)

    catch_old = "\t\t\tcatch (Exception e) {\n\t\t\t\tif (e is TiledHeifRequiresProcessException) {"
    catch_new = (
        "\t\t\tcatch (Exception e) {\n"
        "\t\t\t\tstring diagnostics = FfmpegLogCapture.GetRecent();\n"
        "\t\t\t\tfailureCategory = FfmpegErrorClassifier.Categorize(\n"
        "\t\t\t\t\tdiagnostics.Length > 0 ? $\"{diagnostics} {e.Message}\" : e.Message);\n"
        "\t\t\t\tfailureHardwareDeviceType = hardwareDeviceType;\n"
        "\t\t\t\tif (e is TiledHeifRequiresProcessException) {"
    )
    block = replace_once_if_missing(
        block,
        "failureCategory = FfmpegErrorClassifier.Categorize(",
        catch_old,
        catch_new,
        "non-AI native batch failure classification",
    )

    text = text[:start] + block + text[end:]

    caller_start, caller_end, caller = method_slice(
        text,
        "\t\tinternal static bool GetGrayBytesFromVideo(FileEntry videoFile, List<float> positions,",
        "\n\t\t// Markers for FFmpeg PNG demuxer false-positives",
    )

    call_old = (
        "\t\t\tList<GrayByteResult> stagedResults = new(missingPositions);\n"
        "\t\t\tif (nativeGrayByteState == \"available\" && "
        "TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, "
        "extendedLogging, stagedResults)) {"
    )
    call_new = (
        "\t\t\tList<GrayByteResult> stagedResults = new(missingPositions);\n"
        "\t\t\tFfmpegErrorCategory nativeFailureCategory = FfmpegErrorCategory.Unknown;\n"
        "\t\t\tAVHWDeviceType nativeFailureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;\n"
        "\t\t\tif (nativeGrayByteState == \"available\" && "
        "TryGetGrayBytesFromVideoNativeBatch(videoFile, positions, maxSamplingDurationSeconds, "
        "extendedLogging, stagedResults, out nativeFailureCategory, "
        "out nativeFailureHardwareDeviceType)) {"
    )
    caller = replace_once_if_missing(
        caller,
        "nativeFailureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;",
        call_old,
        call_new,
        "non-AI native batch caller",
    )

    skip_marker = "Skipping process-mode retry for '{videoFile.Path}'"
    if skip_marker not in caller:
        before_telemetry = (
            "\t\t\tif (ShouldLogGrayByteScanTelemetry(extendedLogging) && "
            "nativeGrayByteState != \"available\")"
        )
        fast_fail = (
            "\t\t\tif (nativeGrayByteState == \"available\" &&\n"
            "\t\t\t\tShouldSkipProcessRetryForCorruptFile(\n"
            "\t\t\t\t\tnativeFailureCategory,\n"
            "\t\t\t\t\tnativeFailureHardwareDeviceType)) {\n"
            "\t\t\t\tvideoFile.Flags.Set(EntryFlags.ThumbnailError);\n"
            "\t\t\t\tLogger.Instance.Info(\n"
            "\t\t\t\t\t$\"Skipping process-mode retry for '{videoFile.Path}': the native software decode failure indicates a truncated or corrupt file, and the FFmpeg process would run the same decoder over the same damaged bitstream.\");\n"
            "\t\t\t\treturn false;\n"
            "\t\t\t}\n"
            + before_telemetry
        )
        caller = replace_once(
            caller,
            before_telemetry,
            fast_fail,
            "non-AI corrupt fast-fail insertion",
        )

    text = text[:caller_start] + caller + text[caller_end:]
    path.write_text(text, encoding="utf-8")


def patch_ai_corrupt_fast_fail() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.AiCompat.cs")
    text = path.read_text(encoding="utf-8")

    start, end, block = method_slice(
        text,
        "\t\tstatic unsafe bool TryExtractGrayAndAiNativeBatch(",
        "\n\t\tstatic unsafe byte[] ExtractPackedRgbFrame(",
    )

    sig_old = (
        "\t\tstatic unsafe bool TryExtractGrayAndAiNativeBatch(\n"
        "\t\t\tFileEntry videoFile,\n"
        "\t\t\tList<float> positions,\n"
        "\t\t\tdouble maxSamplingDurationSeconds,\n"
        "\t\t\tglobal::VDF.Core.AI.IEmbeddingFrameSink embeddingSink,\n"
        "\t\t\tbool extendedLogging,\n"
        "\t\t\tbool forceCpuDecode = false) {"
    )
    sig_new = (
        "\t\tstatic unsafe bool TryExtractGrayAndAiNativeBatch(\n"
        "\t\t\tFileEntry videoFile,\n"
        "\t\t\tList<float> positions,\n"
        "\t\t\tdouble maxSamplingDurationSeconds,\n"
        "\t\t\tglobal::VDF.Core.AI.IEmbeddingFrameSink embeddingSink,\n"
        "\t\t\tbool extendedLogging,\n"
        "\t\t\tout FfmpegErrorCategory failureCategory,\n"
        "\t\t\tout AVHWDeviceType failureHardwareDeviceType,\n"
        "\t\t\tbool forceCpuDecode = false) {"
    )
    block = replace_once_if_missing(
        block,
        "out FfmpegErrorCategory failureCategory",
        sig_old,
        sig_new,
        "AI native batch signature",
    )

    init_old = (
        "\t\t\tAVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;\n"
        "\t\t\ttry {"
    )
    init_new = (
        "\t\t\tAVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;\n"
        "\t\t\tfailureCategory = FfmpegErrorCategory.Unknown;\n"
        "\t\t\tfailureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;\n"
        "\t\t\ttry {"
    )
    block = replace_once_if_missing(
        block,
        "failureHardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;",
        init_old,
        init_new,
        "AI native batch failure initialization",
    )

    catch_old = "\t\t\tcatch (Exception ex) {\n\t\t\t\tif (ex is TiledHeifRequiresProcessException) {"
    catch_new = (
        "\t\t\tcatch (Exception ex) {\n"
        "\t\t\t\tstring diagnostics = FfmpegLogCapture.GetRecent();\n"
        "\t\t\t\tfailureCategory = FfmpegErrorClassifier.Categorize(\n"
        "\t\t\t\t\tdiagnostics.Length > 0 ? $\"{diagnostics} {ex.Message}\" : ex.Message);\n"
        "\t\t\t\tfailureHardwareDeviceType = hardwareDeviceType;\n"
        "\t\t\t\tif (ex is TiledHeifRequiresProcessException) {"
    )
    block = replace_once_if_missing(
        block,
        "failureCategory = FfmpegErrorClassifier.Categorize(",
        catch_old,
        catch_new,
        "AI native batch failure classification",
    )

    recurse_old = (
        "\t\t\t\t\treturn TryExtractGrayAndAiNativeBatch(\n"
        "\t\t\t\t\t\tvideoFile,\n"
        "\t\t\t\t\t\tpositions,\n"
        "\t\t\t\t\t\tmaxSamplingDurationSeconds,\n"
        "\t\t\t\t\t\tembeddingSink,\n"
        "\t\t\t\t\t\textendedLogging,\n"
        "\t\t\t\t\t\tforceCpuDecode: true);"
    )
    recurse_new = (
        "\t\t\t\t\treturn TryExtractGrayAndAiNativeBatch(\n"
        "\t\t\t\t\t\tvideoFile,\n"
        "\t\t\t\t\t\tpositions,\n"
        "\t\t\t\t\t\tmaxSamplingDurationSeconds,\n"
        "\t\t\t\t\t\tembeddingSink,\n"
        "\t\t\t\t\t\textendedLogging,\n"
        "\t\t\t\t\t\tout failureCategory,\n"
        "\t\t\t\t\t\tout failureHardwareDeviceType,\n"
        "\t\t\t\t\t\tforceCpuDecode: true);"
    )
    if recurse_old in block:
        block = block.replace(recurse_old, recurse_new, 1)

    text = text[:start] + block + text[end:]

    caller_start, caller_end, caller = method_slice(
        text,
        "\t\tinternal static bool GetGrayBytesFromVideo(\n",
        "\n\t\tstatic List<GrayByteRequest> GetPendingAiNativeRequests(",
    )

    old_native_call = (
        "\t\t\tif (ShouldUseNativeBinding)\n"
        "\t\t\t\tTryExtractGrayAndAiNativeBatch(\n"
        "\t\t\t\t\tvideoFile,\n"
        "\t\t\t\t\tpositions,\n"
        "\t\t\t\t\tmaxSamplingDurationSeconds,\n"
        "\t\t\t\t\tembeddingSink,\n"
        "\t\t\t\t\textendedLogging);"
    )
    new_native_call = (
        "\t\t\tbool skipRemainingAiProcessRetries = false;\n"
        "\t\t\tif (ShouldUseNativeBinding) {\n"
        "\t\t\t\tbool nativeSucceeded = TryExtractGrayAndAiNativeBatch(\n"
        "\t\t\t\t\tvideoFile,\n"
        "\t\t\t\t\tpositions,\n"
        "\t\t\t\t\tmaxSamplingDurationSeconds,\n"
        "\t\t\t\t\tembeddingSink,\n"
        "\t\t\t\t\textendedLogging,\n"
        "\t\t\t\t\tout FfmpegErrorCategory nativeFailureCategory,\n"
        "\t\t\t\t\tout AVHWDeviceType nativeFailureHardwareDeviceType);\n"
        "\t\t\t\tif (!nativeSucceeded &&\n"
        "\t\t\t\t\tShouldSkipProcessRetryForCorruptFile(\n"
        "\t\t\t\t\t\tnativeFailureCategory,\n"
        "\t\t\t\t\t\tnativeFailureHardwareDeviceType)) {\n"
        "\t\t\t\t\tbool allGrayAvailable = positions.All(position => {\n"
        "\t\t\t\t\t\tdouble key = videoFile.GetGrayBytesIndex(\n"
        "\t\t\t\t\t\t\tposition, maxSamplingDurationSeconds);\n"
        "\t\t\t\t\t\treturn videoFile.grayBytes.TryGetValue(key, out byte[]? bytes) &&\n"
        "\t\t\t\t\t\t\tbytes != null;\n"
        "\t\t\t\t\t});\n"
        "\t\t\t\t\tif (!allGrayAvailable) {\n"
        "\t\t\t\t\t\tvideoFile.Flags.Set(EntryFlags.ThumbnailError);\n"
        "\t\t\t\t\t\tLogger.Instance.Info(\n"
        "\t\t\t\t\t\t\t$\"Skipping process-mode retry for '{videoFile.Path}': the native software decode failure indicates a truncated or corrupt file, and required gray samples are still missing.\");\n"
        "\t\t\t\t\t\treturn false;\n"
        "\t\t\t\t\t}\n"
        "\t\t\t\t\t// Gray matching remains valid; abstain from missing AI embeddings rather\n"
        "\t\t\t\t\t// than grinding the same damaged bitstream through one process per sample.\n"
        "\t\t\t\t\tskipRemainingAiProcessRetries = true;\n"
        "\t\t\t\t}\n"
        "\t\t\t}"
    )
    caller = replace_once_if_missing(
        caller,
        "bool skipRemainingAiProcessRetries = false;",
        old_native_call,
        new_native_call,
        "AI native caller corrupt fast-fail",
    )

    embedding_old = "\t\t\t\tif (embeddingSink.WantsEmbedding(videoFile, position)) {"
    embedding_new = (
        "\t\t\t\tif (!skipRemainingAiProcessRetries &&\n"
        "\t\t\t\t\tembeddingSink.WantsEmbedding(videoFile, position)) {"
    )
    if "!skipRemainingAiProcessRetries &&" not in caller:
        caller = replace_once(
            caller,
            embedding_old,
            embedding_new,
            "AI process retry guard",
        )

    text = text[:caller_start] + caller + text[caller_end:]
    path.write_text(text, encoding="utf-8")


patch_non_ai_corrupt_fast_fail()
patch_ai_corrupt_fast_fail()
