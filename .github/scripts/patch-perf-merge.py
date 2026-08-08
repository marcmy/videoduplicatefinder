from pathlib import Path


def replace_once(text: str, old: str, new: str, description: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"Expected exactly one {description} anchor, found {count}")
    return text.replace(old, new, 1)


def replace_if_present(text: str, old: str, new: str) -> str:
    if old in text:
        return text.replace(old, new, 1)
    return text


def ensure_rgb224_process_support() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
    text = path.read_text(encoding="utf-8")

    if "public bool Rgb224;" not in text:
        needle = "\t\tpublic bool SoftwareDecodeOnly;\n\t}\n}"
        replacement = (
            "\t\tpublic bool SoftwareDecodeOnly;\n"
            "\t\t/// <summary>Produce a raw 224x224 RGB24 AI embedding frame.</summary>\n"
            "\t\tpublic bool Rgb224;\n"
            "\t}\n}"
        )
        text = replace_once(text, needle, replacement, "FfmpegSettings")

    # Older refreshes kept the perf engine but lost the process-mode RGB224 branch.
    # Keep the source permanently repaired here instead of mutating it during releases.
    if "bool isRgbFrame = settings.Rgb224;" not in text:
        replacements = [
            (
                "\t\t\tbool isGrayByte = settings.GrayScale == 1;\n",
                "\t\t\tbool isGrayByte = settings.GrayScale == 1;\n"
                "\t\t\tbool isRgbFrame = settings.Rgb224;\n"
                "\t\t\tint expectedRgbBytes = global::VDF.Core.AI.OnnxEmbedder.InputSide * global::VDF.Core.AI.OnnxEmbedder.InputSide * 3;\n",
            ),
            (
                "\t\t\t\tif (ShouldAttemptNativeSingleFrameExtraction(settings)) {\n",
                "\t\t\t\tif (!isRgbFrame && ShouldAttemptNativeSingleFrameExtraction(settings)) {\n",
            ),
            (
                "\t\t\tif (isGrayByte) {\n"
                "\t\t\t\tstring vfChain = $\"scale={graySideLength}:{graySideLength}:flags=bicubic,format=gray\";\n",
                "\t\t\tif (isRgbFrame) {\n"
                "\t\t\t\tint side = global::VDF.Core.AI.OnnxEmbedder.InputSide;\n"
                "\t\t\t\tpsi.ArgumentList.Add(\"-vf\");\n"
                "\t\t\t\tpsi.ArgumentList.Add($\"scale={side}:{side}:flags=bicubic,format=rgb24\");\n"
                "\t\t\t\tpsi.ArgumentList.Add(\"-f\"); psi.ArgumentList.Add(\"rawvideo\");\n"
                "\t\t\t\tpsi.ArgumentList.Add(\"-pix_fmt\"); psi.ArgumentList.Add(\"rgb24\");\n"
                "\t\t\t}\n"
                "\t\t\telse if (isGrayByte) {\n"
                "\t\t\t\tstring vfChain = $\"scale={graySideLength}:{graySideLength}:flags=bicubic,format=gray\";\n",
            ),
            (
                "\t\t\tforeach (var item in remainingCustomArgs) psi.ArgumentList.Add(item);\n",
                "\t\t\tif (!isRgbFrame)\n"
                "\t\t\t\tforeach (var item in remainingCustomArgs) psi.ArgumentList.Add(item);\n",
            ),
            (
                "\t\t\t\telse if (isGrayByte && bytes.Length != expectedGrayBytes) {\n"
                "\t\t\t\t\terrOut.AppendLine($\"graybytes length != {expectedGrayBytes} (got {bytes.Length})\");\n"
                "\t\t\t\t\tbytes = null;\n"
                "\t\t\t\t}\n",
                "\t\t\t\telse if (isGrayByte && bytes.Length != expectedGrayBytes) {\n"
                "\t\t\t\t\terrOut.AppendLine($\"graybytes length != {expectedGrayBytes} (got {bytes.Length})\");\n"
                "\t\t\t\t\tbytes = null;\n"
                "\t\t\t\t}\n"
                "\t\t\t\telse if (isRgbFrame && bytes.Length != expectedRgbBytes) {\n"
                "\t\t\t\t\terrOut.AppendLine($\"AI frame length != {expectedRgbBytes} (got {bytes.Length})\");\n"
                "\t\t\t\t\tbytes = null;\n"
                "\t\t\t\t}\n",
            ),
        ]
        for old, new in replacements:
            text = replace_once(text, old, new, "RGB224 process support")

    path.write_text(text, encoding="utf-8")


def ensure_stream_group_compatibility() -> None:
    path = Path("VDF.Core/FFTools/FFmpegNative/VideoStreamDecoder.cs")
    text = path.read_text(encoding="utf-8")

    old_property = "\t\tpublic bool HasStreamGroups => _pFormatContext->nb_stream_groups > 0;\n"
    safe_property = "\t\tpublic bool HasStreamGroups => _pFormatContext != null && _pFormatContext->nb_stream_groups > 0;\n"
    if old_property in text:
        text = text.replace(old_property, safe_property, 1)
    elif "HasStreamGroups" not in text:
        needle = "\t\tinternal AVRational StreamSampleAspectRatio => _pFormatContext->streams[_streamIndex]->sample_aspect_ratio;\n"
        text = replace_once(text, needle, needle + safe_property, "VideoStreamDecoder stream metadata")

    path.write_text(text, encoding="utf-8")


def ensure_tiled_heif_native_refusal() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
    text = path.read_text(encoding="utf-8")

    anchors = [
        (
            "\t\t\t\tusing var vsd = new VideoStreamDecoder(videoFile.Path, hardwareDeviceType);\n"
            "\t\t\t\tNativeGrayByteTiming nativeTiming",
            "\t\t\t\tusing var vsd = new VideoStreamDecoder(videoFile.Path, hardwareDeviceType);\n"
            "\t\t\t\tThrowIfTiledHeifRequiresProcess(vsd, videoFile.Path);\n"
            "\t\t\t\tNativeGrayByteTiming nativeTiming",
        ),
        (
            "\t\t\t\t\tusing var vsd = new VideoStreamDecoder(filePath, hardwareDeviceType);\n"
            "\t\t\t\t\tVideoFrameConverter? converter",
            "\t\t\t\t\tusing var vsd = new VideoStreamDecoder(filePath, hardwareDeviceType);\n"
            "\t\t\t\t\tThrowIfTiledHeifRequiresProcess(vsd, filePath);\n"
            "\t\t\t\t\tVideoFrameConverter? converter",
        ),
        (
            "\t\t\t\t\tusing var vsd = new VideoStreamDecoder(settings.File, nativeHardwareDeviceType);\n"
            "\t\t\t\t\topenMs = phaseSw.ElapsedMilliseconds;",
            "\t\t\t\t\tusing var vsd = new VideoStreamDecoder(settings.File, nativeHardwareDeviceType);\n"
            "\t\t\t\t\tThrowIfTiledHeifRequiresProcess(vsd, settings.File);\n"
            "\t\t\t\t\topenMs = phaseSw.ElapsedMilliseconds;",
        ),
        (
            "\t\t\t\tusing var vsd = new VideoStreamDecoder(path);\n"
            "\t\t\t\tif (!vsd.TryDecodeFrame(out var srcFrame, TimeSpan.Zero))",
            "\t\t\t\tusing var vsd = new VideoStreamDecoder(path);\n"
            "\t\t\t\tThrowIfTiledHeifRequiresProcess(vsd, path);\n"
            "\t\t\t\tif (!vsd.TryDecodeFrame(out var srcFrame, TimeSpan.Zero))",
        ),
    ]
    for old, new in anchors:
        if new not in text:
            text = replace_if_present(text, old, new)

    # Expected tiled-HEIF handoff is not a native-health failure.
    batch_catch = (
        "\t\t\tcatch (Exception e) {\n"
        "\t\t\t\tif (IsNativeBindingLoadFailure(e)) {\n"
        "\t\t\t\t\tRecordNativeFailure(videoFile.Path, e);"
    )
    batch_replacement = (
        "\t\t\tcatch (Exception e) {\n"
        "\t\t\t\tif (e is TiledHeifRequiresProcessException) {\n"
        "\t\t\t\t\tLogger.Instance.Info(e.Message);\n"
        "\t\t\t\t\treturn false;\n"
        "\t\t\t\t}\n"
        "\t\t\t\tif (IsNativeBindingLoadFailure(e)) {\n"
        "\t\t\t\t\tRecordNativeFailure(videoFile.Path, e);"
    )
    if "e is TiledHeifRequiresProcessException" not in text:
        text = replace_if_present(text, batch_catch, batch_replacement)

    frame_catch = (
        "\t\t\t\tcatch (Exception e) {\n"
        "\t\t\t\t\tif (IsNativeBindingLoadFailure(e))\n"
        "\t\t\t\t\t\tRecordNativeFailure(filePath, e);"
    )
    frame_replacement = (
        "\t\t\t\tcatch (Exception e) {\n"
        "\t\t\t\t\tif (e is TiledHeifRequiresProcessException)\n"
        "\t\t\t\t\t\tLogger.Instance.Info(e.Message);\n"
        "\t\t\t\t\telse if (IsNativeBindingLoadFailure(e))\n"
        "\t\t\t\t\t\tRecordNativeFailure(filePath, e);"
    )
    if "else if (IsNativeBindingLoadFailure(e))\n\t\t\t\t\t\tRecordNativeFailure(filePath, e);" not in text:
        text = replace_if_present(text, frame_catch, frame_replacement)

    path.write_text(text, encoding="utf-8")


def ensure_tiled_heif_process_retry() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
    text = path.read_text(encoding="utf-8")
    marker = "TryGetTiledHeifGridFrame(\n\t\t\t\tsettings,"
    if marker in text:
        return

    needle = "\t\t\tstring ffmpegError = errOut.ToString();\n"
    replacement = (
        needle
        + "\t\t\tif (bytes == null && FileUtils.IsHeifImageFile(settings.File)) {\n"
        + "\t\t\t\tbyte[]? gridBytes = TryGetTiledHeifGridFrame(\n"
        + "\t\t\t\t\tsettings,\n"
        + "\t\t\t\t\tisGrayByte,\n"
        + "\t\t\t\t\tisRgbFrame,\n"
        + "\t\t\t\t\tgraySideLength,\n"
        + "\t\t\t\t\texpectedGrayBytes,\n"
        + "\t\t\t\t\texpectedRgbBytes,\n"
        + "\t\t\t\t\ttimeoutMilliseconds,\n"
        + "\t\t\t\t\tout string gridError);\n"
        + "\t\t\t\tif (!string.IsNullOrWhiteSpace(gridError))\n"
        + "\t\t\t\t\tffmpegError = string.IsNullOrWhiteSpace(ffmpegError)\n"
        + "\t\t\t\t\t\t? gridError\n"
        + "\t\t\t\t\t\t: $\"{ffmpegError}{Environment.NewLine}HEIF tile-grid retry:{Environment.NewLine}{gridError}\";\n"
        + "\t\t\t\tif (gridBytes != null) {\n"
        + "\t\t\t\t\tbytes = gridBytes;\n"
        + "\t\t\t\t\tprocessAttemptedHardware = false;\n"
        + "\t\t\t\t\thardwarePolicy = \"heif-grid-process\";\n"
        + "\t\t\t\t}\n"
        + "\t\t\t}\n"
    )
    text = replace_once(text, needle, replacement, "FFmpeg process error collection")
    path.write_text(text, encoding="utf-8")


def ensure_ai_tiled_heif_compatibility() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.AiCompat.cs")
    text = path.read_text(encoding="utf-8")

    native_anchor = (
        "\t\t\t\tusing var vsd =\n"
        "\t\t\t\t\tnew VideoStreamDecoder(videoFile.Path, hardwareDeviceType);\n"
        "\t\t\t\tVideoFrameConverter? grayConverter"
    )
    native_replacement = (
        "\t\t\t\tusing var vsd =\n"
        "\t\t\t\t\tnew VideoStreamDecoder(videoFile.Path, hardwareDeviceType);\n"
        "\t\t\t\tThrowIfTiledHeifRequiresProcess(vsd, videoFile.Path);\n"
        "\t\t\t\tVideoFrameConverter? grayConverter"
    )
    if native_replacement not in text:
        text = replace_if_present(text, native_anchor, native_replacement)

    catch_anchor = (
        "\t\t\tcatch (Exception ex) {\n"
        "\t\t\t\tif (IsNativeBindingLoadFailure(ex)) {\n"
        "\t\t\t\t\tRecordNativeFailure(videoFile.Path, ex);"
    )
    catch_replacement = (
        "\t\t\tcatch (Exception ex) {\n"
        "\t\t\t\tif (ex is TiledHeifRequiresProcessException) {\n"
        "\t\t\t\t\tLogger.Instance.Info(ex.Message);\n"
        "\t\t\t\t\treturn false;\n"
        "\t\t\t\t}\n"
        "\t\t\t\tif (IsNativeBindingLoadFailure(ex)) {\n"
        "\t\t\t\t\tRecordNativeFailure(videoFile.Path, ex);"
    )
    if "ex is TiledHeifRequiresProcessException" not in text:
        text = replace_if_present(text, catch_anchor, catch_replacement)

    if "TryGetTiledHeifGridAiRgb224" not in text:
        final_error_anchor = (
            "\t\t\tstring error =\n"
            "\t\t\t\tstring.Join(\n"
            "\t\t\t\t\tEnvironment.NewLine,\n"
            "\t\t\t\t\tnew[] { fast.Error, accurate.Error }"
        )
        final_error_replacement = (
            "\t\t\tstring tiledHeifError = string.Empty;\n"
            "\t\t\tif (FileUtils.IsHeifImageFile(file)) {\n"
            "\t\t\t\tbyte[]? tiledHeif = TryGetTiledHeifGridAiRgb224(\n"
            "\t\t\t\t\tfile,\n"
            "\t\t\t\t\tTimeoutDuration,\n"
            "\t\t\t\t\tout tiledHeifError);\n"
            "\t\t\t\tif (tiledHeif != null) {\n"
            "\t\t\t\t\tif (extendedLogging && !string.IsNullOrWhiteSpace(tiledHeifError))\n"
            "\t\t\t\t\t\tLogger.Instance.Info($\"FFmpeg tiled-HEIF AI extraction for '{file}': {tiledHeifError}\");\n"
            "\t\t\t\t\treturn tiledHeif;\n"
            "\t\t\t\t}\n"
            "\t\t\t}\n\n"
            "\t\t\tstring error =\n"
            "\t\t\t\tstring.Join(\n"
            "\t\t\t\t\tEnvironment.NewLine,\n"
            "\t\t\t\t\tnew[] { fast.Error, accurate.Error, tiledHeifError }"
        )
        text = replace_once(text, final_error_anchor, final_error_replacement, "AI CLI final error")

    path.write_text(text, encoding="utf-8")


ensure_rgb224_process_support()
ensure_stream_group_compatibility()
ensure_tiled_heif_native_refusal()
ensure_tiled_heif_process_retry()
ensure_ai_tiled_heif_compatibility()
