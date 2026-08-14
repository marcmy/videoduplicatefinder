from pathlib import Path


def replace_once(text: str, old: str, new: str, description: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"Expected exactly one {description} anchor, found {count}")
    return text.replace(old, new, 1)


def patch_partial_verify_decode_safety() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
    text = path.read_text(encoding="utf-8")

    start = text.find("\t\tinternal static unsafe byte[]?[] GetGrayFrames(")
    end = text.find("\n\t\tstatic int GetGrayScaleSideLength(", start)
    if start < 0 or end < 0:
        raise RuntimeError("Unable to locate GetGrayFrames")
    block = text[start:end]

    # Upstream #863: this helper is used only by the partial-clip visual gate.
    # One to three tiny verification frames do not justify an in-process GPU session,
    # while a driver access violation would terminate the whole application. Keep the
    # main scan's accelerated paths untouched; only this visual verification helper is
    # deliberately software-only.
    old_device = (
        "\t\t\t\tAVHWDeviceType hardwareDeviceType = ShouldBypassHardwareDecodeForCodec(hardwareCodecName, out _)\n"
        "\t\t\t\t\t? AVHWDeviceType.AV_HWDEVICE_TYPE_NONE\n"
        "\t\t\t\t\t: GetConfiguredHardwareDeviceType();"
    )
    new_device = (
        "\t\t\t\tAVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;"
    )
    if old_device in block:
        block = block.replace(old_device, new_device, 1)

    # Upstream #861: decoded frame metadata is authoritative. Corrupt streams can
    # diverge from the codec context mid-stream, and feeding stale format metadata to
    # swscale risks an in-process native crash. The shared helper safely falls back to
    # open-time metadata only when the frame itself has no usable format.
    old_format = (
        "\t\t\t\t\t\t\tAVPixelFormat srcPixFmt = vsd.IsHardwareDecode ? "
        "(AVPixelFormat)srcFrame.format : vsd.PixelFormat;"
    )
    new_format = (
        "\t\t\t\t\t\t\tAVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);"
    )
    if old_format in block:
        block = block.replace(old_format, new_format, 1)

    # Keep the process fallback software-only as well. Otherwise a failed native
    # verifier could immediately re-enter the same GPU driver through ffmpeg.exe.
    old_fallback = (
        "\t\t\t\tframes[i] ??= GetThumbnail(new FfmpegSettings {\n"
        "\t\t\t\t\tFile = filePath,\n"
        "\t\t\t\t\tPosition = TimeSpan.FromSeconds(positionsSeconds[i]),\n"
        "\t\t\t\t\tGrayScale = 1,\n"
        "\t\t\t\t\tHardwareCodecName = hardwareCodecName\n"
        "\t\t\t\t}, extendedLogging);"
    )
    new_fallback = (
        "\t\t\t\tframes[i] ??= GetThumbnail(new FfmpegSettings {\n"
        "\t\t\t\t\tFile = filePath,\n"
        "\t\t\t\t\tPosition = TimeSpan.FromSeconds(positionsSeconds[i]),\n"
        "\t\t\t\t\tGrayScale = 1,\n"
        "\t\t\t\t\tHardwareCodecName = hardwareCodecName,\n"
        "\t\t\t\t\tSoftwareDecodeOnly = true\n"
        "\t\t\t\t}, extendedLogging);"
    )
    if old_fallback in block:
        block = block.replace(old_fallback, new_fallback, 1)

    required = [
        "AVHWDeviceType hardwareDeviceType = AVHWDeviceType.AV_HWDEVICE_TYPE_NONE;",
        "AVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);",
        "SoftwareDecodeOnly = true",
    ]
    for marker in required:
        if marker not in block:
            raise RuntimeError(f"GetGrayFrames safety marker missing after patch: {marker}")

    text = text[:start] + block + text[end:]
    path.write_text(text, encoding="utf-8")


def patch_native_thumbnail_frame_dimensions() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
    text = path.read_text(encoding="utf-8")

    start = text.find("\t\tpublic static unsafe byte[]? GetThumbnail(")
    if start < 0:
        start = text.find("\t\tstatic unsafe byte[]? GetThumbnail(")

    # Upstream removed redundant private modifiers in its style cleanup. Accept both
    # forms so reconciliation remains tied to the TokenizeArgs method boundary rather
    # than a cosmetic access-modifier choice.
    end = text.find("\n\t\tprivate static List<string> TokenizeArgs(", start)
    if end < 0:
        end = text.find("\n\t\tstatic List<string> TokenizeArgs(", start)

    if start < 0 or end < 0:
        raise RuntimeError("Unable to locate GetThumbnail")
    block = text[start:end]

    marker = (
        "sourceSize = new Size(\n"
        "\t\t\t\t\t\tsrcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width,"
    )
    if marker not in block:
        anchor = (
            "\t\t\t\t\tseekMs = frameTiming.SeekMs;\n"
            "\t\t\t\t\tdecodeMs = frameTiming.DecodeMs;\n"
            "\t\t\t\t\ttransferMs = frameTiming.TransferMs;\n"
            "\t\t\t\t\thardwareTransfers = frameTiming.HardwareTransfers;\n\n"
            "\t\t\t\t\tAVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);"
        )
        replacement = (
            "\t\t\t\t\tseekMs = frameTiming.SeekMs;\n"
            "\t\t\t\t\tdecodeMs = frameTiming.DecodeMs;\n"
            "\t\t\t\t\ttransferMs = frameTiming.TransferMs;\n"
            "\t\t\t\t\thardwareTransfers = frameTiming.HardwareTransfers;\n\n"
            "\t\t\t\t\t// #861: frame metadata wins over the open-time codec context. A\n"
            "\t\t\t\t\t// corrupt or dynamically changing stream can report different decoded\n"
            "\t\t\t\t\t// dimensions; configure swscale from the frame that will actually be read.\n"
            "\t\t\t\t\tsourceSize = new Size(\n"
            "\t\t\t\t\t\tsrcFrame.width > 0 ? srcFrame.width : vsd.FrameSize.Width,\n"
            "\t\t\t\t\t\tsrcFrame.height > 0 ? srcFrame.height : vsd.FrameSize.Height);\n"
            "\t\t\t\t\tAVPixelFormat srcPixFmt = GetConvertiblePixelFormat(vsd, srcFrame);"
        )
        block = replace_once(
            block,
            anchor,
            replacement,
            "GetThumbnail decoded-frame dimension",
        )

    if marker not in block:
        raise RuntimeError("GetThumbnail decoded-frame dimension marker missing after patch")

    text = text[:start] + block + text[end:]
    path.write_text(text, encoding="utf-8")


patch_partial_verify_decode_safety()
patch_native_thumbnail_frame_dimensions()
