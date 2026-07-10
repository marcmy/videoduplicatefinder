from pathlib import Path
import re

ENGINE_PATH = Path("VDF.Core/FFTools/FfmpegEngine.cs")
TESTS_PATH = Path("VDF.Core.Tests/FFTools/FfmpegEngineTests.cs")
SPLIT_FILES = [
    Path("VDF.Core/FFTools/FfmpegEngine.NativeHealth.cs"),
    Path("VDF.Core/FFTools/FfmpegEngine.Telemetry.cs"),
    Path("VDF.Core/FFTools/FfmpegEngine.HardwarePolicy.cs"),
]


def extract(text: str, start_marker: str, end_marker: str, destination: str, header: str, transform=None) -> str:
    start = text.find(start_marker)
    end = text.find(end_marker, start + 1)
    if start < 0 or end < 0:
        raise RuntimeError(f"{destination}: markers not found (start={start}, end={end})")
    region = text[start:end].rstrip() + "\n"
    if transform is not None:
        region = transform(region)
    Path(destination).write_text(header + region + "\t}\n}\n", encoding="utf-8")
    print(f"{destination}: extracted {region.count(chr(10))} lines")
    return text[:start] + text[end:]


def gate_telemetry(region: str) -> str:
    replacements = [
        (
            r"\t\tinternal static bool ShouldLogNativeSuccessTiming\(bool extendedLogging\) \{\n\t\t\t_ = extendedLogging;\n\t\t\treturn true;\n\t\t\}",
            "\t\tinternal static bool ShouldLogNativeSuccessTiming(bool extendedLogging) =>\n\t\t\textendedLogging;",
        ),
        (
            r"\t\tinternal static bool ShouldLogGrayByteScanTelemetry\(bool extendedLogging\) \{\n\t\t\t_ = extendedLogging;\n\t\t\treturn true;\n\t\t\}",
            "\t\tinternal static bool ShouldLogGrayByteScanTelemetry(bool extendedLogging) =>\n\t\t\textendedLogging;",
        ),
    ]
    for pattern, replacement in replacements:
        region, count = re.subn(pattern, lambda _: replacement, region)
        if count != 1:
            raise RuntimeError(f"Telemetry gate replacement count: {count}")
    return region


def split_engine() -> None:
    engine = ENGINE_PATH.read_text(encoding="utf-8")
    old_decl = "\tinternal static class FfmpegEngine {"
    new_decl = "\tinternal static partial class FfmpegEngine {"

    if old_decl not in engine:
        if new_decl in engine and all(path.exists() for path in SPLIT_FILES):
            print("FFmpeg engine is already split; no transformation needed.")
            return
        raise RuntimeError("FFmpeg engine is neither in the reviewed monolithic form nor the expected split form.")

    if engine.count(old_decl) != 1:
        raise RuntimeError(f"FfmpegEngine declaration count: {engine.count(old_decl)}")
    engine = engine.replace(old_decl, new_decl, 1)

    engine = extract(
        engine,
        "\t\tstatic bool ShouldUseNativeBinding =>",
        "\t\tstatic void LogNativeTiming(",
        str(SPLIT_FILES[0]),
        "using System;\nusing System.Collections.Generic;\nusing System.Linq;\nusing System.Threading;\nusing VDF.Core.FFTools.FFmpegNative;\nusing VDF.Core.Utils;\n\nnamespace VDF.Core.FFTools {\n\tinternal static partial class FfmpegEngine {\n",
    )
    engine = extract(
        engine,
        "\t\tstatic void LogNativeTiming(",
        "\t\tconst double SequentialBatchMaxSpanSeconds = 2d;",
        str(SPLIT_FILES[1]),
        "using System;\nusing VDF.Core.FFTools.FFmpegNative;\nusing VDF.Core.Utils;\n\nnamespace VDF.Core.FFTools {\n\tinternal static partial class FfmpegEngine {\n",
        gate_telemetry,
    )
    engine = extract(
        engine,
        "\t\tstatic bool TryGetNativeGrayByteD3D11ManualMaxConcurrency(out int concurrency) {",
        "\t\tstatic unsafe byte[] ExtractGrayFrameFromFrame(",
        str(SPLIT_FILES[2]),
        "using System;\nusing System.Collections.Generic;\nusing System.Globalization;\nusing System.Linq;\nusing System.Threading;\nusing FFmpeg.AutoGen;\nusing VDF.Core.Utils;\n\nnamespace VDF.Core.FFTools {\n\tinternal static partial class FfmpegEngine {\n",
    )
    ENGINE_PATH.write_text(engine, encoding="utf-8")


def update_tests() -> None:
    tests = TESTS_PATH.read_text(encoding="utf-8")
    if "ShouldLogNativeSuccessTiming_FollowsExtendedLogging" in tests and "ShouldLogGrayByteScanTelemetry_FollowsExtendedLogging" in tests:
        print("Telemetry tests are already updated.")
        return

    start_marker = "\t[Theory]\n\t[InlineData(false)]\n\t[InlineData(true)]\n\tpublic void ShouldLogNativeSuccessTiming_"
    start = tests.find(start_marker)
    second = tests.find("\tpublic void ShouldLogGrayByteScanTelemetry_", start)
    end = tests.find("\n\t}", second)
    if start < 0 or second < 0 or end < 0:
        raise RuntimeError(f"Telemetry test markers not found (start={start}, second={second}, end={end})")
    end += len("\n\t}")
    replacement = """\t[Theory]
\t[InlineData(false, false)]
\t[InlineData(true, true)]
\tpublic void ShouldLogNativeSuccessTiming_FollowsExtendedLogging(
\t\tbool extendedLogging,
\t\tbool expected) {
\t\tAssert.Equal(
\t\t\texpected,
\t\t\tFfmpegEngine.ShouldLogNativeSuccessTiming(extendedLogging));
\t}

\t[Theory]
\t[InlineData(false, false)]
\t[InlineData(true, true)]
\tpublic void ShouldLogGrayByteScanTelemetry_FollowsExtendedLogging(
\t\tbool extendedLogging,
\t\tbool expected) {
\t\tAssert.Equal(
\t\t\texpected,
\t\t\tFfmpegEngine.ShouldLogGrayByteScanTelemetry(extendedLogging));
\t}"""
    TESTS_PATH.write_text(tests[:start] + replacement + tests[end:], encoding="utf-8")


split_engine()
update_tests()
