from pathlib import Path


def ensure_rgb224_setting() -> None:
    path = Path("VDF.Core/FFTools/FfmpegEngine.cs")
    text = path.read_text(encoding="utf-8")
    if "public bool Rgb224;" in text:
        return

    needle = "\t\tpublic bool SoftwareDecodeOnly;\n\t}\n}"
    replacement = (
        "\t\tpublic bool SoftwareDecodeOnly;\n"
        "\t\t/// <summary>Produce a raw 224x224 RGB24 AI embedding frame.</summary>\n"
        "\t\tpublic bool Rgb224;\n"
        "\t}\n}"
    )
    if needle not in text:
        raise RuntimeError("Unable to locate FfmpegSettings insertion point")
    path.write_text(text.replace(needle, replacement, 1), encoding="utf-8")


def restore_phash_quorum() -> None:
    path = Path("VDF.Core/ScanEngine.cs")
    text = path.read_text(encoding="utf-8")

    method_marker = "\t\tinternal bool CheckIfDuplicate("
    method_start = text.find(method_marker)
    if method_start < 0:
        raise RuntimeError("Unable to locate CheckIfDuplicate")

    block_start = text.find("\t\t\tif (Settings.UsePHashing) {", method_start)
    block_end = text.find("\n\t\t\tbyte[]?[] compGray", block_start)
    if block_start < 0 or block_end < 0:
        raise RuntimeError("Unable to locate pHash comparison block")

    replacement = """\t\t\tif (Settings.UsePHashing) {
\t\t\t\tulong[]? phashes = overrideGray != null ? overridePHashes : entry.comparePHashes;
\t\t\t\tulong[]? phashesComp = compItem.comparePHashes;
\t\t\t\tif (phashes == null || phashesComp == null)
\t\t\t\t\treturn false;

\t\t\t\tint sampleCount = Math.Min(phashes.Length, phashesComp.Length);
\t\t\t\tif (sampleCount == 0)
\t\t\t\t\treturn false;
\t\t\t\tint requiredMatches = matchingRequiredSampleMatches is int precomputed && sampleCount == positionList.Count
\t\t\t\t\t? precomputed
\t\t\t\t\t: Math.Max(1, (int)Math.Ceiling(sampleCount * Math.Clamp(Settings.PHashRequiredMatchingSampleRatio, 0.01f, 1f)));
\t\t\t\tint maxDifferentBits = (int)Math.Floor((1.0 - Settings.Percent / 100.0) * 64.0);
\t\t\t\tint matches = 0;
\t\t\t\tfloat pHashDiffSum = 0f;

\t\t\t\tfor (int j = 0; j < sampleCount; j++) {
\t\t\t\t\tint differingBits = BitOperations.PopCount(phashes[j] ^ phashesComp[j]);
\t\t\t\t\tpHashDiffSum += differingBits / 64f;
\t\t\t\t\tif (differingBits <= maxDifferentBits)
\t\t\t\t\t\tmatches++;
\t\t\t\t\telse if (matches + (sampleCount - j - 1) < requiredMatches)
\t\t\t\t\t\treturn false;
\t\t\t\t}
\t\t\t\tif (matches < requiredMatches)
\t\t\t\t\treturn false;

\t\t\t\tdifference = pHashDiffSum / sampleCount;
\t\t\t\treturn !float.IsNaN(difference);
\t\t\t}
"""

    text = text[:block_start] + replacement + text[block_end:]
    path.write_text(text, encoding="utf-8")


ensure_rgb224_setting()
restore_phash_quorum()
