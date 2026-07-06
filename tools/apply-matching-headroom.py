# Temporary validation helper. The generated source files replace this before merge.
from pathlib import Path
import re


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    Path(path).write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one occurrence, found {count}: {old!r}")
    write(path, text.replace(old, new, 1))


replace_once(
    "VDF.Core/Settings.cs",
    "\t\tpublic int MaxDegreeOfParallelism = 1;\n",
    "\t\tpublic int MaxDegreeOfParallelism = 1;\n"
    "\t\t/// <summary>Maximum workers for duplicate matching. Non-positive values use an automatic CPU-headroom cap.</summary>\n"
    "\t\tpublic int MatchingMaxDegreeOfParallelism;\n")

replace_once(
    "VDF.GUI/Data/SettingsFile.cs",
    "\t\tpublic int MaxDegreeOfParallelism {\n"
    "\t\t\tget => _MaxDegreeOfParallelism;\n"
    "\t\t\tset => this.RaiseAndSetIfChanged(ref _MaxDegreeOfParallelism, value);\n"
    "\t\t}\n",
    "\t\tpublic int MaxDegreeOfParallelism {\n"
    "\t\t\tget => _MaxDegreeOfParallelism;\n"
    "\t\t\tset => this.RaiseAndSetIfChanged(ref _MaxDegreeOfParallelism, value);\n"
    "\t\t}\n"
    "\t\tint _MatchingMaxDegreeOfParallelism;\n"
    "\t\t[JsonPropertyName(\"MatchingMaxDegreeOfParallelism\")]\n"
    "\t\tpublic int MatchingMaxDegreeOfParallelism {\n"
    "\t\t\tget => _MatchingMaxDegreeOfParallelism;\n"
    "\t\t\tset => this.RaiseAndSetIfChanged(ref _MatchingMaxDegreeOfParallelism, value);\n"
    "\t\t}\n")

vm_path = "VDF.GUI/ViewModels/MainWindowVM.cs"
vm = read(vm_path)
pattern = r"(?m)^(\s*)MaxDegreeOfParallelism\s*=\s*SettingsFile\.Instance\.MaxDegreeOfParallelism,\s*$"
matches = list(re.finditer(pattern, vm))
if len(matches) != 1:
    raise RuntimeError(f"{vm_path}: expected one core-settings MaxDegree mapping, found {len(matches)}")
match = matches[0]
indent = match.group(1)
replacement = match.group(0) + "\n" + indent + "MatchingMaxDegreeOfParallelism = SettingsFile.Instance.MatchingMaxDegreeOfParallelism,"
vm = vm[:match.start()] + replacement + vm[match.end():]
write(vm_path, vm)

concurrency_source = '''// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

namespace VDF.Core {
\tpublic sealed partial class ScanEngine {
\t\t/// <summary>
\t\t/// Resolves comparison concurrency independently from media extraction.
\t\t/// A positive configured value is honored up to the machine's logical CPU
\t\t/// count. Non-positive values reserve one or two logical processors and
\t\t/// cap matching at roughly 80% of the machine for UI/system headroom.
\t\t/// </summary>
\t\tinternal static int CalculateMatchingMaxDegreeOfParallelism(
\t\t\tint configured,
\t\t\tint processorCount) {
\t\t\tprocessorCount = Math.Max(1, processorCount);
\t\t\tif (configured > 0)
\t\t\t\treturn Math.Min(configured, processorCount);

\t\t\tint reservedProcessors = processorCount >= 8 ? 2 : processorCount >= 2 ? 1 : 0;
\t\t\tint reserveCap = Math.Max(1, processorCount - reservedProcessors);
\t\t\tint percentageCap = Math.Max(1, (int)Math.Ceiling(processorCount * 0.80d));
\t\t\treturn Math.Min(reserveCap, percentageCap);
\t\t}

\t\tinternal int GetMatchingMaxDegreeOfParallelism() =>
\t\t\tCalculateMatchingMaxDegreeOfParallelism(
\t\t\t\tSettings.MatchingMaxDegreeOfParallelism,
\t\t\t\tEnvironment.ProcessorCount);
\t}
}
'''
write("VDF.Core/ScanEngine.MatchingConcurrency.cs", concurrency_source)

scan_path = "VDF.Core/ScanEngine.cs"
scan = read(scan_path)
scan_log = '\t\t\tLogger.Instance.Info($"Scanning for duplicates in {ScanList.Count:N0} files");'
if scan.count(scan_log) != 1:
    raise RuntimeError(f"{scan_path}: duplicate-scan log anchor count was {scan.count(scan_log)}")
scan = scan.replace(
    scan_log,
    scan_log +
    "\n\n\t\t\tint matchingMaxDegreeOfParallelism = GetMatchingMaxDegreeOfParallelism();" +
    "\n\t\t\tLogger.Instance.Info($\"Duplicate matching concurrency: {matchingMaxDegreeOfParallelism} worker(s) on {Environment.ProcessorCount} logical processor(s); configured matching={Settings.MatchingMaxDegreeOfParallelism}, media extraction={Settings.MaxDegreeOfParallelism}\");",
    1)

normal_start = scan.index("\t\tinternal void ScanForDuplicates()")
partial_start = scan.index("\t\tvoid ScanForPartialDuplicates()", normal_start)
normal = scan[normal_start:partial_start]
old_parallelism = "MaxDegreeOfParallelism = Settings.MaxDegreeOfParallelism"
normal_count = normal.count(old_parallelism)
if normal_count != 4:
    raise RuntimeError(f"{scan_path}: expected four normal matching parallelism sites, found {normal_count}")
normal = normal.replace(old_parallelism, "MaxDegreeOfParallelism = matchingMaxDegreeOfParallelism")
scan = scan[:normal_start] + normal + scan[partial_start:]

partial_start = scan.index("\t\tvoid ScanForPartialDuplicates()")
sim_anchor = "\t\t\tfloat simThreshold = (float)Settings.PartialClipSimilarityThreshold;"
if scan[partial_start:].count(sim_anchor) != 1:
    raise RuntimeError(f"{scan_path}: partial similarity anchor missing or duplicated")
tail = scan[partial_start:].replace(
    sim_anchor,
    sim_anchor + "\n\t\t\tint matchingMaxDegreeOfParallelism = GetMatchingMaxDegreeOfParallelism();",
    1)
partial_old = "MaxDegreeOfParallelism = Math.Max(1, Settings.MaxDegreeOfParallelism)"
if tail.count(partial_old) != 1:
    raise RuntimeError(f"{scan_path}: expected one partial matching parallelism site, found {tail.count(partial_old)}")
tail = tail.replace(partial_old, "MaxDegreeOfParallelism = matchingMaxDegreeOfParallelism", 1)
scan = scan[:partial_start] + tail
write(scan_path, scan)

tests = '''// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using VDF.Core;

namespace VDF.Core.Tests;

public class MatchingConcurrencyTests {
\t[Theory]
\t[InlineData(0, 1, 1)]
\t[InlineData(0, 2, 1)]
\t[InlineData(0, 4, 3)]
\t[InlineData(0, 8, 6)]
\t[InlineData(0, 16, 13)]
\t[InlineData(0, 32, 26)]
\t[InlineData(-1, 8, 6)]
\tpublic void CalculateMatchingMaxDegreeOfParallelism_AutoLeavesCpuHeadroom(
\t\tint configured,
\t\tint processorCount,
\t\tint expected) {
\t\tAssert.Equal(expected,
\t\t\tScanEngine.CalculateMatchingMaxDegreeOfParallelism(configured, processorCount));
\t}

\t[Theory]
\t[InlineData(1, 16, 1)]
\t[InlineData(6, 16, 6)]
\t[InlineData(99, 16, 16)]
\tpublic void CalculateMatchingMaxDegreeOfParallelism_ExplicitValueIsHonoredAndClamped(
\t\tint configured,
\t\tint processorCount,
\t\tint expected) {
\t\tAssert.Equal(expected,
\t\t\tScanEngine.CalculateMatchingMaxDegreeOfParallelism(configured, processorCount));
\t}
}
'''
write("VDF.Core.Tests/MatchingConcurrencyTests.cs", tests)
