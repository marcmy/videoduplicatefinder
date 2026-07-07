from pathlib import Path
import subprocess

REFERENCE = "origin/perf/native-hwaccel-from-crashfix"


def copy_from_reference(path: str) -> None:
    data = subprocess.check_output(["git", "show", f"{REFERENCE}:{path}"])
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


copy_from_reference("VDF.Core/ScanEngine.MatchingConcurrency.cs")
copy_from_reference("VDF.Core.Tests/MatchingConcurrencyTests.cs")

settings_path = Path("VDF.Core/Settings.cs")
settings = settings_path.read_text(encoding="utf-8")
if "MatchingMaxDegreeOfParallelism" not in settings:
    settings = replace_once(
        settings,
        "\t\tpublic int MaxDegreeOfParallelism = 1;\n",
        "\t\tpublic int MaxDegreeOfParallelism = 1;\n"
        "\t\t/// <summary>Maximum workers for duplicate matching. Non-positive values use an automatic CPU-headroom cap.</summary>\n"
        "\t\tpublic int MatchingMaxDegreeOfParallelism;\n",
        "core matching setting",
    )
settings_path.write_text(settings, encoding="utf-8")

gui_settings_path = Path("VDF.GUI/Data/SettingsFile.cs")
gui_settings = gui_settings_path.read_text(encoding="utf-8")
if "public int MatchingMaxDegreeOfParallelism" not in gui_settings:
    anchor = (
        "\t\tpublic int MaxDegreeOfParallelism {\n"
        "\t\t\tget => _MaxDegreeOfParallelism;\n"
        "\t\t\tset => this.RaiseAndSetIfChanged(ref _MaxDegreeOfParallelism, value);\n"
        "\t\t}\n"
    )
    addition = anchor + (
        "\t\tint _MatchingMaxDegreeOfParallelism;\n"
        "\t\t[JsonPropertyName(\"MatchingMaxDegreeOfParallelism\")]\n"
        "\t\tpublic int MatchingMaxDegreeOfParallelism {\n"
        "\t\t\tget => _MatchingMaxDegreeOfParallelism;\n"
        "\t\t\tset => this.RaiseAndSetIfChanged(ref _MatchingMaxDegreeOfParallelism, value);\n"
        "\t\t}\n"
    )
    gui_settings = replace_once(gui_settings, anchor, addition, "GUI matching setting")
gui_settings_path.write_text(gui_settings, encoding="utf-8")

scan_path = Path("VDF.Core/ScanEngine.cs")
scan = scan_path.read_text(encoding="utf-8")
dup_start = scan.index("internal void ScanForDuplicates()")
partial_start = scan.index("void ScanForPartialDuplicates()", dup_start)
dup = scan[dup_start:partial_start]

if "Duplicate matching concurrency:" not in dup:
    logger = "\t\t\tLogger.Instance.Info($\"Scanning for duplicates in {ScanList.Count:N0} files\");\n"
    insertion = logger + (
        "\n\t\t\tint matchingMaxDegreeOfParallelism = GetMatchingMaxDegreeOfParallelism();\n"
        "\t\t\tLogger.Instance.Info($\"Duplicate matching concurrency: {matchingMaxDegreeOfParallelism} worker(s) on {Environment.ProcessorCount} logical processor(s); configured matching={Settings.MatchingMaxDegreeOfParallelism}, media extraction={Settings.MaxDegreeOfParallelism}\");\n"
    )
    dup = replace_once(dup, logger, insertion, "matching concurrency log")

old_parallel = "MaxDegreeOfParallelism = Settings.MaxDegreeOfParallelism"
normal_count = dup.count(old_parallel)
if normal_count < 4:
    raise RuntimeError(f"normal matching loops: expected at least four sites, found {normal_count}")
dup = dup.replace(old_parallel, "MaxDegreeOfParallelism = matchingMaxDegreeOfParallelism")
scan = scan[:dup_start] + dup + scan[partial_start:]

partial_start = scan.index("void ScanForPartialDuplicates()", dup_start)
partial = scan[partial_start:]
if "int matchingMaxDegreeOfParallelism = GetMatchingMaxDegreeOfParallelism();" not in partial:
    threshold = "\t\t\tfloat simThreshold = (float)Settings.PartialClipSimilarityThreshold;\n"
    partial = replace_once(
        partial,
        threshold,
        threshold + "\t\t\tint matchingMaxDegreeOfParallelism = GetMatchingMaxDegreeOfParallelism();\n",
        "partial matching concurrency",
    )
partial_old = "MaxDegreeOfParallelism = Math.Max(1, Settings.MaxDegreeOfParallelism)"
partial_count = partial.count(partial_old)
if partial_count < 1:
    raise RuntimeError("partial matching loop: no media-DOP site found")
partial = partial.replace(partial_old, "MaxDegreeOfParallelism = matchingMaxDegreeOfParallelism")
scan = scan[:partial_start] + partial
scan_path.write_text(scan, encoding="utf-8")

sync_anchor = "\t\t\tScanner.Settings.MaxDegreeOfParallelism = SettingsFile.Instance.MaxDegreeOfParallelism;\n"
sync_addition = sync_anchor + "\t\t\tScanner.Settings.MatchingMaxDegreeOfParallelism = SettingsFile.Instance.MatchingMaxDegreeOfParallelism;\n"
sync_files = []
for path in Path("VDF.GUI/ViewModels").glob("MainWindowVM*.cs"):
    text = path.read_text(encoding="utf-8")
    if sync_anchor in text:
        if "Scanner.Settings.MatchingMaxDegreeOfParallelism" not in text:
            text = replace_once(text, sync_anchor, sync_addition, f"core settings sync in {path}")
            path.write_text(text, encoding="utf-8")
        sync_files.append(str(path))
if len(sync_files) != 1:
    raise RuntimeError(f"expected one core settings sync file, found {sync_files}")

print(f"Ported matching headroom: {normal_count} normal loops, {partial_count} partial loop site(s), sync={sync_files[0]}")
