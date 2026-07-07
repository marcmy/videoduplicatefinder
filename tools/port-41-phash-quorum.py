from pathlib import Path
import subprocess

REFERENCE = "origin/perf/native-hwaccel-from-crashfix"


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one match, found {count}")
    return text.replace(old, new, 1)


def git_show(path: str) -> str:
    return subprocess.check_output(["git", "show", f"{REFERENCE}:{path}"], text=True, encoding="utf-8")


# Core setting: ratio of sampled positions that must individually pass pHash similarity.
core_settings_path = Path("VDF.Core/Settings.cs")
core_settings = core_settings_path.read_text(encoding="utf-8")
if "PHashRequiredMatchingSampleRatio" not in core_settings:
    anchor = "\t\tpublic float Percent = 96f;\n"
    addition = anchor + (
        "\t\t/// <summary>Minimum fraction of sampled frame positions that must individually pass pHash similarity.</summary>\n"
        "\t\tpublic float PHashRequiredMatchingSampleRatio = 0.6f;\n"
    )
    core_settings = replace_once(core_settings, anchor, addition, "Core pHash ratio setting")
core_settings_path.write_text(core_settings, encoding="utf-8")

# GUI persisted percentage, copied from the proven 4.0 performance branch implementation.
old_gui_settings = git_show("VDF.GUI/Data/SettingsFile.cs")
prop_start = old_gui_settings.index("\t\tfloat _PHashRequiredMatchingSampleRatioPercent")
prop_end = old_gui_settings.index("\t\tbool _UseExifCreationDate", prop_start)
prop_block = old_gui_settings[prop_start:prop_end]

gui_settings_path = Path("VDF.GUI/Data/SettingsFile.cs")
gui_settings = gui_settings_path.read_text(encoding="utf-8")
if "PHashRequiredMatchingSampleRatioPercent" not in gui_settings:
    insert_at = gui_settings.index("\t\tbool _UseExifCreationDate")
    gui_settings = gui_settings[:insert_at] + prop_block + gui_settings[insert_at:]
gui_settings_path.write_text(gui_settings, encoding="utf-8")

# Restore the proven all-sample snapshot + quorum matcher while preserving 4.1's surrounding scanner.
old_scan = git_show("VDF.Core/ScanEngine.cs")
old_start = old_scan.index("\t\tbool TryBuildCompareSnapshot")
old_end = old_scan.index("\t\tinternal void ScanForDuplicates()", old_start)
old_region = old_scan[old_start:old_end]

scan_path = Path("VDF.Core/ScanEngine.cs")
scan = scan_path.read_text(encoding="utf-8")
new_start = scan.index("\t\tbool TryBuildCompareSnapshot")
new_end = scan.index("\t\tinternal void ScanForDuplicates()", new_start)
scan = scan[:new_start] + old_region + scan[new_end:]
scan_path.write_text(scan, encoding="utf-8")

# Copy the percentage into Core before every scan/compare operation.
vm_path = Path("VDF.GUI/ViewModels/MainWindowVM.cs")
vm = vm_path.read_text(encoding="utf-8")
ratio_sync = "\t\t\tScanner.Settings.PHashRequiredMatchingSampleRatio = SettingsFile.Instance.PHashRequiredMatchingSampleRatioPercent / 100f;\n"
if ratio_sync not in vm:
    anchor = "\t\t\tScanner.Settings.Percent = SettingsFile.Instance.Percent;\n"
    vm = replace_once(vm, anchor, anchor + ratio_sync, "Core pHash ratio sync")
vm_path.write_text(vm, encoding="utf-8")

# Expose it in the redesigned 4.1 Matching settings section, directly under the pHash switch.
view_path = Path("VDF.GUI/Views/SettingsView.xaml")
view = view_path.read_text(encoding="utf-8")
if "PHashSampleRatio" not in view:
    use_binding = "Path=UsePHash}"
    binding_at = view.index(use_binding)
    row_end = view.index("              </views:SettingRow>", binding_at) + len("              </views:SettingRow>")
    row = """

              <views:SettingRow
                  Title="{Binding Source={x:Static local:App.Lang}, Path=[Settings.Row.PHashSampleRatio.Title]}"
                  Description="{Binding Source={x:Static local:App.Lang}, Path=[Settings.Row.PHashSampleRatio.Desc]}"
                  SearchTags="phash samples quorum frames false positives"
                  IsEnabled="{Binding Source={x:Static Settings:SettingsFile.Instance}, Path=UsePHash}">
                <NumericUpDown Classes="stepper" FormatString="\{0\}%" Increment="1" Maximum="100" Minimum="1"
                               Value="{Binding Source={x:Static Settings:SettingsFile.Instance}, Path=PHashRequiredMatchingSampleRatioPercent}" />
              </views:SettingRow>"""
    view = view[:row_end] + row + view[row_end:]
view_path.write_text(view, encoding="utf-8")

# Locale parity is tested; add meaningful translations in every shipped language.
translations = {
    "en.json": (
        "Required matching sample ratio",
        "Minimum percentage of sampled frame positions that must individually pass the pHash similarity threshold. Higher values reduce false positives from one coincidental frame; 60 % is a balanced default.",
    ),
    "de.json": (
        "Erforderlicher Anteil übereinstimmender Samples",
        "Mindestprozentsatz der abgetasteten Bildpositionen, die den pHash-Ähnlichkeitsschwellenwert einzeln erfüllen müssen. Höhere Werte reduzieren Fehlalarme durch ein zufällig ähnliches Einzelbild; 60 % ist ein ausgewogener Standard.",
    ),
    "es.json": (
        "Porcentaje requerido de muestras coincidentes",
        "Porcentaje mínimo de posiciones de fotograma muestreadas que deben superar individualmente el umbral de similitud pHash. Los valores altos reducen falsos positivos por un fotograma coincidente; 60 % es un valor equilibrado.",
    ),
    "fr.json": (
        "Proportion requise d’échantillons concordants",
        "Pourcentage minimal de positions d’image échantillonnées qui doivent chacune dépasser le seuil de similarité pHash. Une valeur élevée réduit les faux positifs dus à une seule image ressemblante ; 60 % est un bon équilibre.",
    ),
    "ko.json": (
        "필수 일치 샘플 비율",
        "샘플링한 프레임 위치 중 pHash 유사도 기준을 각각 통과해야 하는 최소 비율입니다. 값을 높이면 우연히 비슷한 한 프레임으로 인한 오탐이 줄어듭니다. 기본값 60%가 균형 잡힌 설정입니다.",
    ),
    "pt.json": (
        "Proporção necessária de amostras correspondentes",
        "Percentual mínimo das posições de quadro amostradas que devem superar individualmente o limite de similaridade pHash. Valores maiores reduzem falsos positivos causados por um único quadro parecido; 60 % é um padrão equilibrado.",
    ),
    "zh-Hans.json": (
        "所需匹配采样比例",
        "采样帧位置中必须分别通过 pHash 相似度阈值的最低百分比。较高的值可减少单个偶然相似帧造成的误报；60% 是较均衡的默认值。",
    ),
}
for filename, (title, desc) in translations.items():
    locale_path = Path("VDF.GUI/Assets/Locales") / filename
    locale = locale_path.read_text(encoding="utf-8")
    if '"Settings.Row.PHashSampleRatio.Title"' in locale:
        continue
    lines = locale.splitlines(keepends=True)
    for i, line in enumerate(lines):
        if '"Settings.Row.UsePHash.Desc"' in line:
            indent = line[:len(line) - len(line.lstrip())]
            lines[i + 1:i + 1] = [
                f'{indent}"Settings.Row.PHashSampleRatio.Title": {title!r},\n'.replace("'", '"'),
                f'{indent}"Settings.Row.PHashSampleRatio.Desc": {desc!r},\n'.replace("'", '"'),
            ]
            break
    else:
        raise RuntimeError(f"UsePHash locale anchor missing from {locale_path}")
    locale_path.write_text("".join(lines), encoding="utf-8")

# Focused tests pin both the all-sample behavior and the configurable quorum.
test_path = Path("VDF.Core.Tests/PHashSampleQuorumTests.cs")
test_path.write_text(r'''// Copyright (C) 2026 0x90d
// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Reflection;

namespace VDF.Core.Tests;

public class PHashSampleQuorumTests {
    [Fact]
    public void PHashUsesAllSamples_NotJustFirst() {
        var scanner = Scanner(percent: 90f, ratio: 0.6f, 0.25f, 0.5f);
        var entry = Entry((1d, 0UL), (2d, 0UL));
        var other = Entry((1d, 0UL), (2d, ulong.MaxValue));

        Assert.False(Check(scanner, entry, other, out _));
    }

    [Fact]
    public void PHashDifferenceAveragesMatchingSamples() {
        var scanner = Scanner(percent: 75f, ratio: 1f, 0.25f, 0.5f);
        var entry = Entry((1d, 0UL), (2d, 0UL));
        var other = Entry((1d, 0UL), (2d, 0xFFFFUL));

        Assert.True(Check(scanner, entry, other, out float difference));
        Assert.InRange(difference, 0.124f, 0.126f);
    }

    [Fact]
    public void PHashRequiresConfiguredSampleQuorum() {
        var scanner = Scanner(percent: 75f, ratio: 0.6f, 0.25f, 0.5f, 0.75f, 1f, 1.25f);
        var entry = Entry((1d, 0UL), (2d, 0UL), (3d, 0UL), (4d, 0UL), (5d, 0UL));
        var other = Entry((1d, 0UL), (2d, 0UL), (3d, 0xFFFFFUL), (4d, 0xFFFFFUL), (5d, 0xFFFFFUL));

        Assert.False(Check(scanner, entry, other, out _));

        scanner.Settings.PHashRequiredMatchingSampleRatio = 0.4f;
        Assert.True(Check(scanner, entry, other, out _));
    }

    static ScanEngine Scanner(float percent, float ratio, params float[] positions) {
        var scanner = new ScanEngine {
            Settings = new Settings {
                UsePHashing = true,
                Percent = percent,
                PHashRequiredMatchingSampleRatio = ratio,
                ThumbnailCount = positions.Length,
            }
        };
        var field = typeof(ScanEngine).GetField("positionList", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var list = (List<float>)field.GetValue(scanner)!;
        list.Clear();
        list.AddRange(positions);
        return scanner;
    }

    static FileEntry Entry(params (double Index, ulong PHash)[] samples) {
        var entry = new FileEntry {
            _Path = @"X:\video.mp4",
            Folder = @"X:\",
            mediaInfo = new MediaInfo { Duration = TimeSpan.FromSeconds(4), Streams = [] },
            invalid = false,
        };
        foreach ((double index, ulong hash) in samples) {
            entry.grayBytes[index] = new byte[1024];
            entry.PHashes[index] = hash;
        }
        return entry;
    }

    static bool Check(ScanEngine scanner, FileEntry entry, FileEntry other, out float difference) {
        var build = typeof(ScanEngine).GetMethod("TryBuildCompareSnapshot", BindingFlags.Instance | BindingFlags.NonPublic)!;
        Assert.True((bool)build.Invoke(scanner, [entry, true])!);
        Assert.True((bool)build.Invoke(scanner, [other, true])!);

        var check = typeof(ScanEngine).GetMethod("CheckIfDuplicate", BindingFlags.Instance | BindingFlags.NonPublic)!;
        object?[] args = [entry, null, null, other, 0f];
        bool result = (bool)check.Invoke(scanner, args)!;
        difference = (float)args[4]!;
        return result;
    }
}
''', encoding="utf-8")

# Keep the port ledger honest.
doc_path = Path("docs/4.1-perf-port.md")
doc = doc_path.read_text(encoding="utf-8")
line = "- Multi-sample pHash matching quorum with a configurable 60% default\n"
if line not in doc:
    marker = "- Independent CPU headroom for normal and partial-clip duplicate matching\n"
    doc = replace_once(doc, marker, marker + line, "performance port documentation")
doc_path.write_text(doc, encoding="utf-8")

print("Ported the configurable all-sample pHash matching quorum to the 4.1 architecture.")
