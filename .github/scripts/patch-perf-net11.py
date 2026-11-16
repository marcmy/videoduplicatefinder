#!/usr/bin/env python3
"""Keep fork UI strings and portable headless fonts after the .NET 11 sync."""

import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[2]


def write_if_changed(path, text):
    if path.read_text(encoding="utf-8-sig") != text:
        path.write_text(text, encoding="utf-8")


def main():
    core = (ROOT / "VDF.Core/VDF.Core.csproj").read_text(encoding="utf-8-sig")
    if "<TargetFramework>net11.0</TargetFramework>" not in core:
        return

    gui = ET.parse(ROOT / "VDF.GUI/VDF.GUI.csproj")
    version = gui.find('.//PackageReference[@Include="Avalonia"]').get("Version")
    project = ROOT / "VDF.GUI.HeadlessTests/VDF.GUI.HeadlessTests.csproj"
    text = project.read_text(encoding="utf-8-sig")
    if 'Include="Avalonia.Fonts.Inter"' not in text:
        anchor = f'    <PackageReference Include="Avalonia.Headless" Version="{version}" />'
        if anchor not in text:
            raise ValueError("Headless Avalonia must match the application's Avalonia version.")
        text = text.replace(anchor, anchor + f'\n    <PackageReference Include="Avalonia.Fonts.Inter" Version="{version}" />')
    else:
        text = re.sub(r'(Include="Avalonia.Fonts.Inter" Version=")[^"]+',
                      lambda match: match[1] + version, text)
    write_if_changed(project, text)

    entrypoint = ROOT / "VDF.GUI.HeadlessTests/HeadlessUi.cs"
    text = entrypoint.read_text(encoding="utf-8-sig")
    if ".WithInterFont()" not in text:
        anchor = "AppBuilder.Configure<App>()"
        if anchor not in text:
            raise ValueError("Headless test entrypoint changed; review font initialization.")
        text = text.replace(anchor, anchor + "\n\t\t\t.WithInterFont()", 1)
    if 'DefaultFamilyName = "fonts:Inter#Inter"' not in text:
        # Headless Skia can have no installed default font. Registering the
        # collection alone does not provide the final fallback for composite
        # families such as Cascadia Mono,Consolas,monospace.
        text = text.replace(".WithInterFont()", '.WithInterFont()\n\t\t\t.With(new Avalonia.Media.FontManagerOptions { DefaultFamilyName = "fonts:Inter#Inter" })', 1)
    write_if_changed(entrypoint, text)

    folder = ROOT / "VDF.GUI/Assets/Locales"
    russian_path = folder / "ru.json"
    english = json.loads((folder / "en.json").read_text(encoding="utf-8-sig"))
    russian = json.loads(russian_path.read_text(encoding="utf-8-sig"))
    additions = json.loads((ROOT / ".github/sync-resolutions/ru-perf.json").read_text(encoding="utf-8-sig"))
    for key, value in additions.items():
        if key not in english:
            raise ValueError(f"Reviewed locale key no longer exists: {key}")
        if set(re.findall(r"\{\d+\}", value)) != set(re.findall(r"\{\d+\}", english[key])):
            raise ValueError(f"Translation placeholders differ for {key}")
        russian.setdefault(key, value)
    write_if_changed(russian_path, json.dumps(russian, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
