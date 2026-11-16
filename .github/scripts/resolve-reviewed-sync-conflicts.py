#!/usr/bin/env python3
"""Apply reviewed merge resolutions only to their exact pair of Git input blobs."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET


CONFLICT = re.compile(
    r"^<<<<<<<[^\n]*\n(.*?)^=======\n(.*?)^>>>>>>>[^\n]*(?:\n|$)",
    re.MULTILINE | re.DOTALL,
)


def blob_sha(content):
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content).hexdigest()


def render_resolution(text, rule, root):
    if rule["kind"] == "template":
        # Compare canonical Git bytes even when Windows checks the template out as CRLF.
        result = (root / rule["template"]).read_text(encoding="utf-8-sig").encode("utf-8")
    else:
        def take_reviewed_hunk(match):
            ours, theirs = match.groups()
            package = rule.get("preserve_package")
            if package and f'Include="{package}"' in ours and f'Include="{package}"' not in theirs:
                line = next(line for line in ours.splitlines(True)
                            if f'Include="{package}"' in line)
                return line + theirs
            return theirs

        result = CONFLICT.sub(take_reviewed_hunk, text).encode("utf-8")
    if blob_sha(result) != rule["result"]:
        raise ValueError("The resolution differs from the reviewed result; refusing to stage it.")
    if re.search(rb"^(?:<<<<<<<|=======|>>>>>>>)", result, re.MULTILINE):
        raise ValueError("Conflict markers remain in the reviewed resolution.")
    if rule["kind"] == "project":
        ET.fromstring(result)
    return result


def resolve(root):
    root = Path(root).resolve()
    rules = json.loads((root / ".github/sync-resolutions/net11.json").read_text())
    entries = subprocess.check_output(["git", "-C", str(root), "ls-files", "-u", "-z"])
    conflicts = {}
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        metadata, path = entry.decode().split("\t", 1)
        _, sha, stage = metadata.split()
        conflicts.setdefault(path, {})[stage] = sha

    resolved = []
    for path, stages in conflicts.items():
        rule = rules.get(path)
        if not rule or stages.get("2") != rule["ours"] or stages.get("3") != rule["theirs"]:
            print(f"Leaving unreviewed conflict for manual review: {path}")
            continue
        target = root / path
        text = target.read_text(encoding="utf-8-sig")
        result = render_resolution(text, rule, root)
        target.write_bytes(result)
        subprocess.run(["git", "-C", str(root), "add", "--", path], check=True)
        resolved.append(path)
        print(f"Applied exact reviewed resolution: {path}")
    return resolved


if __name__ == "__main__":
    resolve(Path(__file__).resolve().parents[2])
