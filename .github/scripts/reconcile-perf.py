#!/usr/bin/env python3
"""Run every perf/upstream reconciliation pass from one canonical entrypoint.

The individual patchers remain deliberately focused and independently reviewable. This
orchestrator owns their order so the refresh workflow, local maintenance, and the
fixed-point/idempotence check cannot drift into subtly different reconciliation stacks.
"""

from pathlib import Path
import subprocess
import sys

SCRIPTS = (
    "patch-perf-merge.py",
    "patch-perf-upstream-parity.py",
    "patch-perf-upstream-safety.py",
    "patch-perf-ai-process.py",
)


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[1]

    for name in SCRIPTS:
        script = script_dir / name
        if not script.is_file():
            print(f"error: reconciliation script is missing: {script}", file=sys.stderr)
            return 1
        print(f"==> reconcile: {name}", flush=True)
        try:
            subprocess.run([sys.executable, str(script)], cwd=repo_root, check=True)
        except subprocess.CalledProcessError as exc:
            print(f"error: {name} failed with exit code {exc.returncode}", file=sys.stderr)
            return exc.returncode or 1

    print("Perf reconciliation stack completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
