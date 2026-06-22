# Perf release modernization plan

## Objective

Adopt upstream's useful Native AOT release model while keeping the fork's hardware-accelerated paths, combined bundle, safer automation, and separate GUI/CLI/Web assets.

## Phase 1: build and packaging

- Publish GUI and CLI with Native AOT.
- Keep Web as a self-contained JIT publish.
- Use a native ARM64 runner for `linux-arm64` Native AOT.
- collect `.pdb`, `.dbg`, and `.dSYM` output into separate symbols archives.
- Exclude symbols from all runtime archives.
- Smoke-test the published CLI.
- Report output file counts and sizes.
- Restore explicit read-only permissions in pull-request CI.

## Phase 2: release safety

- Build Windows, Linux, and macOS assets independently.
- Upload validated outputs as workflow artifacts.
- Publish from one final job only after every build succeeds.
- Replace release assets with `gh release upload --clobber` instead of deleting the working release first.
- Trigger Scoop only after release upload succeeds.

## Phase 3: Scoop layout

After the new Windows AOT GUI asset passes manual hardware-path testing:

- Change `videoduplicatefinder-perf` to install `GUI-win-x64.zip`.
- Add `videoduplicatefinder-perf-full` for `Bundle-win-x64.zip`.
- Preserve state-file migration and persistence behavior in both manifests.

## Required Windows validation

- GUI starts from `GUI-win-x64.zip`.
- CLI `--help` succeeds from `CLI-win-x64.zip`.
- Software thumbnail extraction works.
- Native FFmpeg extraction works.
- D3D11VA decoding works.
- D3D11 video-processor gray-byte conversion works.
- Hardware failure falls back correctly.
- Runtime archives contain no debug-symbol files.
- `Symbols-win-x64.zip` contains the expected application and native-library symbols.

## Rollout rule

Do not switch the default Scoop package to the GUI-only AOT asset until the Windows hardware-acceleration validation above passes. The full bundle remains available throughout the transition.
