# Fork differences: `perf/4.1-native-hwaccel`

This file is the maintenance map for the actively maintained performance branch. It documents which divergences from upstream are intentional, which behaviors are compatibility bridges for upstream correctness fixes, and which older fork changes are now upstream-owned.

Snapshot: 2026-08-08. At this snapshot the perf branch is fully based on current `master` (0 commits behind) and keeps its custom Windows/native performance engine on top.

## Branch contract

- `master` is the upstream-tracking branch and should remain an upstream mirror apart from fork automation.
- `perf/4.1-native-hwaccel` is the product/performance branch.
- Upstream `master` is merged into the perf branch by the refresh workflow.
- When `FfmpegEngine.cs` or `VideoStreamDecoder.cs` conflict, the perf implementation is retained and the parity/safety reconciliation scripts reapply upstream behavior that the custom engine must preserve.
- Release builds consume committed source verbatim. They must not rewrite source code during release packaging.

## Status vocabulary

- **FORK-OWNED** — intentional implementation that provides functionality/performance not present in upstream. Preserve unless upstream gains an equivalent or better implementation.
- **PARITY BRIDGE** — upstream correctness/safety behavior adapted to the custom perf engine. Preserve the behavior, but the exact bridge code may be simplified over time.
- **UPSTREAM-OWNED** — originated in or was influenced by this fork, but upstream now has the canonical implementation. Do not maintain a second fork implementation.
- **TOOLING** — benchmark, release, sync, test, or maintenance infrastructure specific to this fork.
- **EXTRA REGRESSION GUARD** — test-only divergence that hardens behavior without claiming runtime ownership.

## Intentional fork-owned runtime divergences

| Area | Status | Primary files | Why it remains fork-owned |
| --- | --- | --- | --- |
| Batched/multi-position native decode | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/VideoStreamDecoder.cs`, `VDF.Core/FFTools/FfmpegEngine.cs` | Reuses one decoder across requested positions and can decode clustered positions sequentially instead of paying a complete seek/open path for every sample. |
| D3D11 32x32 gray-byte filter scaler | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/D3D11GrayByteScaler.cs` | Scales hardware frames to the tiny matching image on the GPU before transfer to CPU memory. |
| D3D11 VideoProcessor gray-byte path | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/D3D11VideoProcessorGrayByteScaler.cs` | Alternative direct D3D11 VideoProcessor route for producing a tiny NV12/P010 result before staging/download. |
| Gray-byte extraction helper | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/Gray32FrameExtractor.cs` | Specialized low-overhead extraction used by the perf paths. |
| Capability-scoped hardware policy | FORK-OWNED | `VDF.Core/FFTools/FfmpegEngine.HardwarePolicy.cs` | Tracks failures/success by codec/family rather than globally poisoning otherwise-working hardware decode; includes adaptive D3D11 gray-byte decisions. |
| Native health state | FORK-OWNED | `VDF.Core/FFTools/FfmpegEngine.NativeHealth.cs` | Adds perf-engine-specific native binding/fallback health management. |
| Extraction telemetry | FORK-OWNED | `VDF.Core/FFTools/FfmpegEngine.Telemetry.cs` | Measures native/D3D11 behavior without changing upstream's public scan model. |
| AI/native compatibility layer | FORK-OWNED + PARITY BRIDGE | `VDF.Core/FFTools/FfmpegEngine.AiCompat.cs`, `FfmpegEngine.AiCompatOverloads.cs` | Keeps AI matching compatible with the custom native engine and its fallbacks. |
| Hardened combined gray+AI process fallback | FORK-OWNED + PARITY BRIDGE | `VDF.Core/FFTools/FfmpegEngine.AiProcessCombined.cs` | Uses upstream's one-decode concept while adding bounded process I/O, tiled-HEIF handling, H.264 reference-warning detection, accurate-seek recovery, and safe fallback to the established separate paths. |
| Native-library discovery/load safety | FORK-OWNED DELTA | `VDF.Core/FFTools/FFmpegNative/FFmpegHelper.cs` | The perf branch keeps stronger Windows DLL-set discovery/load safeguards around the native engine. |
| pHash pair hot loop | FORK-OWNED MICRO-OPT | `VDF.Core/ScanEngine.cs` | Preserves upstream quorum/all-sample semantics but precomputes the strict Hamming-bit threshold once per pair and performs direct `PopCount` comparisons instead of recomputing the percentage-to-bit threshold for every sampled position. Upstream's canonical quorum tests remain the semantic contract. |

## Upstream correctness carried into the custom engine

These behaviors are **not optional fork features**. They are upstream correctness/safety semantics that must remain true even though the perf branch owns the surrounding decoder implementation.

| Upstream behavior | Status in perf branch | Bridge |
| --- | --- | --- |
| Native timeout budget is re-armed per decoded position; software fallback frames are not passed through hardware-frame transfer | PARITY BRIDGE | Custom `VideoStreamDecoder` implementation + reconciliation. |
| Decoded-frame dimensions/pixel format are trusted over stale open-time metadata before conversion | PARITY BRIDGE | Custom conversion sites and upstream safety reconciliation. |
| Partial-clip visual verification uses software decoding to avoid fatal in-process GPU-driver faults for a tiny verification workload | PARITY BRIDGE | Perf `GetGrayFrames`/fallback behavior. |
| Corrupt/truncated software-decode failures can skip the redundant process retry ladder | PARITY BRIDGE | `ShouldSkipProcessRetryForCorruptFile` and its production call sites. |
| Tiled Apple HEIF/HEIC stream groups are refused by the single-stream native decoder and routed through FFmpeg's assembled `[0:g:0]` process graph | PARITY BRIDGE | `FfmpegEngine.UpstreamCompat.cs`, native refusal call sites, and AI/process handling. |
| Gray + AI process fallback can derive both outputs from one decode | PARITY BRIDGE, HARDENED | `FfmpegEngine.AiProcessCombined.cs`. |
| Anamorphic/SAR metadata survives hardware-frame transfer and display thumbnails use display aspect | UPSTREAM-OWNED semantics | Inherited from upstream plus preserved by the custom decoder. |

The compatibility partials and scripts exist because retaining the perf decoder during an upstream conflict can otherwise copy an API/property without copying the behavior that made upstream add it. Any new upstream correctness commit touching the owned FFmpeg files should therefore be audited semantically, not resolved only until the branch compiles.

## Features that are now upstream-owned

These no longer justify a fork implementation. The perf branch should inherit their canonical implementation from `master`.

| Feature/fix | Upstream status |
| --- | --- |
| Multi-position pHash quorum / required matching-sample ratio | Upstream adopted it from this fork and subsequently refined snapshot construction, all-sample difference averaging, quorum precomputation, CLI handling, and tests. The perf branch retains only a semantically equivalent hot-loop micro-optimization, not a separate quorum design. |
| Separate CPU-bound matching parallelism | Upstream adopted it from this fork as `MatchingMaxDegreeOfParallelism`. Old fork matching-concurrency implementation has been removed. |
| Comparer zoom/pan/swipe, dual-mode loading, and stale async thumbnail completion fixes | Upstream adopted the fork work and owns the implementation now. |
| Anamorphic/SAR-correct display thumbnails | Upstream adopted the fork work and owns the general implementation now. |
| Reverse-proxy scheme trust / Secure auth cookie / password logging hardening | Upstream adopted and reworked the fork work; Web behavior should come from upstream. |

### Explicit upstream provenance

Upstream commits that explicitly credit `marcmy/videoduplicatefinder (perf/4.1-native-hwaccel)` include:

- `f55f25d` — pHash matching requires a quorum of sampled positions.
- `2761177` — native decoder timeout per position + software-frame hardware-download guard.
- `99ddb67` — separate worker cap for CPU-bound matching phases.
- `3aced4b` — anamorphic display-thumbnail sizing.
- `c8edaf3` — reverse-proxy/auth hardening.
- `721e77e` — comparer zoom/pan/swipe/mode fixes.

## Tooling and regression guards that intentionally differ from upstream

| Area | Status | Files / reason |
| --- | --- | --- |
| Upstream sync / perf refresh | TOOLING | `.github/workflows/sync-master.yml`, `.github/workflows/refresh-patched-branch.yml` |
| Upstream parity reconciliation | TOOLING | `.github/scripts/patch-perf-merge.py`, `patch-perf-upstream-parity.py`, `patch-perf-upstream-safety.py`, `patch-perf-ai-process.py` |
| Windows x64 GUI-only Native AOT release | TOOLING | `.github/workflows/releases.yml` |
| Performance regression probe | TOOLING | `VDF.Benchmarks/Scenarios/RegressionProbe.cs`, `VDF.Benchmarks/README.md` |
| Perf-specific FFmpeg regression coverage | TOOLING | `VDF.Core.Tests/FFTools/*`, `VDF.IntegrationTests/FFTools/*` where the test exercises a fork-only path |
| Comparer mode regression | EXTRA REGRESSION GUARD | `VDF.GUI.Tests/ThumbnailComparerModeTests.cs` pins the upstream-owned selection-vs-bitmap-load behavior, for which upstream currently has no equivalent dedicated test file. |
| Folder-count cancellation race | EXTRA REGRESSION GUARD | `VDF.GUI.Tests/FolderCountingServiceTests.cs` issues cancellation from the first worker progress callback so an unusually fast directory walk cannot finish before the test thread gets scheduled. Runtime code is unchanged. |
| D3D11 dependency | FORK-OWNED SUPPORT | `VDF.Core/VDF.Core.csproj` adds `Vortice.Direct3D11` for the two fork-owned D3D11 gray-byte paths. |

The normal Windows PR workflow is intentionally kept at upstream test coverage (Core, CLI, GUI, Web, integration). The perf release workflow adds its own stricter GUI/FFmpeg/Native-AOT gates on top; it should not weaken upstream coverage merely because only the GUI artifact is released.

## Completed cleanup from the 2026-08-08 audit

- Removed `ScanEngine.MatchingConcurrency.cs` and its duplicate tests after upstream absorbed the matching-parallelism implementation.
- Removed the fork's old `PHashSampleQuorumTests.cs`; upstream `PHashQuorumTests` is a strict superset and directly exercises the current implementation without reflection.
- Removed an extra `MainWindowVM.SyncCoreSettings` pHash-ratio assignment; upstream already assigns it later in the same method.
- Removed unused `DuplicateItem.ThumbnailDisplayAspectCorrected` state left from the pre-upstream SAR implementation.
- Reverted a test-only async rewrite of `PauseTokenSourceTests.cs` to upstream because it added no fork-specific coverage.
- Restored upstream GUI and Web tests in `.github/workflows/dotnetcore.yml` instead of carrying a weaker PR gate.

## Remaining maintenance cleanup

1. Consolidate the four reconciliation scripts where practical and add explicit idempotence tests. The desired end state is fewer textual source transformations and more stable source partials.
2. Periodically shrink `FfmpegEngine.UpstreamCompat.cs` when a bridge can move into the permanent owning partial without making future upstream merges harder. Some helpers intentionally remain only to preserve upstream test/source compatibility even when the perf production path has a richer policy.
3. Re-audit `ScanEngine.cs` whenever upstream changes pHash threshold semantics. The fork's direct `PopCount` loop is allowed only while it is exactly equivalent to upstream `PHashCompare.IsDuplicateByPercent(..., strict: true)` semantics.

## Maintenance rules

1. Keep `master` upstream-first. Do not land product features there merely to make perf refresh easier.
2. Merge current `master` into `perf/4.1-native-hwaccel`, then run reconciliation immediately.
3. On conflicts in fork-owned FFmpeg files, compare **behavior**, not just signatures/build errors.
4. Every upstream commit touching native decode, image/video conversion, FFmpeg process fallback, AI frame extraction, or partial-clip verification gets a parity review against the perf engine.
5. When upstream absorbs a fork feature, remove the duplicate fork implementation after proving the inherited implementation and tests cover it.
6. Release only committed source after Core, GUI, FFmpeg integration, Native AOT publish, and archive-validation gates pass.
7. Use the regression probe on the same machine/driver/corpus when evaluating performance changes; do not compare GPU numbers across unlike environments.
8. Prefer correctness and bounded fallback behavior over preserving a fast path for pathological media. Mainline scanning can be aggressive; tiny verification paths should stay conservative.

## Next audit targets

- Add explicit idempotence tests for each reconciliation script, then consolidate the scripts where safe.
- Build a small evil-media corpus covering tiled HEIF, corrupt/truncated streams, H.264 random-access recovery, 10-bit HEVC, VP9, odd dimensions, SAR/rotation, VFR, very short clips, and mid-stream layout changes.
- Promote the regression probe into a repeatable workflow/baseline process; GPU comparisons require a stable hardware runner.
- Profile the full scanner after the native extraction optimizations to find the new end-to-end bottleneck rather than assuming FFmpeg extraction remains dominant.
- Consider precomputing the pHash strict Hamming threshold once per compare phase (as already done for required quorum matches) if profiling shows the pHash pair loop is significant.
