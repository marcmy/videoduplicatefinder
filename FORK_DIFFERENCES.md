# Fork differences: `perf/4.1-native-hwaccel`

This is the maintenance map for the actively maintained performance branch. It documents what the fork still owns, which upstream correctness behaviors are adapted around the custom engine, and which historical fork features are now upstream-owned.

Snapshot: 2026-08-08. At this snapshot the perf branch is fully based on current `master` (0 commits behind). The master-to-perf comparison contains 40 changed files; every current delta is accounted for below.

## Branch contract

- `master` is the upstream-tracking branch and should remain an upstream mirror apart from fork automation.
- `perf/4.1-native-hwaccel` is the product/performance branch.
- Upstream `master` is merged into the perf branch by the refresh workflow.
- When `FfmpegEngine.cs` or `VideoStreamDecoder.cs` conflict, the perf implementation is retained and the reconciliation scripts reapply upstream behavior that the custom engine must preserve.
- The reconciliation layer is run twice; pass #2 must be a no-op.
- Release builds consume committed source verbatim. They must not rewrite application source during packaging.

## Status vocabulary

- **FORK-OWNED** — intentional runtime implementation that provides functionality/performance not present upstream.
- **PARITY BRIDGE** — upstream correctness/safety behavior adapted to the custom perf engine.
- **PARITY TEST SHIM** — a small API/helper retained so inherited upstream tests can pin an upstream contract even when perf production code uses a richer/different policy.
- **UPSTREAM-OWNED** — historically fork-originated/influenced work for which upstream now owns the canonical implementation.
- **TOOLING** — benchmark, release, sync, test, or maintenance infrastructure specific to this fork.
- **EXTRA REGRESSION GUARD** — test-only divergence that hardens behavior without claiming runtime ownership.

## Current delta inventory

The current 40-file master-to-perf surface is explainable as follows:

- **Native/performance runtime:** custom decoder, D3D11 scalers, gray extractor, FFmpeg engine partials, native-library loading delta, pHash hot-loop micro-optimization, and D3D11 support dependency.
- **Parity/source compatibility:** upstream decoder-safety behavior, tiled-HEIF handling, corrupt-file fast fail, AI call-shape overloads, and inherited-test helper seams.
- **Tests/benchmarks:** fork-only engine tests, evil-media fixtures, native FFmpeg integration coverage, parity tests, and the performance regression probe.
- **Maintenance/release:** sync/refresh/reconciliation scripts, release gate, rolling tag handling, and documentation.

No unexplained runtime divergence remains from this audit. Historical commits that no longer affect the final tree are not treated as current features merely because they remain in branch history.

## Intentional fork-owned runtime divergences

| Area | Status | Primary files | Why it remains fork-owned |
| --- | --- | --- | --- |
| Batched/multi-position native decode | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/VideoStreamDecoder.cs`, `VDF.Core/FFTools/FfmpegEngine.cs` | Reuses one decoder across requested positions and can walk clustered samples without paying a complete open/seek path for every sample. |
| D3D11 32x32 gray-byte filter scaler | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/D3D11GrayByteScaler.cs` | Scales hardware frames to the tiny matching image on GPU before transfer to CPU memory. |
| D3D11 VideoProcessor gray-byte path | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/D3D11VideoProcessorGrayByteScaler.cs` | Alternative direct D3D11 VideoProcessor route for producing a tiny result before staging/download. |
| Gray-byte extraction helper | FORK-OWNED | `VDF.Core/FFTools/FFmpegNative/Gray32FrameExtractor.cs` | Specialized low-overhead extraction used by the perf paths. |
| Capability-scoped hardware policy | FORK-OWNED | `VDF.Core/FFTools/FfmpegEngine.HardwarePolicy.cs` | Tracks failures/success by codec/family rather than globally poisoning working hardware decode; includes adaptive D3D11 gray-byte concurrency/policy. |
| Native health state | FORK-OWNED | `VDF.Core/FFTools/FfmpegEngine.NativeHealth.cs` | Disables native binding globally only for binding/ABI infrastructure failures; media/decode failures remain operation-local. |
| Extraction telemetry | FORK-OWNED | `VDF.Core/FFTools/FfmpegEngine.Telemetry.cs` | Measures native/D3D11 behavior without changing upstream's public scan model. |
| AI/native extraction | FORK-OWNED + PARITY BRIDGE | `FfmpegEngine.AiCompat.cs`, `FfmpegEngine.AiCompatOverloads.cs` | Keeps AI matching on the custom native engine while preserving current upstream call shapes. The overload file is actively exercised by integration tests and is not dead compatibility residue. |
| Hardened combined gray+AI process fallback | FORK-OWNED + PARITY BRIDGE | `FfmpegEngine.AiProcessCombined.cs` | Uses upstream's one-decode idea but adds bounded process I/O, tiled-HEIF handling, H.264 warning detection, accurate-seek recovery, and safe fallback to established separate paths. |
| Native-library discovery/load safety | FORK-OWNED DELTA | `FFmpegNative/FFmpegHelper.cs` | Keeps stronger Windows DLL-set discovery/load safeguards around the native engine. |
| pHash pair hot loop | FORK-OWNED MICRO-OPT | `VDF.Core/ScanEngine.cs` | Uses upstream quorum/all-sample semantics but precomputes the strict Hamming threshold once per pair and performs direct `BitOperations.PopCount` comparisons. |

## Upstream correctness carried into the custom engine

These are not optional fork features. They are upstream contracts that must remain true even though perf owns the surrounding decoder implementation.

| Upstream behavior | Status in perf | Bridge |
| --- | --- | --- |
| Native timeout budget is re-armed per decoded position; software fallback frames are not sent through hardware-frame transfer | PARITY BRIDGE | Custom `VideoStreamDecoder` + reconciliation. |
| Decoded-frame dimensions/pixel format are authoritative over stale open-time metadata | PARITY BRIDGE | Custom conversion sites + upstream safety reconciliation. |
| Partial-clip visual verification uses software decode to avoid fatal in-process GPU-driver faults for a tiny workload | PARITY BRIDGE | Perf `GetGrayFrames` and process fallback. |
| Corrupt/truncated software decode can skip a redundant process retry | PARITY BRIDGE | `ShouldSkipProcessRetryForCorruptFile` + production call sites. |
| Tiled Apple HEIF/HEIC stream groups are refused by the single-stream native decoder and routed through FFmpeg's assembled `[0:g:0]` graph | PARITY BRIDGE | `FfmpegEngine.UpstreamCompat.cs`, native refusal sites, AI/process handling. |
| Gray + AI process fallback can derive both outputs from one decode | PARITY BRIDGE, HARDENED | `FfmpegEngine.AiProcessCombined.cs`. |
| Anamorphic/SAR metadata survives hardware transfer and display thumbnails use display aspect | UPSTREAM-OWNED semantics | Inherited from upstream and preserved by custom decoder paths. |

`FfmpegEngine.UpstreamCompat.cs` intentionally contains two pure helper seams whose production equivalents differ in perf:

- `ResolveSourcePixelFormat` is retained because inherited upstream #861 tests pin the frame-format-over-open-time-format rule.
- `GetNativeFailureLogMode` is retained because inherited upstream #861 tests pin upstream's logging-tier policy. Perf production native health intentionally uses a different infrastructure-failure policy, so this helper is a **PARITY TEST SHIM**, not perf runtime policy.

The compatibility layer exists because preserving the custom decoder during a merge can otherwise copy a new property/signature without copying the behavior that motivated upstream's change.

## Historical commit disposition

The useful way to read branch history is by feature family, not by raw commit count. Refresh merges, CI repair iterations, and upstream commits merged into the branch are historical mechanics rather than independent fork features.

| Fork commit / family | Original purpose | Current disposition |
| --- | --- | --- |
| `a8f906f` | Seed 4.1 native FFmpeg decoder layer | **FORK-OWNED, EVOLVED.** Ancestor of the current custom `VideoStreamDecoder`/native decoder path. |
| `b1d4828` | Integrate native FFmpeg extraction engine | **FORK-OWNED, EVOLVED.** Core reason the perf branch still exists. |
| `e93ca19` and nearby native-regression restoration commits | Restore native FFmpeg regression coverage | **EXTRA REGRESSION GUARD, EVOLVED.** Superseded/expanded by current Core + native-enabled integration/evil-media coverage. |
| `ae42461`, `5123643`, `8219f8a`, `e54ce6a`, `6d32c53` | Port/reserve CPU headroom during duplicate matching | **UPSTREAM-OWNED.** Canonical upstream implementation is `MatchingMaxDegreeOfParallelism` from `99ddb67`; old fork matching-concurrency implementation/tests were removed. |
| Early pHash quorum work | Require multiple sampled positions to match | **UPSTREAM-OWNED.** Upstream `f55f25d` adopted it and `10b5fa1` refined averaging, precomputation, CLI handling, and tests. Perf retains only a semantically equivalent hot-loop micro-optimization. |
| Early comparer fixes | Zoom/pan/swipe/mode selection and stale async extraction | **UPSTREAM-OWNED.** Canonical upstream implementation is `721e77e`. |
| Early anamorphic thumbnail work | Respect SAR/display aspect | **UPSTREAM-OWNED.** Canonical upstream implementation is `3aced4b`. |
| Early Web proxy/auth hardening | Proxy scheme trust, Secure cookie, password-log safety | **UPSTREAM-OWNED.** Canonical upstream implementation is `c8edaf3`. |
| Native timeout/software-frame guard work | Per-position timeout and safe software fallback frame handling | **PARTIALLY UPSTREAMED / PARITY BRIDGE.** Upstream `2761177` owns the correctness rule; perf keeps it inside a larger custom decoder. |
| `57d583a` | Add regression harness and split FFmpeg engine | **FORK-OWNED + TOOLING.** Evolved into the current partial architecture and pinned release performance gate. |
| `d1e77c` | Resolve conflicts after upstream adopted 4.1 performance work | **HISTORICAL RECONCILIATION.** Important milestone, not a separate runtime feature. |
| `3796131`, `f155568`, `f5a4e82`, `40c3000` and related AI compatibility follow-ups | Keep upstream AI features working against the retained perf engine | **FORK-OWNED + PARITY BRIDGE, EVOLVED.** Current form is `FfmpegEngine.AiCompat*` plus hardened combined process fallback. |
| Later #861/#863/#867/#869 ports | Carry upstream crash/corrupt-file/HEIF safety into custom paths | **PARITY BRIDGE.** Upstream owns the rules; perf owns their integration into the specialized engine. |
| 2026-08-08 evil-media/native-CI work | Make pathological media and native bindings release-blocking | **TOOLING / EXTRA REGRESSION GUARD.** Current and intentional. |
| 2026-08-08 performance baseline/gate work | Compare candidate against pinned known-good product code on the same runner | **TOOLING.** Current and intentional. |

### Explicit upstream provenance

Upstream commits that explicitly credit `marcmy/videoduplicatefinder (perf/4.1-native-hwaccel)` include:

- `f55f25d` — pHash matching requires a quorum of sampled positions.
- `2761177` — native decoder timeout per position + software-frame hardware-download guard.
- `99ddb67` — separate worker cap for CPU-bound matching phases.
- `3aced4b` — anamorphic display-thumbnail sizing.
- `c8edaf3` — reverse-proxy/auth hardening.
- `721e77e` — comparer zoom/pan/swipe/mode fixes.

## Tooling and regression guards that intentionally differ

| Area | Status | Files / reason |
| --- | --- | --- |
| Upstream sync / perf refresh | TOOLING | `.github/workflows/sync-master.yml`, `.github/workflows/refresh-patched-branch.yml` |
| Upstream parity reconciliation | TOOLING | `patch-perf-merge.py`, `patch-perf-upstream-parity.py`, `patch-perf-upstream-safety.py`, `patch-perf-ai-process.py`; complete layer is run twice and second pass must be a no-op. |
| Windows x64 GUI-only Native AOT release | TOOLING | `.github/workflows/releases.yml`; actual rolling `4.1.x` Git tag is force-moved before release asset replacement. |
| Performance regression gate | TOOLING | `RegressionProbe.cs`, `perf-regression-gate.ps1`, pinned `perf/4.1-baseline`. Hosted gate uses process/native-CPU, same harness, same FFmpeg/corpus/runner, p50 throughput, 9 iterations + 2 warmups, 15% threshold, and confirmation pair on a breach. |
| Native FFmpeg CI | TOOLING / REGRESSION GUARD | Release CI installs checksum-verified BtbN GPL shared FFmpeg 8.1 so native-binding tests execute instead of skipping. |
| Evil-media corpus | EXTRA REGRESSION GUARD | Generated long-GOP, VFR, odd-size, ultra-short, rotation, truncation, and mid-stream resolution-change fixtures plus existing HEVC10/VP9/SAR/corrupt/HEIC coverage. |
| Comparer mode regression | EXTRA REGRESSION GUARD | `VDF.GUI.Tests/ThumbnailComparerModeTests.cs` pins an upstream-owned behavior not otherwise dedicated in upstream tests. |
| Folder-count cancellation race | EXTRA REGRESSION GUARD | `FolderCountingServiceTests.cs` makes cancellation deterministic under very fast walks; runtime code is unchanged. |
| D3D11 dependency | FORK-OWNED SUPPORT | `VDF.Core.csproj` adds `Vortice.Direct3D11`; `InternalsVisibleTo` for benchmarks supports the perf harness. |

## Completed cleanup from the 2026-08-08 audit

- Removed `ScanEngine.MatchingConcurrency.cs` and duplicate tests after upstream absorbed matching parallelism.
- Removed the old fork `PHashSampleQuorumTests.cs`; upstream's quorum suite is stronger and exercises the current implementation directly.
- Removed the duplicate `MainWindowVM.SyncCoreSettings` pHash-ratio assignment.
- Removed unused `DuplicateItem.ThumbnailDisplayAspectCorrected` state from the pre-upstream SAR implementation.
- Reverted a test-only `PauseTokenSourceTests` rewrite that added no fork-specific coverage.
- Restored upstream GUI and Web tests in the normal Windows CI gate.
- Added reconciliation fixed-point validation; its first live run caught and fixed a HEIF marker that could have duplicated a fallback block on later refreshes.
- Added the generated evil-media suite and made real shared/native FFmpeg coverage release-blocking.
- Added a pinned same-run performance regression gate and stabilized it against harness drift/build noise.
- Fixed the rolling release so the real `4.1.x` Git tag moves with the validated build instead of only changing release metadata.
- Reclassified `AiCompatOverloads.cs` as an active source-compatibility bridge after verifying current integration tests call both overloads.
- Reclassified upstream #861 log-throttle/source-format helpers as **PARITY TEST SHIMS** after verifying inherited upstream tests call them; they are intentionally not perf production policy.
- Rewrote `docs/4.1-perf-port.md` as historical provenance so it no longer claims upstream-owned matching/pHash/comparer work as current fork-owned features.

## Remaining maintenance work

1. Consolidate reconciliation scripts where practical. The fixed-point gate now protects their idempotence; prefer fewer textual transformations and more stable owning partials when that does not make upstream merges harder to reason about.
2. Profile the full scanner end-to-end. Native extraction is no longer automatically the dominant bottleneck; measure discovery, probing, extraction, pHash, AI, matching, DB work, and result construction before choosing the next optimization.
3. Periodically shrink `FfmpegEngine.UpstreamCompat.cs` only when a bridge/test seam can move or disappear without weakening inherited upstream contracts.
4. Re-audit the `ScanEngine` pHash micro-optimization whenever upstream changes strict threshold semantics.
5. Add a redistributable true tiled-Apple-HEIC fixture if one with clear provenance becomes available; the test is already opt-in through `VDF_TEST_TILED_HEIC`.

## Maintenance rules

1. Keep `master` upstream-first. Do not land product features there merely to make perf refresh easier.
2. Merge current `master` into `perf/4.1-native-hwaccel`, then run reconciliation immediately.
3. On conflicts in fork-owned FFmpeg files, compare **behavior**, not just signatures/build errors.
4. Every upstream commit touching native decode, image/video conversion, FFmpeg process fallback, AI frame extraction, or partial-clip verification gets a parity review against the perf engine.
5. When upstream absorbs a fork feature, remove the duplicate fork implementation after proving inherited implementation/tests cover it.
6. Release only committed source after structural checks, Core/GUI tests, performance gate, shared/native FFmpeg integration tests, Native AOT publish, and archive validation pass.
7. Keep `perf/4.1-baseline` pinned until an intentional performance change is independently accepted; never auto-promote the latest release into the baseline.
8. Compare GPU performance only on the same adapter/driver/power environment. Hosted runners are not a D3D11 performance oracle.
9. Prefer correctness and bounded fallback behavior over preserving a fast path for pathological media.
