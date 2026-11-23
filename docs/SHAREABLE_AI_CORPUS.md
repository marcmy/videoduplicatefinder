# Reproducible, shareable AI video benchmark corpus

**No personal media is read or uploaded.** This experiment uses only video
generated locally by FFmpeg's built-in `lavfi` sources
(`testsrc2`, `mandelbrot`, `testsrc`). It has no download step or
external media dependency. The corpus and reports can be recreated by anyone
with FFmpeg, PowerShell 7 and .NET 11, subject to encoder-version differences.

## Generate the corpus

From a clean benchmark worktree:

```powershell
$corpus = Join-Path $env:TEMP 'VDF.PublicAiCorpus'
pwsh -NoProfile -File .\scripts\New-ShareableAiCorpus.ps1 -OutputDirectory $corpus
```

The script generates 21 short H.264 MP4 files: 3 sources, 5 transformed
variants per source (compressed, resized, two crop levels, brightness) plus a
partial-time subset per source. It extracts 36 corresponding 224x224 RGB24
frame samples at two timestamps from the 18 full-length clips. The extra
partial clips are reserved for end-to-end temporal scanning; they do not
participate in aligned-frame comparisons because their timelines are shifted.
Every clip and frame has a SHA-256 in `manifest.json`.

All video and frame filenames in the manifest are **relative**; it contains
no local usernames, private video paths or browser data. Use `-Force` to
regenerate the same destination.

This corpus gives *known aligned positives* (different encodes/effects from
the same generator at the same timestamp) and other pairs that can be used as
distractors. Pairs not aligned by source/time are **not guaranteed to be
semantic negatives**. This procedural data is useful for repeatable
threshold-drift and provider comparisons, not a replacement for natural-scene
duplicate-detection precision/recall benchmarks.

## Benchmark the production baseline against FP16 CUDA

The real ONNX models are downloaded and SHA-verified by the existing benchmark
harness. The following runs the production-style INT8/DirectML configuration
against the proposed FP16/CUDA EXTENDED optimization. Both use exactly the
same 36 generated RGB frames.

```powershell
pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1 `
    -Model int8 -Providers directml -CorpusDirectory $corpus

pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1 `
    -Model fp16 -Providers cuda -CudaOptimization extended -CorpusDirectory $corpus

pwsh -NoProfile -File .\scripts\Compare-OnnxModelScores.ps1 `
    -BaselineProvider directml -CandidateProvider cuda `
    -BaselineModel int8 -CandidateModel fp16 `
    -CandidateOptimization extended -CorpusTag public
```

The runner reads only the `manifest.json` and its explicitly declared RGB
frames, checks each frame's SHA-256, then scores them. Reports contain only
sample IDs and hashes, not user paths or RGB payloads. Absolute profiler
paths are printed only to the local console; new JSON reports contain
`profileFileName` rather than `profilePath`. Benchmark artifact filenames
contain `-public-` to prevent overwriting previous six-frame test results.

A useful additional comparison is FP16 CUDA BASIC vs EXTENDED on the same
corpus. Both must be rerun with `-CorpusDirectory $corpus`; do not compare
one public corpus result to a six-frame synthetic benchmark.

## Package only shareable metadata

```powershell
$reports = Join-Path $env:TEMP 'VDF.OnnxBenchmark\reports'
$files = @(Get-ChildItem -LiteralPath $reports -Filter '*-public-*.json' -File |
    ForEach-Object FullName)
$files += Join-Path $corpus 'manifest.json'
Compress-Archive -Path $files `
    -DestinationPath "$HOME\Downloads\VDF-Shareable-AI-Results.zip" -Force
```

This archive contains neither source MP4s nor extracted RGB frames. It is
enough to compare scores and thresholds; optionally include the ONNX profiler
JSON files if operator-level analysis is requested. Always inspect a ZIP's
contents before publicly uploading it. Old benchmark report formats may
include local absolute `profilePath` fields; use new reports for sharing.

The clips are not derived from anyone's personal videos. If you later add
an actual film to improve natural-scene coverage, only use a source whose
license permits your intended use, preserve the attribution, and keep it
separate from this strictly generated corpus.

**Limitations:** The sampling/embedding harness tests model behavior on
real decoded and transformed video frames, but it does not execute VDF's full
frame-selection, temporal matching, or database scan. It also cannot infer
precision and recall on real-world natural footage from synthetic textures.
Keep production thresholds and model unchanged until separate full-pipeline
validation.


## First public-corpus results (2026-11-23, RTX 2070 Super)

The complete shareable artifact contains only `manifest.json` and two
backend benchmark reports. Their 36 frame IDs and per-frame SHA-256 values
match exactly, so all 630 cross-model pairs are like-for-like. The source
corpus consists of 3 procedural FFmpeg sources, 6 transforms per source and
2 times. The 90 "aligned" pairs are same-generator, same-timestamp,
different-transform samples (15 pairs per timestamp, 2 timestamps per
generator, 3 generators). There are 108 same-generator but different-time
pairs, and 432 cross-generator pairs. Pairs are heavily correlated; 630
is **not** the independent sample size for accuracy inference.

| Backend | Single-frame median | Batch-of-eight median | GPU node events | CPU node events |
|---|---:|---:|---:|---:|
| INT8 DirectML (existing production path) | 23.4479 ms | 133.2250 ms | 901 | 11 |
| FP16 CUDA EXTENDED (candidate) | 4.1682 ms | 11.9739 ms | 374 | 11 |

This is a 5.63x single-frame and 11.13x batched **embedding inference**
speedup, not a full VDF scan speedup. One benchmark run per provider.

The maximum absolute **quantized similarity score** change across all
630 pairs is **11.6064 percentage points** (between two different source
patterns, far below the classification thresholds). For the 90 aligned
variant pairs, mean absolute drift is **1.233 points**, maximum
**6.2372 points**; their average score remains ~93.7% under both models.
This illustrates why maximum drift alone is not a good accuracy metric.

| Comparison / threshold | INT8 pass | FP16 pass | INT8-only | FP16-only | Agreement |
|---|---:|---:|---:|---:|---:|
| All 630 pairs at 89% | 145 | 145 | 5 | 5 | 620/630 |
| All 630 pairs at 94% | 85 | 91 | 3 | 9 | 618/630 |
| Aligned 90 pairs at 89% | 79 | 79 | 2 | 2 | 86/90 |
| Aligned 90 pairs at 94% | 51 | 51 | 2 | 2 | 86/90 |
| Cross-source 432 pairs at 89% or 94% | 0 | 0 | 0 | 0 | 432/432 |

At 94%, `c-crop08-t8` vs `c-crop16-t8` changes from **90.0552%** to
**96.2924%** (+6.2372 points, same-generated-source and timestamp).
Conversely, `b-crop08-t8` vs `b-crop16-t8` changes from 94.7734%
to 92.3740% (−2.3994 points). Some other shifts involve the *same*
source at different timestamps and are **not labeled negative**. No
cross-source pair passed either threshold in this particular corpus,
but unrelated natural video content remains untested.

This is **not evidence of equivalent real-video recall or precision**.
At the aligned-pair level the 89% and 94% pass *counts* match, but
specific pair identities differ; 4/90 aligned decisions flip at each
threshold. Such changes can affect near-threshold matching, and frame
scores are not the same as full-video, union or temporal-partial decisions.

**Do not globally change the 89% or 94% cutoffs yet.** A better
experimental next step is to mine borderline aligned *generated* pairs,
add more parameterized crop/brightness/compression levels and diverse
publicly licensed natural scenes, then compare per-pair errors and the
full VDF matching pipeline. Keep legacy INT8 caches isolated by model
identity and leave production INT8/DirectML as default until validation.
