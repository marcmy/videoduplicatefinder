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
