# ONNX FP16 CUDA Experimental Windows Test Build

This is a **test-only** build from draft PR #179, NOT an upstream or
production VDF release. It contains the published Windows x64 GUI
(Native AOT) and CLI, an isolated launcher for each provider/model
combination, and a completely generated reproducible video corpus.

## Download and prerequisites

Open the PR's latest **Windows 64-bit** GitHub Actions run; at the bottom
of the run, download the artifact
`vdf-onnx-fp16-experimental-win-x64`. Unzip the GitHub artifact, then
extract `VDF-ONNX-FP16-Test-win-x64.zip` to a writable folder.
The bundle must contain `GUI/VDF.GUI.exe`, `CLI/vdf-cli.exe`, and
the `Launch-*.cmd` scripts.

Needs Windows x64, an NVIDIA GPU supported by your installed ONNX CUDA
runtime, compatible CUDA and cuDNN DLLs on PATH (on the reference machine:
CUDA 13.5 and cuDNN 9.28.1), the Microsoft Visual C++ x64 runtime, and
PowerShell 7. The first AI-enabled scan downloads an ONNX Runtime package
and the SHA256-pinned model. FFmpeg must be on PATH for the full-video
test corpus and CLI scan. Run the two modes **separately**.

**cuDNN dependency:** CUDA's ONNX Runtime session can initialize while
`cudnn64_9.dll` remains undiscoverable; the first DINOv2 convolution
then fails with a CUDA kernel error. The bundled
`Initialize-VdfCudaDependencies.ps1` verifies that cuDNN 9 is available
before the FP16 run. It searches the current PATH, an optional
`VDF_CUDNN_BIN` directory, and common NVIDIA/CUDA installation roots
without scanning arbitrary media folders. If several installs are found,
select the correct one explicitly (do not mix cuDNN CUDA variants):

```powershell
$env:VDF_CUDNN_BIN = 'C:\Program Files\NVIDIA\CUDNN\v9.x\bin'
# Replace that EXAMPLE with the real folder containing cudnn64_9.dll.
```

Paths under Python/conda environments may be used if they contain the
matching cuDNN 9 DLLs. The helper only modifies the *current process*
PATH and never downloads or copies GPU DLLs. The FP16 CUDA
`EnsureReady()` now executes a real inference before a scan starts.
When `VDF_AI_STRICT_INFERENCE=1` is set by the test scripts, any
later ONNX failure in union/dense AI matching aborts the run rather
than producing an apparently successful but incomplete comparison.
Normal nonexperimental VDF remains best-effort as before.

## Launch the GUI

Double-click either:

- `Launch-FP16-CUDA.cmd`: **FP16 model + CUDA + EXTENDED** optimizations
- `Launch-INT8-DirectML.cmd`: **original INT8 model + DirectML** control

Enable AI matching (and, optionally, AI partial detection) in VDF's
Settings → Matching before scanning generated sample clips. The
settings page will identify the experimental FP16 model in the runtime
description. Use the downloaded test build, not your installed copy.

The launch scripts set `VDF_DATA_DIR` to a dedicated directory under
`%LOCALAPPDATA%/VDF-ONNX-Test/<mode>`. This keeps test settings, the
scan database, downloaded ONNX runtimes, embedding sidecar caches and
log files separate from the user's installed VDF. The FP16 and INT8
modes also have separate test data directories. The app's normal
model and provider defaults are unchanged if these settings aren't supplied.

**Never scan personal video folders when preparing shareable results.**
Use only the FFmpeg-generated corpus below. The two GUIs should not be
running simultaneously if their windows appear hard to distinguish.

## Run the actual full scan pipeline on shareable sample clips

From PowerShell 7 in the extracted artifact folder:

```powershell
$corpus = Join-Path $env:TEMP 'VDF.PublicAiCorpus'
pwsh -NoProfile -File .\New-ShareableAiCorpus.ps1 -OutputDirectory $corpus

pwsh -NoProfile -File .\Compare-PublicAiFullScans.ps1 -CorpusDirectory $corpus
```

The comparison script refuses to scan unless all 21 expected generated
MP4 files are present, exactly listed in `manifest.json`, and pass
their SHA-256 checks. It calls **VDF.CLI scan-and-compare**, with
`--ai-matching --ai-partial`, on the actual MP4 media for both modes.
Every run uses a fresh, private `VDF_DATA_DIR` and separate scan
database; all existing user VDF data remains untouched. **No automatic
deletion or marking of files is performed.**

The resulting shareable files live in
`%TEMP%/VDF.PublicAiFullScan`:
`shareable-int8-directml.json`,
`shareable-fp16-cuda.json`,
`shareable-group-differences.json`.
Results contain generated clip IDs, similarity scores, matching flags,
and group memberships; **not absolute media paths**. The script
removes the raw path-bearing CLI result JSON after sanitizing it,
and clears previous shareable result files at the start of each run
so stale results from a prior failed scan cannot be mistaken for a
new successful comparison.
If an error interrupts the run, keep its `private-*` state folders
private.

To package **only** safe files for sharing:

```powershell
$results = Join-Path $env:TEMP 'VDF.PublicAiFullScan'
$files = @(Get-ChildItem $results -Filter 'shareable-*.json' -File |
    ForEach-Object FullName)
$files += Join-Path $corpus 'manifest.json'
Compress-Archive -Path $files -DestinationPath "$HOME\Downloads\VDF-Full-Scan-Public-Results.zip" -Force
```

Check the ZIP contents before sharing. Do NOT zip the `private-*`
directories, VDF logs, runtime downloads, or the complete test-data
directory.

### What this does and doesn't establish

This test executes the real VDF scan engine, both its classic matching
and its opt-in AI union and dense partial passes. Group counts, group
membership differences, and per-item flags show the final decisions,
not just isolated frame cosine similarities. The clips are procedurally
generated and correlated, however, so a match/no-match difference
cannot establish natural-video precision or recall.

The FP16/DirectML/CUDA comparison previously showed threshold flips in
a frame-level generated corpus. The full-video test measures their
practical effect; it is not a justification for changing the 89%
and 94% thresholds globally.

### Embedding cache safety

The original INT8 files `UnionEmbeddings.db` and `DenseEmbeddings.db`
retain their normal names in standard installations. The experimental
FP16 model gets its own SHA256/preprocessing/provider/optimization
scoped sidecar filenames, and uses its own private test data directory.
You do **not** need to delete existing caches.

When the application is launched normally, without `VDF_AI_MODEL`
and the CUDA variables, it retains its existing INT8/DirectML default.
The experimental build is for validation only.


## Next experiment: isolated temporal AI partial-duplicate detection

The original 21-video public corpus contains five-second excerpts, too short
to satisfy the dense AI partial detector's requirement for at least four
temporally consistent frame hits. The next experiment deliberately isolates
the **AI partial pass**, instead of running union matching before it.

The experimental test ZIP now includes two more scripts:
`New-ShareableAiPartialCorpus.ps1` and
`Compare-PublicAiPartialScans.ps1`. From PowerShell 7 in the **newly
extracted** ZIP's root folder:

```powershell
# The existing 21-video baseline remains intact.
$partialCorpus = Join-Path $env:TEMP 'VDF.PublicAiPartialCorpus'
pwsh -NoProfile -File .\New-ShareableAiPartialCorpus.ps1 -OutputDirectory $partialCorpus

# Uses the existing extracted cuDNN 9 directory; no permanent PATH changes.
$env:VDF_CUDNN_BIN = Join-Path $env:USERPROFILE 'Downloads\cudnn-windows-x86_64-9.28.1.4_cuda13-archive-extracted\cudnn-windows-x86_64-9.28.1.4_cuda13-archive\bin\x64'
pwsh -NoProfile -File .\Compare-PublicAiPartialScans.ps1 -CorpusDirectory $partialCorpus
```

The generator creates three 90-second procedural originals and
45-second excerpts starting at second 20, under separate test-case
directories: **9 positive** pairs (same source, plain trim/compression/
crop) and **3 negative** pairs (different sources). No user media is read,
copied, or uploaded. The test runner verifies the file hashes from
`manifest.json`, scans each two-video case separately, and enables
only `--ai-partial --ai-partial-hit-percent 89` (not the AI union pass).
This prevents already-grouped full-length videos from being skipped
by the dense partial detector.

INT8/DirectML and FP16/CUDA EXTENDED use separate private state
directories, while each case has its own database and embedding caches.
CUDA's real-inference preflight and strict error behavior remain enabled.

The runner records whether each expected pair was grouped,
**whether it was specifically marked as an AI partial match**, the
matching flags, similarity, returned partial offset, and per-case
elapsed time. The final comparison summarizes changed decisions.
An unrelated pair forming a match is a potential false positive.

To zip **only shareable JSON and the manifest**:

```powershell
$partialResults = Join-Path $env:TEMP 'VDF.PublicAiPartialResults'
$files = @(Get-ChildItem -LiteralPath $partialResults -Filter 'shareable-partial-*.json' -File |
    ForEach-Object FullName)
$files += Join-Path $partialCorpus 'manifest.json'
Compress-Archive -Path $files -DestinationPath "$HOME\Downloads\VDF-Public-AI-Partial-Results.zip" -Force
```

Do not include the generated videos unless you intentionally want to
share them. Never include `private-*` scan databases or logs. The
shareable report describes cases using stable generated IDs, not
absolute paths. The scripts clear previous shareable outputs at the
start and publish reports only after both inference modes succeed.

**How to interpret:** A correct positive is a match with an
`AiMatched` and `PartialClip` flag at the intended nonzero offset.
An identical outcome count across backends is not enough if *which*
cases matched differs. With procedural patterns, false offsets are
possible; inspect the reported offsets. This still isn't a natural-video
accuracy estimate or a production threshold-calibration study.
