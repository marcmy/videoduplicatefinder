# ONNX Runtime 1.32 / CUDA 13 compatibility gate

**Experimental only.** The Windows default remains DirectML. CUDA is enabled
only by `VDF_AI_PROVIDER=cuda` on Windows/Linux x64. Restart VDF after changing
the provider because the ONNX native library is selected at process startup.

| Backend | Native runtime | Managed API |
| --- | --- | --- |
| Windows DirectML | 1.24.4 | 1.24.4 |
| Windows/Linux CPU | 1.32.0 | 1.24.4 |
| Windows/Linux CUDA | 1.32.0 | 1.24.4 |

The managed 1.24.4 / native 1.32 pairing is not presumed compatible solely
because the C API is versioned: test the downloaded binary and actual model.

## Downloaded-runtime smoke

For a CPU smoke (works on GitHub's non-GPU runners), run:
```powershell
$env:VDF_TEST_ONNX_DOWNLOAD = '1'
$env:VDF_TEST_ONNX_GPU = '0'
dotnet test VDF.Core.Tests/VDF.Core.Tests.csproj -c Release --filter 'FullyQualifiedName~DownloadedNativeRuntime_ExecutesModel_OptIn'
```

For NVIDIA CUDA 13 and cuDNN 9 on Windows x64, add:
```powershell
$env:VDF_AI_PROVIDER = 'cuda'
$env:VDF_TEST_ONNX_GPU = '1'
```
On Linux x64 run the same filtered test with
`VDF_AI_PROVIDER=cuda VDF_TEST_ONNX_DOWNLOAD=1 VDF_TEST_ONNX_GPU=1`.

## Before merging

- Windows/Linux CI and Windows Native AOT publish must pass.
- Downloaded native CPU and DirectML backend smoke tests must pass.
- GPU machine must successfully load CUDA dependencies (CUDA 13, cuDNN 9)
  and run the **real DINOv2 INT8 model** without fallback to CPU.
- ONNX profiling must prove actual GPU operator execution rather than merely
  listing the CUDA execution provider.
- Compare normalized embeddings and duplicate decisions against the prior
  build on the same files, including borderline AI similarity thresholds
  (94% matching, 89% partial).
- Benchmark warmup, batch inference, full scan throughput and memory on the
  same RTX 2070 Super. Do not change DirectML defaults until validated.
- Test missing CUDA/cuDNN failures, re-downloads, and rollback.

The smoke test uses the checked-in tiny ONNX model; it is **not sufficient**
for the real-model correctness or GPU performance gates.


## Windows CUDA 13 / cuDNN 9 troubleshooting

The NVIDIA driver's `nvidia-smi` CUDA UMD value is **not** proof the CUDA
runtime or cuDNN DLLs are installed and visible to a .NET process.

If inference fails at `AveragePool` with
`cuDNN is unavailable ... cudnn64_9.dll ... error 2`, the CUDA provider
got as far as execution but cannot locate the cuDNN 9 library. This is
**not** a valid passing compatibility test. Install NVIDIA cuDNN 9 for CUDA
13 from https://developer.nvidia.com/cudnn-downloads and ensure the
directory containing `cudnn64_9.dll` is on PATH for the test process.

From the same PowerShell session:

```powershell
where.exe cudnn64_9.dll
where.exe cudart64_13.dll
$env:CUDA_PATH
# If installed but absent from PATH, find it and add its bin directory
$cudnn = Get-ChildItem "$env:ProgramFiles\NVIDIA\CUDNN" -Recurse -File \
  -Filter cudnn64_9.dll -ErrorAction SilentlyContinue | Select-Object -First 1
if ($cudnn) { $env:PATH = "$($cudnn.DirectoryName);$env:PATH" }
```

Windows can retain a loaded `onnxruntime.dll` until the test host exits.
The smoke test handles cleanup failures so the original inference error is
not masked. After the test host exits, stale `vdf-onnx-smoke*` directories
under the system temporary directory can be removed.


## Real DINOv2 CUDA vs DirectML vs CPU benchmark

The native-library smoke test uses the small test fixture. Run the real-model
benchmark separately on your NVIDIA Windows x64 system (PowerShell 7) **after**
configuring CUDA 13 and cuDNN 9 in the current shell:

```powershell
pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1
```

The script uses a **separate dotnet test process per provider** so Windows
does not keep the wrong ONNX DLL loaded. It runs in this order: DirectML
(native 1.24.4), CUDA (native 1.32.0) and CPU (native 1.32.0), with the
managed wrapper kept at 1.24.4. Each run verifies the real DINOv2 model's
pinned SHA-256 and requests **strict GPU mode** where appropriate. A failed
provider does **not** silently become a CPU benchmark.

The first run downloads and caches the DINOv2 model in a dedicated temporary
`VDF.OnnxBenchmark/components` folder; later runs reuse it and swap native
libraries between processes. The folder is separate from your actual VDF
scan database and AI installation.

Per-provider JSON reports and ONNX profiling traces are written to
`$env:TEMP\VDF.OnnxBenchmark\reports`. The script prints median inference
times for one frame and a batch of eight (inclusive of VDF preprocessing),
and compares the quantized pair scores at VDF's 94% and 89% thresholds.
It reports GPU and CPU execution-node counts from ONNX profiles. These are
operator-placement diagnostics, **not** percentage GPU utilization or proof
of performance improvement. Compare actual latency and end-to-end scan times.

Use `-Repeat 3` for multiple passes. The six deterministic synthetic images
are deliberately reproducible across CPU/DirectML/CUDA; they are **not**
representative real video frames, and the threshold-flip counts do not prove
real-world accuracy. Follow up with a representative media corpus and full
VDF scans before considering CUDA as the default.

Do not run different provider benchmarks simultaneously against the same
component folder: the on-demand downloader replaces the ONNX native files.


### FP32 model comparison (diagnosing CUDA's UINT8 CPU fallback)

The supplied INT8 profile on RTX 2070 Super showed 72 `MatMulInteger`
operators executing on CPU under CUDA, while DirectML executed them on
GPU. Those operators had `uint8` activation inputs. CUDA also placed 49
`DynamicQuantizeLinear` operators on CPU, leading to 171 inserted Memcpy
nodes. The issue is graph/provider partitioning, not a missing driver.

Test a non-quantized export **without changing production VDF models**:

```powershell
pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1 -Model fp32
```

The FP32 model is fetched from the same Xenova DINOv2-small repository at
`onnx/model.onnx` (~88.5 MB), SHA256 pinned to
`83141175ec78b4ff9a2bb58a4c7c264ba0054d1c2e122e5a8114b79a8d4179ea`.
It is cached separately as `dinov2-small-fp32.onnx`; no production
download URL, model cache, default execution provider, or release artifact
changes. On first use it also initializes the native runtime/model cache,
so allow for downloading.

`-Model int8` is the default. JSON report names and profiler traces
include the selected variant, allowing INT8 and FP32 runs to coexist.
Compare CPU/GPU node placement, latency, full embedding vectors and
similarity pairs. Changing INT8 to FP32 can change embedding values and
existing cached matching scores, so a production change would require
cache invalidation and real-media threshold validation.

Source model: https://huggingface.co/Xenova/dinov2-small/blob/main/onnx/model.onnx
CUDA UINT8 limitation context: https://github.com/microsoft/onnxruntime/discussions/26735


### FP32 profiling outcome and production migration gate (RTX 2070 SUPER)

Local full-model benchmark (2026-11-22; 1 run per provider; medians from 12
single-frame and 6 batch-8 iterations) showed FP32 CUDA **6.52 ms** single
and **28.95 ms** batch-8 versus FP32 DirectML **13.39 ms** single and
**86.12 ms** batch-8. Compared to CUDA INT8's **37.21 ms** single and
**199.86 ms** batch-8, FP32 CUDA is ~5.7x and ~6.9x faster respectively.
The JSON traces show zero inserted `MemcpyFromHost` / `MemcpyToHost` events
and only 11 CPU node events under FP32 CUDA, compared with 171 inserted
copies and 133 CPU node events under INT8 CUDA.

Float embedding coordinate differences across FP32 CUDA/DirectML/CPU were
<= 5.01e-7, and quantized pair scores were identical for the synthetic
images. **This is cross-provider agreement within FP32**, not agreement
between INT8 and FP32. Comparing the existing INT8 and new FP32 models on
the same sample inputs, quantized pair-score changes reached ~0.07 on
CPU, ~0.056 on DirectML, and ~0.054 on CUDA. The six synthetic samples
did not flip VDF's 89%/94% cutoffs; real-media checks are still required.

**Do not switch the production model merely by changing ModelPath.**
`UnionEmbeddingStore` and `DenseEmbeddingStore` currently validate records
by media file size and mtime and use fixed binary magic (`VDFAI002` and
`VDFAI003`), **not the ONNX model SHA256 or preprocessing identity**.
Switching models without invalidating/segregating these sidecar caches
could silently reuse INT8 embeddings during FP32 scans and yield false
matches or missed matches. A production migration must version both caches
by model identity (ideally hash plus preprocessing revision), invalidate or
separate old records, and verify representative real-media decisions first.
Current PR intentionally leaves production model and caches unchanged.


### FP16 model experiment and score-drift diagnostics

FP16 is available as a separate model benchmark variant (it does not replace
the production INT8 ONNX file). The 44.4 MB Xenova export at
https://huggingface.co/Xenova/dinov2-small/blob/main/onnx/model_fp16.onnx
is pinned to SHA-256
`4e9ea6fe106e2225e28ee3c1c3d53b5b92aa4af62142f6ed6b66b6a92213cf04`.

```powershell
# PowerShell 7, .NET 11, and CUDA 13 / cuDNN 9 remain required.
pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1 -Model fp16

# Compare scores for the same synthetic image pairs.
pwsh -NoProfile -File .\scripts\Compare-OnnxModelScores.ps1 -Provider cuda -CandidateModel fp16
pwsh -NoProfile -File .\scripts\Compare-OnnxModelScores.ps1 -Provider cuda -CandidateModel fp32
```

FP16 defaults to DirectML and CUDA because CPU ONNX Runtime does not
support many half-precision operators. You may explicitly request other
providers with `-Providers`. The embedder recognizes FP16 tensor inputs
and outputs by ONNX metadata, performing conversions only when required.

The first model comparison found up to ~7 percentage points score movement
between INT8 and FP32, but the largest shifts were among low-similarity
synthetic pairs (~45–58%). Among CUDA pairs above 89% the largest observed
shift was ~1.59 percentage points; there were no threshold flips at
89% or 94% in this tiny synthetic dataset. These are descriptive values,
**not bounds** on real-media drift. Do not globally retune thresholds or
claim recall/precision parity without labeled real-video validation.

### Safe persistent cache partitioning

The production INT8 model hash and preprocessing revision 1 continue to
use `UnionEmbeddings.db` and `DenseEmbeddings.db` unchanged. In contrast,
when `AiComponents.ModelSha256` is changed to a different verified model
or the preprocessing revision is incremented, VDF automatically uses
`UnionEmbeddings.<sha256>.p<revision>.db` and
`DenseEmbeddings.<sha256>.p<revision>.db` instead.

This prevents the two model variants from sharing embedding records and
preserves old caches for rollback. It is a filename partition rather than a
binary-format migration; no old files need to be deleted. The new model
download must also update the expected pinned SHA-256 and verify that file
before production use. Merely replacing bytes on disk while leaving the
configured SHA-256 unchanged is **not** a supported model switch. The FP16
and FP32 benchmark paths remain entirely separate from these production
sidecars.


### FP16 local benchmark results (2026-11-23) and CUDA graph-optimization follow-up

Local Windows RTX 2070 Super / CUDA 13.5 / cuDNN 9.28 tests with the
SHA-pinned FP16 export succeeded on CUDA and DirectML. Medians:

| Model / provider | Single RGB frame | Eight RGB frames |
|---|---:|---:|
| FP32 / CUDA | 6.52 ms | 28.95 ms |
| FP16 / CUDA | 7.81 ms | **17.81 ms** |
| FP32 / DirectML | 13.39 ms | 86.12 ms |
| FP16 / DirectML | 13.34 ms | 77.40 ms |

FP16 CUDA has about 1.63x higher batch throughput than FP32 CUDA, but its
single-frame latency is about 20% worse. FP16 CUDA profiling shows 634 GPU
node events, 11 CPU events and no explicit inserted Memcpy node events.
Both CUDA and DirectML FP16 sessions completed the real-model inference.

On the six synthetic frames, CUDA FP16 vs FP32 maximum quantized pair-score
drift was **0.211 percentage points**, and at high-similarity scores (>=89%)
the maximum was 0.068 points. FP16 CUDA vs FP16 DirectML had up to 0.490
points provider-dependent drift (unlike FP32 provider parity, which was
quantized-score identical). None of the 15 synthetic pairs crossed 89% or
94%; no samples were in the 89%-94% gap. This is not an accuracy validation
on actual videos. Real-media thresholds must be calibrated before model
migration. The INT8-vs-FP16 CUDA maximum difference was 5.369 points, mostly
for low-similarity synthetic pairs.

CUDA currently uses conservative ORT_ENABLE_BASIC graph optimization because
the DirectML path previously required it for the quantized ONNX graph.
Test alternate graph optimizations on FP16 without changing the default:

```powershell
pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1 -Model fp16 -Providers cuda -Repeat 3
pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1 -Model fp16 -Providers cuda -CudaOptimization extended -Repeat 3
pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1 -Model fp16 -Providers cuda -CudaOptimization all -Repeat 3

# Compare the first run's actual scores for changes at VDF thresholds:
pwsh -NoProfile -File .\scripts\Compare-OnnxModelScores.ps1 -Provider cuda -BaselineModel fp16 -CandidateModel fp16 -CandidateOptimization extended
pwsh -NoProfile -File .\scripts\Compare-OnnxModelScores.ps1 -Provider cuda -BaselineModel fp16 -CandidateModel fp16 -CandidateOptimization all
```

Every `dotnet test` invocation loads ONNX in a fresh process; the experimental
graph-optimization level is passed through `VDF_AI_CUDA_GRAPH_OPT`, applied
**only when CUDA is explicitly requested**. The default is BASIC. This does
not enable CUDA Graph capture (a separate feature); it selects ONNX's graph
rewrite/fusion optimization level. Reports/profiles are tagged with the
nondefault optimization level so no baseline files are overwritten.
Individual modes can fail or crash in the experimental runner; keep the
normal DirectML/production INT8 configuration unchanged.


### FP16 CUDA graph-optimization experiment results (three runs per mode)

Measured locally on an RTX 2070 Super, CUDA 13.5, cuDNN 9.28, native
ONNX Runtime 1.32.0; each run includes 12 single-frame and 6 batch-8
unprofiled timing samples. Values below are **median of the three run
medians**; they are not whole-application scan benchmarks.

| CUDA optimization | Single-frame (ms) | Batch-8 (ms) | GPU node events | CPU node events |
|---|---:|---:|---:|---:|
| BASIC | 10.54 | 17.29 | 634 | 11 |
| EXTENDED | 5.94 | 13.97 | 374 | 11 |
| ALL | 5.68 | 12.52 | 374 | 11 |

EXTENDED and ALL have *identical* execution-provider assignments, operator
type/count histograms, all embedding coordinates, and all reported cosine
scores across the six deterministic images. Both graph variants fuse
25 LayerNormalization, 12 Gelu, and 12 FusedMatMul CUDA operators,
eliminating many unfused arithmetic operators; GPU node event count
drops 41%. Compared with BASIC, EXTENDED achieves ~1.24x faster
batch-8 medians and ALL ~1.38x. The observed ALL/EXTENDED timing gap
is not attributable to different graph operator structures; more
randomized-order measurements are needed to separate clock/thermal noise.

Compared with BASIC, EXTENDED/ALL have maximum **0.2914 percentage-point**
change in the quantized pair similarity among all 15 synthetic pairs,
and maximum **0.1116 points** for the high-similarity pairs (>=89%).
There are no 89% or 94% classification flips in these samples, but there
are no pairs between 89% and 94%. Real-world accuracy/threshold testing
remains mandatory. BASIC timing also varied from an earlier run
(7.81ms single to ~10.54ms), so avoid treating one run as definitive.

**Production choice:** leave `VDF_AI_CUDA_GRAPH_OPT=basic` as default for
now. Changing CUDA optimization levels does change embeddings slightly,
even with identical model SHA256 and preprocessing; the current cache
partitioning only covers model SHA/preprocessing revision and not
optimizer mode. A production optimizer change requires either adding the
execution-profile/optimizer identity to the cache namespace, or a
controlled cache regeneration/revalidation. Preserve existing INT8
sidecars and do not compare embeddings across different models.
