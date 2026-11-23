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
