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
