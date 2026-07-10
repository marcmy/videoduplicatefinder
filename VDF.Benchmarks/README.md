# VDF performance regression probe

The benchmark project contains BenchmarkDotNet scenarios and direct probes. For repeatable end-to-end gray-frame extraction measurements, use `--probe-regression`.

```powershell
dotnet run -c Release --project VDF.Benchmarks -- `
  --probe-regression `
  --modes process,native-cpu,d3d11 `
  --iterations 7 `
  --output artifacts\vdf-perf-current.json
```

Without `--corpus`, the probe uses the cached synthetic H.264, HEVC 10-bit, and VP9 corpus. To test representative real files:

```powershell
dotnet run -c Release --project VDF.Benchmarks -- `
  --probe-regression `
  --corpus D:\Media\VDF-Perf-Corpus `
  --positions 1,5,15 `
  --output artifacts\vdf-perf-current.json
```

Establish a baseline by keeping a known-good JSON report. A later run exits with code 2 when any matched case loses more throughput than the allowed percentage:

```powershell
dotnet run -c Release --project VDF.Benchmarks -- `
  --probe-regression `
  --baseline artifacts\vdf-perf-baseline.json `
  --max-regression-percent 12 `
  --output artifacts\vdf-perf-current.json
```

Compare reports only on the same machine, power plan, FFmpeg build, corpus, and background-load conditions. GPU cases are meaningful only on the same adapter and driver. Detailed successful-extraction telemetry stays disabled during the probe so logging overhead does not contaminate the measurements.
