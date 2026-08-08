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

Establish a baseline by keeping a known-good JSON report. A later run exits with code 2 when any matched case loses more median throughput than the allowed percentage:

```powershell
dotnet run -c Release --project VDF.Benchmarks -- `
  --probe-regression `
  --baseline artifacts\vdf-perf-baseline.json `
  --max-regression-percent 12 `
  --output artifacts\vdf-perf-current.json
```

The comparator rejects mismatched runtime environments, changed sample counts, missing baseline cases, partial measurement failures, and reports with no comparable cases. Mean throughput remains in the report for context, but the gate uses the p50 batch time converted to median samples/second so a single noisy iteration is less likely to create a false regression.

## Release performance gate

The `perf/4.1-native-hwaccel` release workflow runs `.github/scripts/perf-regression-gate.ps1` after installing the verified shared FFmpeg build and before the integration/AOT stages. The script checks out `perf/4.1-baseline` as a detached worktree while leaving its product code pinned.

To prevent benchmark-harness drift from looking like a product performance change, the script replaces only the baseline worktree's `VDF.Benchmarks` project with the candidate's current benchmark source before compiling either side. Both benchmark projects are fully built before timing begins. Baseline and candidate therefore measure different product revisions through the same harness code, on the same Windows runner, with the same verified FFmpeg binaries and the exact same generated corpus bytes.

The release gate currently measures `process` and `native-cpu` with 9 measured iterations, 2 warmups, and a 15% maximum median-throughput regression per case. If the primary baseline/candidate pair crosses that threshold, the script automatically runs one additional paired measurement. Publication is blocked only when the threshold breach reproduces in both pairs; a one-off breach is retained in the artifacts and treated as hosted-runner noise.

GitHub-hosted Windows runners do not expose usable D3D11VA hardware, so D3D11 remains available to the probe for local or GPU-runner measurements but is not part of the hosted release gate.

Every normal gate run writes `baseline.json`, `current.json`, and both probe logs under `artifacts/perf-gate`. A confirmation run additionally writes `baseline-confirm.json`, `current-confirm.json`, and their logs. The release workflow uploads the directory even when the gate fails.

`perf/4.1-baseline` is deliberately not advanced automatically. A failed candidate therefore cannot become the next baseline simply by being published. Move that branch only after an intentional performance change has been independently verified and accepted.

For an equivalent local comparison (including D3D11 on a capable Windows GPU), run:

```powershell
.github\scripts\perf-regression-gate.ps1 `
  -BaselineRef perf/4.1-baseline `
  -Modes process,native-cpu,d3d11 `
  -Iterations 9 `
  -WarmupIterations 2 `
  -MaxRegressionPercent 15
```

Compare ad-hoc reports only on the same machine, power plan, FFmpeg build, corpus, and background-load conditions. GPU cases are meaningful only on the same adapter and driver. Detailed successful-extraction telemetry stays disabled during the probe so logging overhead does not contaminate the measurements.
