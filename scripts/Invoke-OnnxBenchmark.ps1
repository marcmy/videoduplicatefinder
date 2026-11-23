# Run each native ONNX backend in a separate process. Requires PowerShell 7 and .NET 11.
# Usage: pwsh -NoProfile -File .\scripts\Invoke-OnnxBenchmark.ps1
[CmdletBinding()]
param(
    [ValidateRange(1, 100)]
    [int]$Repeat = 1,
    [ValidateSet('int8','fp32','fp16')]
    [string]$Model = 'int8',
    [ValidateSet('directml','cuda','cpu')]
    [string[]]$Providers = @(),
    [string]$OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.OnnxBenchmark\reports')
)
$ErrorActionPreference = 'Stop'
$repo = Resolve-Path (Join-Path $PSScriptRoot '..')
$project = Join-Path $repo 'VDF.Core.Tests\VDF.Core.Tests.csproj'
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$OutputDirectory = (Resolve-Path $OutputDirectory).Path
# Float16 CPU kernels are not broadly supported by ONNX Runtime. Benchmark
# GPU providers by default; CPU can still be explicitly requested.
if ($Providers.Count -eq 0) {
    $Providers = if ($Model -eq 'fp16') { @('directml','cuda') } else { @('directml','cuda','cpu') }
}

$names = @('VDF_AI_PROVIDER','VDF_AI_CPU_RUNTIME','VDF_AI_DEVICE',
    'VDF_ONNX_BENCHMARK','VDF_ONNX_BENCH_OUTPUT','VDF_AI_PROFILE_PREFIX',
    'VDF_ONNX_BENCH_MODEL')
$previous = @{}
foreach ($name in $names) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }

try {
    $results = @()
    foreach ($run in 1..$Repeat) {
        foreach ($provider in $Providers) {
            if ($provider -ne 'cpu' -and -not $IsWindows) {
                if ($provider -eq 'directml') { continue }
            }
            $env:VDF_AI_PROVIDER = if ($provider -eq 'cpu') { 'auto' } else { $provider }
            $env:VDF_AI_CPU_RUNTIME = if ($provider -eq 'cpu') { '1' } else { '0' }
            $env:VDF_AI_DEVICE = if ($provider -eq 'cpu') { 'cpu' } else { 'gpu' }
            $env:VDF_ONNX_BENCHMARK = '1'
            $env:VDF_ONNX_BENCH_MODEL = $Model
            $env:VDF_ONNX_BENCH_OUTPUT = Join-Path $OutputDirectory ("{0}-{1}-run{2}.json" -f $Model,$provider,$run)
            Remove-Item Env:VDF_AI_PROFILE_PREFIX -ErrorAction SilentlyContinue

            Write-Host "Benchmarking $Model $provider run $run/$Repeat..." -ForegroundColor Cyan
            & dotnet test $project -c Release --filter 'FullyQualifiedName~DINOv2_RealModel_Benchmark_OptIn' --logger 'console;verbosity=normal'
            if ($LASTEXITCODE -ne 0) { throw "Benchmark failed for $provider ($LASTEXITCODE). No fallback accepted." }
            if (-not (Test-Path $env:VDF_ONNX_BENCH_OUTPUT)) { throw "Missing report: $env:VDF_ONNX_BENCH_OUTPUT" }
            $results += Get-Content $env:VDF_ONNX_BENCH_OUTPUT -Raw | ConvertFrom-Json
        }
    }

    Write-Host "`nMedian $Model inference timings (wall time includes VDF preprocessing)" -ForegroundColor Green
    $results | Select-Object provider, singleMedianMs, batch8MedianMs,
        @{Name='Batch8PerFrameMs';Expression={[math]::Round($_.batch8MedianMs / 8, 3)}},
        @{Name='CUDA_nodes';Expression={$_.profile.gpuNodes}},
        @{Name='CPU_nodes';Expression={$_.profile.cpuNodes}} |
        Format-Table -AutoSize

    $baseline = $results | Where-Object { $_.provider -eq 'directml' } | Select-Object -First 1
    if (-not $baseline) { $baseline = $results | Where-Object { $_.provider -eq 'cpu' } | Select-Object -First 1 }
    foreach ($item in $results) {
        if (-not $baseline -or $item -eq $baseline) { continue }
        $maxDrift = 0.0
        $flips94 = 0
        $flips89 = 0
        for ($i=0; $i -lt $item.pairs.Count; $i++) {
            $a = [double]$baseline.pairs[$i].quantizedSimilarity
            $b = [double]$item.pairs[$i].quantizedSimilarity
            $maxDrift = [math]::Max($maxDrift, [math]::Abs($a - $b))
            if (($a -ge 0.94) -ne ($b -ge 0.94)) { $flips94++ }
            if (($a -ge 0.89) -ne ($b -ge 0.89)) { $flips89++ }
        }
        Write-Host ("{0} vs {1}: max quantized pair drift={2:F6}, 94% flips={3}, 89% flips={4}" -f
            $item.provider,$baseline.provider,$maxDrift,$flips94,$flips89)
    }
    Write-Host "Profiles and JSON reports: $OutputDirectory"
    Write-Warning 'Synthetic image samples and provider-node counts do not replace end-to-end VDF scan benchmarks or GPU utilization profiling.'
}
finally {
    foreach ($name in $names) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
}
