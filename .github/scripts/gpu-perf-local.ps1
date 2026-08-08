[CmdletBinding()]
param(
    [string]$BaselineRef = 'perf/4.1-baseline',
    [string]$Corpus,
    [int]$Iterations = 9,
    [int]$WarmupIterations = 2,
    [double]$MaxRegressionPercent = 15,
    [string]$OutputRoot = 'artifacts/gpu-perf',
    [switch]$UsePHash,
    [switch]$EnablePartialClipDetection,
    [switch]$SkipExtractionGate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (Get-Variable PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$resolvedCorpus = if ($Corpus) { (Resolve-Path $Corpus).Path } else { $null }
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$relativeRunDir = Join-Path $OutputRoot $stamp
$runDir = Join-Path $repoRoot $relativeRunDir
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

function Invoke-OptionalText {
    param([Parameter(Mandatory)] [scriptblock]$Command)
    try { return (& $Command 2>&1 | Out-String).Trim() }
    catch { return "unavailable: $($_.Exception.Message)" }
}

$videoControllers = @()
try {
    $videoControllers = @(Get-CimInstance Win32_VideoController | Select-Object Name, DriverVersion, AdapterRAM, VideoProcessor, PNPDeviceID)
}
catch { }

$processors = @()
try {
    $processors = @(Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors, MaxClockSpeed)
}
catch { }

$os = $null
try {
    $os = Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version, BuildNumber
}
catch { }

$metadata = [ordered]@{
    capturedAt = (Get-Date).ToUniversalTime().ToString('o')
    gitHead = (git -C $repoRoot rev-parse HEAD).Trim()
    baselineRef = $BaselineRef
    corpus = $resolvedCorpus
    processors = $processors
    videoControllers = $videoControllers
    operatingSystem = $os
    activePowerScheme = Invoke-OptionalText { powercfg /GETACTIVESCHEME }
    nvidiaSmi = if (Get-Command nvidia-smi -ErrorAction SilentlyContinue) {
        Invoke-OptionalText { nvidia-smi --query-gpu=name,driver_version,memory.total,pstate,temperature.gpu,power.limit,clocks.gr,clocks.mem --format=csv,noheader }
    } else { 'unavailable' }
    dotnet = Invoke-OptionalText { dotnet --info }
}
$metadata | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $runDir 'environment.json') -Encoding utf8

Write-Host "GPU benchmark environment captured in $runDir"

if (!$SkipExtractionGate) {
    Write-Host 'Running same-machine baseline/candidate extraction gate with D3D11 enabled...'

    $gateArgs = @{
        BaselineRef = $BaselineRef
        Modes = 'process,native-cpu,d3d11'
        Iterations = $Iterations
        WarmupIterations = $WarmupIterations
        MaxRegressionPercent = $MaxRegressionPercent
        ArtifactsDir = (Join-Path $relativeRunDir 'extraction-gate')
    }
    & (Join-Path $PSScriptRoot 'perf-regression-gate.ps1') @gateArgs
    if ($LASTEXITCODE -ne 0) {
        throw "GPU extraction regression gate failed with exit code $LASTEXITCODE. Reports are under '$runDir'."
    }
}
else {
    Write-Host 'Skipping baseline/candidate extraction gate as requested.'
}

$pipelineModes = @('process', 'native-cpu', 'd3d11')
$pipelineSummaries = @()

foreach ($mode in $pipelineModes) {
    Write-Host "Running full ScanEngine pipeline in $mode mode..."
    $scanReport = Join-Path $runDir "scan-pipeline-$mode.json"
    $scanLog = Join-Path $runDir "scan-pipeline-$mode.log"
    $scanArgs = @(
        'run', '-c', 'Release', '--project', 'VDF.Benchmarks', '--',
        '--probe-scan-pipeline',
        '--mode', $mode,
        '--output', $scanReport
    )
    if ($resolvedCorpus) { $scanArgs += @('--corpus', $resolvedCorpus) }
    if ($UsePHash) { $scanArgs += '--phash' }
    if ($EnablePartialClipDetection) { $scanArgs += '--partial' }

    Push-Location $repoRoot
    try {
        & dotnet @scanArgs 2>&1 | Tee-Object -FilePath $scanLog
        if ($LASTEXITCODE -ne 0) {
            throw "$mode scan-pipeline probe failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        Pop-Location
    }

    $report = Get-Content -LiteralPath $scanReport -Raw | ConvertFrom-Json
    $pipelineSummaries += [pscustomobject]@{
        Mode = $mode
        TotalMs = [math]::Round([double]$report.TotalWallMs, 1)
        DiscoveryMs = [math]::Round([double]$report.FileDiscoveryWallMs, 1)
        AnalysisMs = [math]::Round([double]$report.AnalysisHashingWallMs, 1)
        CompareMs = [math]::Round([double]$report.CompareFinalizeWallMs, 1)
        CpuMs = [math]::Round([double]$report.ProcessCpuMs, 1)
        AllocMiB = [math]::Round([double]$report.AllocatedBytes / 1MB, 2)
        Files = [int]$report.InputFiles
    }
}

Write-Host ''
Write-Host 'Full ScanEngine pipeline comparison:'
$pipelineSummaries | Format-Table -AutoSize | Out-Host

Write-Host "GPU performance run complete: $runDir"
Write-Host 'Keep environment.json with the reports; compare GPU results only when adapter, driver, and power conditions are equivalent.'
