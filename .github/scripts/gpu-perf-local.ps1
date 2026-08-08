[CmdletBinding()]
param(
    [string]$BaselineRef = 'perf/4.1-baseline',
    [string]$Corpus,
    [int]$Iterations = 9,
    [int]$WarmupIterations = 2,
    [double]$MaxRegressionPercent = 15,
    [string]$OutputRoot = 'artifacts/gpu-perf',
    [switch]$UsePHash,
    [switch]$EnablePartialClipDetection
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (Get-Variable PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
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
    corpus = if ($Corpus) { (Resolve-Path $Corpus).Path } else { $null }
    processors = $processors
    videoControllers = $videoControllers
    operatingSystem = $os
    activePowerScheme = Invoke-OptionalText { powercfg /GETACTIVESCHEME }
    nvidiaSmi = if (Get-Command nvidia-smi -ErrorAction SilentlyContinue) {
        Invoke-OptionalText { nvidia-smi --query-gpu=name,driver_version,pstate,temperature.gpu,power.limit,clocks.gr,clocks.mem --format=csv,noheader }
    } else { 'unavailable' }
    dotnet = Invoke-OptionalText { dotnet --info }
}
$metadata | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $runDir 'environment.json') -Encoding utf8

Write-Host "GPU benchmark environment captured in $runDir"
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

Write-Host 'Running full ScanEngine pipeline in D3D11 mode...'
$scanReport = Join-Path $runDir 'scan-pipeline-d3d11.json'
$scanArgs = @(
    'run', '-c', 'Release', '--project', 'VDF.Benchmarks', '--',
    '--probe-scan-pipeline',
    '--mode', 'd3d11',
    '--output', $scanReport
)
if ($Corpus) { $scanArgs += @('--corpus', (Resolve-Path $Corpus).Path) }
if ($UsePHash) { $scanArgs += '--phash' }
if ($EnablePartialClipDetection) { $scanArgs += '--partial' }

Push-Location $repoRoot
try {
    & dotnet @scanArgs 2>&1 | Tee-Object -FilePath (Join-Path $runDir 'scan-pipeline.log')
    if ($LASTEXITCODE -ne 0) {
        throw "D3D11 scan-pipeline probe failed with exit code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

Write-Host ''
Write-Host "GPU performance run complete: $runDir"
Write-Host 'Keep environment.json with the reports; compare GPU results only when adapter, driver, and power conditions are equivalent.'
