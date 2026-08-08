param(
    [string]$BaselineRef = 'perf/4.1-baseline',
    [string]$Modes = 'process,native-cpu',
    [int]$Iterations = 9,
    [int]$WarmupIterations = 2,
    [double]$MaxRegressionPercent = 15,
    [string]$ArtifactsDir = 'artifacts/perf-gate'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (Get-Variable PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

if ($Iterations -le 0) { throw 'Iterations must be positive.' }
if ($WarmupIterations -lt 0) { throw 'WarmupIterations cannot be negative.' }
if ($MaxRegressionPercent -lt 0) { throw 'MaxRegressionPercent cannot be negative.' }

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$artifactRoot = Join-Path $repoRoot $ArtifactsDir
$baselineWorktree = Join-Path $env:RUNNER_TEMP 'vdf-perf-baseline-worktree'
$baselineJson = Join-Path $artifactRoot 'baseline.json'
$currentJson = Join-Path $artifactRoot 'current.json'
$baselineLog = Join-Path $artifactRoot 'baseline.log'
$currentLog = Join-Path $artifactRoot 'current.log'
$originalGithubSha = $env:GITHUB_SHA

New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null

function Invoke-RegressionProbe {
    param(
        [Parameter(Mandatory)] [string]$WorkingDirectory,
        [Parameter(Mandatory)] [string]$OutputPath,
        [Parameter(Mandatory)] [string]$LogPath,
        [string]$BaselinePath
    )

    $arguments = @(
        'run',
        '-c', 'Release',
        '--project', 'VDF.Benchmarks',
        '--',
        '--probe-regression',
        '--modes', $Modes,
        '--iterations', $Iterations.ToString([Globalization.CultureInfo]::InvariantCulture),
        '--warmup', $WarmupIterations.ToString([Globalization.CultureInfo]::InvariantCulture),
        '--output', $OutputPath
    )

    if ($BaselinePath) {
        $arguments += @(
            '--baseline', $BaselinePath,
            '--max-regression-percent', $MaxRegressionPercent.ToString([Globalization.CultureInfo]::InvariantCulture)
        )
    }

    Push-Location $WorkingDirectory
    try {
        & dotnet @arguments 2>&1 | Tee-Object -FilePath $LogPath | Out-Host
        return [int]$LASTEXITCODE
    }
    finally {
        Pop-Location
    }
}

try {
    if (Test-Path $baselineWorktree) {
        git -C $repoRoot worktree remove --force $baselineWorktree 2>$null
        if (Test-Path $baselineWorktree) { Remove-Item $baselineWorktree -Recurse -Force }
    }

    Write-Host "Fetching known-good performance baseline: $BaselineRef"
    git -C $repoRoot fetch --no-tags origin "+refs/heads/$BaselineRef`:refs/remotes/origin/$BaselineRef"
    if ($LASTEXITCODE -ne 0) { throw "Failed to fetch baseline ref '$BaselineRef'." }

    git -C $repoRoot worktree add --detach $baselineWorktree "refs/remotes/origin/$BaselineRef"
    if ($LASTEXITCODE -ne 0) { throw "Failed to create baseline worktree for '$BaselineRef'." }

    $baselineSha = (git -C $baselineWorktree rev-parse HEAD).Trim()
    $currentSha = (git -C $repoRoot rev-parse HEAD).Trim()
    Write-Host "Baseline: $baselineSha"
    Write-Host "Current:  $currentSha"
    Write-Host "Modes: $Modes; iterations=$Iterations; warmup=$WarmupIterations; max regression=$MaxRegressionPercent%"

    # Both refs intentionally share VideoCorpus.CacheDir (%TEMP%\vdf_bench_corpus).
    # Clear it once so the baseline creates the corpus with this job's verified
    # FFmpeg build and the candidate reuses the exact same bytes.
    $corpusCache = Join-Path ([IO.Path]::GetTempPath()) 'vdf_bench_corpus'
    if (Test-Path $corpusCache) { Remove-Item $corpusCache -Recurse -Force }

    $env:GITHUB_SHA = $baselineSha
    $baselineExit = Invoke-RegressionProbe `
        -WorkingDirectory $baselineWorktree `
        -OutputPath $baselineJson `
        -LogPath $baselineLog
    if ($baselineExit -ne 0) {
        throw "Known-good baseline probe failed with exit code $baselineExit."
    }

    $env:GITHUB_SHA = if ($originalGithubSha) { $originalGithubSha } else { $currentSha }
    $currentExit = Invoke-RegressionProbe `
        -WorkingDirectory $repoRoot `
        -OutputPath $currentJson `
        -LogPath $currentLog `
        -BaselinePath $baselineJson

    if ($currentExit -eq 2) {
        Write-Error "Performance regression exceeded the allowed $MaxRegressionPercent% median-throughput slowdown."
        exit 2
    }
    if ($currentExit -ne 0) {
        throw "Current performance probe failed with exit code $currentExit."
    }

    Write-Host 'Performance regression gate passed.'
}
finally {
    $env:GITHUB_SHA = $originalGithubSha
    if (Test-Path $baselineWorktree) {
        git -C $repoRoot worktree remove --force $baselineWorktree 2>$null
        if (Test-Path $baselineWorktree) { Remove-Item $baselineWorktree -Recurse -Force }
    }
}
