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
$originalGithubSha = $env:GITHUB_SHA

New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null

function Invoke-Git {
    param(
        [Parameter(Mandatory)] [string]$WorkingDirectory,
        [Parameter(Mandatory)] [string[]]$Arguments,
        [Parameter(Mandatory)] [string]$FailureMessage
    )

    & git -C $WorkingDirectory @Arguments
    if ($LASTEXITCODE -ne 0) { throw $FailureMessage }
}

function Build-BenchmarkHarness {
    param([Parameter(Mandatory)] [string]$WorkingDirectory)

    Push-Location $WorkingDirectory
    try {
        & dotnet build VDF.Benchmarks/VDF.Benchmarks.csproj -c Release -v q --nologo
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to build benchmark harness in $WorkingDirectory."
        }
    }
    finally {
        Pop-Location
    }
}

function Get-BenchmarkDll {
    param([Parameter(Mandatory)] [string]$WorkingDirectory)

    $dll = Join-Path $WorkingDirectory 'VDF.Benchmarks/bin/Release/net10.0/VDF.Benchmarks.dll'
    if (!(Test-Path -LiteralPath $dll -PathType Leaf)) {
        throw "Benchmark harness DLL was not produced: $dll"
    }
    return $dll
}

function Invoke-RegressionProbe {
    param(
        [Parameter(Mandatory)] [string]$WorkingDirectory,
        [Parameter(Mandatory)] [string]$OutputPath,
        [Parameter(Mandatory)] [string]$LogPath,
        [string]$BaselinePath
    )

    $benchmarkDll = Get-BenchmarkDll -WorkingDirectory $WorkingDirectory
    $arguments = @(
        $benchmarkDll,
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

function Invoke-PairedMeasurement {
    param(
        [AllowEmptyString()] [string]$Suffix = '',
        [Parameter(Mandatory)] [string]$BaselineSha,
        [Parameter(Mandatory)] [string]$CurrentSha
    )

    $baselineJson = Join-Path $artifactRoot "baseline$Suffix.json"
    $currentJson = Join-Path $artifactRoot "current$Suffix.json"
    $baselineLog = Join-Path $artifactRoot "baseline$Suffix.log"
    $currentLog = Join-Path $artifactRoot "current$Suffix.log"

    $env:GITHUB_SHA = $BaselineSha
    $baselineExit = Invoke-RegressionProbe `
        -WorkingDirectory $baselineWorktree `
        -OutputPath $baselineJson `
        -LogPath $baselineLog
    if ($baselineExit -ne 0) {
        throw "Known-good baseline probe$Suffix failed with exit code $baselineExit."
    }

    $env:GITHUB_SHA = if ($originalGithubSha) { $originalGithubSha } else { $CurrentSha }
    return Invoke-RegressionProbe `
        -WorkingDirectory $repoRoot `
        -OutputPath $currentJson `
        -LogPath $currentLog `
        -BaselinePath $baselineJson
}

try {
    if (Test-Path $baselineWorktree) {
        git -C $repoRoot worktree remove --force $baselineWorktree 2>$null
        if (Test-Path $baselineWorktree) { Remove-Item $baselineWorktree -Recurse -Force }
    }

    Write-Host "Fetching known-good performance baseline: $BaselineRef"
    Invoke-Git -WorkingDirectory $repoRoot `
        -Arguments @('fetch', '--no-tags', 'origin', "+refs/heads/$BaselineRef`:refs/remotes/origin/$BaselineRef") `
        -FailureMessage "Failed to fetch baseline ref '$BaselineRef'."
    Invoke-Git -WorkingDirectory $repoRoot `
        -Arguments @('worktree', 'add', '--detach', $baselineWorktree, "refs/remotes/origin/$BaselineRef") `
        -FailureMessage "Failed to create baseline worktree for '$BaselineRef'."

    $baselineSha = (git -C $baselineWorktree rev-parse HEAD).Trim()
    $currentSha = (git -C $repoRoot rev-parse HEAD).Trim()
    if ($originalGithubSha -and
        ![string]::Equals($currentSha, $originalGithubSha, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Performance gate checkout mismatch: current HEAD is $currentSha but GITHUB_SHA is $originalGithubSha. Refusing to benchmark or publish mismatched source."
    }
    Write-Host "Baseline product code: $baselineSha"
    Write-Host "Current product code:  $currentSha"
    Write-Host "Modes: $Modes; iterations=$Iterations; warmup=$WarmupIterations; max regression=$MaxRegressionPercent%"

    # Benchmark methodology must not drift between refs. Replace only the baseline
    # worktree's benchmark project with the candidate's current harness source;
    # VDF.Core and the rest of the baseline product tree remain pinned. Exclude
    # local build outputs so a developer's existing bin/obj cannot contaminate it.
    $currentHarness = Join-Path $repoRoot 'VDF.Benchmarks'
    $baselineHarness = Join-Path $baselineWorktree 'VDF.Benchmarks'
    if (Test-Path $baselineHarness) { Remove-Item $baselineHarness -Recurse -Force }
    New-Item -ItemType Directory -Path $baselineHarness -Force | Out-Null
    Get-ChildItem -LiteralPath $currentHarness -Force |
        Where-Object { $_.Name -notin @('bin', 'obj') } |
        Copy-Item -Destination $baselineHarness -Recurse -Force
    Write-Host 'Synchronized current benchmark harness source into the baseline worktree.'

    # Build both refs before any timing starts. This keeps restore/compiler/Defender
    # activity out of the immediate baseline-vs-candidate measurement sequence.
    Write-Host 'Building baseline benchmark harness...'
    Build-BenchmarkHarness -WorkingDirectory $baselineWorktree
    Write-Host 'Building current benchmark harness...'
    Build-BenchmarkHarness -WorkingDirectory $repoRoot

    # Both refs intentionally share VideoCorpus.CacheDir (%TEMP%\vdf_bench_corpus).
    # Clear it once so the first baseline pass creates the corpus with this job's
    # verified FFmpeg build and every later pass reuses the exact same bytes.
    $corpusCache = Join-Path ([IO.Path]::GetTempPath()) 'vdf_bench_corpus'
    if (Test-Path $corpusCache) { Remove-Item $corpusCache -Recurse -Force }

    Write-Host 'Running primary paired performance measurement...'
    $currentExit = Invoke-PairedMeasurement -BaselineSha $baselineSha -CurrentSha $currentSha

    if ($currentExit -eq 2) {
        Write-Warning 'Primary pair crossed the regression threshold; running one confirmation pair before blocking publication.'
        $confirmationExit = Invoke-PairedMeasurement `
            -Suffix '-confirm' `
            -BaselineSha $baselineSha `
            -CurrentSha $currentSha

        if ($confirmationExit -eq 2) {
            Write-Error "Performance regression exceeded the allowed $MaxRegressionPercent% median-throughput slowdown in both paired measurements."
            exit 2
        }
        if ($confirmationExit -ne 0) {
            throw "Confirmation performance probe failed with exit code $confirmationExit."
        }

        Write-Warning 'Primary threshold breach was not reproduced by the confirmation pair; treating it as runner noise.'
        Write-Host 'Performance regression gate passed after confirmation.'
        exit 0
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
