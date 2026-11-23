# Real VDF AI partial-duplicate validation on hash-verified public cases.
# Runs every two-video case in isolation, with fresh databases and strict inference.
[CmdletBinding()]
param(
    [string]$CorpusDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.PublicAiPartialCorpus'),
    [string]$OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.PublicAiPartialResults'),
    [ValidateSet('int8-directml','fp16-cuda')]
    [string[]]$Modes = @('int8-directml','fp16-cuda')
)
$ErrorActionPreference = 'Stop'
$CorpusDirectory = [IO.Path]::GetFullPath($CorpusDirectory)
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
$cli = Join-Path $PSScriptRoot 'CLI\vdf-cli.exe'
if (-not (Test-Path -LiteralPath $cli -PathType Leaf)) {
    throw "CLI executable missing: $cli. Extract the full experimental build ZIP."
}
$null = Get-Command ffmpeg -ErrorAction Stop
$null = Get-Command ffprobe -ErrorAction Stop
$manifestPath = Join-Path $CorpusDirectory 'manifest.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw "Missing generated corpus manifest: $manifestPath"
}
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ($manifest.schemaVersion -ne 1 -or $manifest.corpusType -ne 'ffmpeg-partial-temporal-v1' -or
    $manifest.source -ne 'FFmpeg lavfi testsrc2, mandelbrot, testsrc' -or
    $manifest.sourceDurationSeconds -ne 90 -or $manifest.excerptStartSeconds -ne 20 -or
    $manifest.excerptDurationSeconds -ne 45) {
    throw 'Unexpected partial corpus manifest. No scans were started.'
}
$cases = @($manifest.cases)
if ($cases.Count -ne 12 -or @($cases | Where-Object { $_.expectedMatch }).Count -ne 9) {
    throw 'Expected exactly 9 matching source/excerpt pairs and 3 negative pairs.'
}
$caseRoot = Join-Path $CorpusDirectory 'cases'
$ids = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($case in $cases) {
    $id = [string]$case.id
    if ($id -cnotmatch '^(p-[abc]-(trim|crop|compression)|n-[abc]-[abc])$' -or -not $ids.Add($id)) {
        throw "Invalid or duplicate scenario ID: $id"
    }
    if ([bool]$case.expectedMatch -ne $id.StartsWith('p-', [StringComparison]::Ordinal)) {
        throw "Contradictory expectedMatch label: $id"
    }
    if (@($case.videos).Count -ne 2) {
        throw "Exactly two videos required in case: $id"
    }
    $caseDir = Join-Path $caseRoot $id
    foreach ($video in @($case.videos)) {
        $name = [string]$video.id
        if ($name -notin @('source','excerpt') -or
            [string]$video.file -cne "cases/$id/$name.mp4") {
            throw "Unexpected video entry in case: $id"
        }
        $file = Join-Path $caseDir "$name.mp4"
        if (-not (Test-Path -LiteralPath $file -PathType Leaf)) {
            throw "Missing generated video: $id/$name"
        }
        $sha = (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash
        if ($sha -ine [string]$video.sha256) {
            throw "Generated video SHA-256 mismatch: $id/$name"
        }
    }
    $actual = @(Get-ChildItem -LiteralPath $caseDir -Filter '*.mp4' -File)
    if ($actual.Count -ne 2) {
        throw "Unexpected extra media files: $id"
    }
}
if (@(Get-ChildItem -LiteralPath $caseRoot -File -Filter '*.mp4' -Recurse).Count -ne 24) {
    throw 'Unexpected number of MP4 files in generated corpus.'
}
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
# Never permit previous, possibly invalid CUDA results to survive a new run.
Get-ChildItem -LiteralPath $OutputDirectory -Filter 'shareable-partial-*.json' -File |
    Remove-Item -Force
$executionId = [Guid]::NewGuid().ToString('N')
$private = Join-Path $OutputDirectory "private-$executionId"
$modeReports = @{}

foreach ($mode in $Modes) {
    $state = Join-Path $private "$mode-state"
    New-Item -ItemType Directory -Path $state -Force | Out-Null
    $env:VDF_DATA_DIR = $state
    $env:VDF_AI_MODEL = if ($mode -eq 'fp16-cuda') { 'fp16' } else { 'int8' }
    $env:VDF_AI_PROVIDER = if ($mode -eq 'fp16-cuda') { 'cuda' } else { 'directml' }
    $env:VDF_AI_DEVICE = 'gpu'
    $env:VDF_AI_CUDA_GRAPH_OPT = if ($mode -eq 'fp16-cuda') { 'extended' } else { 'basic' }
    $env:VDF_AI_STRICT_INFERENCE = '1'
    if ($mode -eq 'fp16-cuda') {
        . (Join-Path $PSScriptRoot 'Initialize-VdfCudaDependencies.ps1')
        Initialize-VdfCudaDependencies
    }
    $outcomes = [System.Collections.Generic.List[object]]::new()
    foreach ($case in $cases) {
        $id = [string]$case.id
        $caseDir = Join-Path $caseRoot $id
        $privateCase = Join-Path $private "$mode-$id"
        $db = Join-Path $privateCase 'db'
        $raw = Join-Path $privateCase 'raw.json'
        New-Item -ItemType Directory -Path $db -Force | Out-Null
        $cliArgs = @('scan-and-compare','--include',$caseDir,'--db',$db,
            '--ai-partial','--ai-partial-hit-percent','89',
            '--format','json','--output',$raw)
        Write-Host ("[{0}] {1} (expected partial match: {2})" -f $mode,$id,[bool]$case.expectedMatch) -ForegroundColor Cyan
        $started = [DateTimeOffset]::UtcNow
        & $cli @cliArgs
        if ($LASTEXITCODE -ne 0) { throw "VDF $mode failed on $id with exit code $LASTEXITCODE." }
        if (-not (Test-Path -LiteralPath $raw -PathType Leaf)) {
            throw "Missing CLI result for $mode/$id"
        }
        $sourceGroups = @(Get-Content -LiteralPath $raw -Raw | ConvertFrom-Json)
        $groups = @(
            foreach ($group in $sourceGroups) {
                $members = @(
                    foreach ($item in $group.Items) {
                        $full = [IO.Path]::GetFullPath([string]$item.Path)
                        $prefix = [IO.Path]::GetFullPath($caseDir).TrimEnd('\','/') + [IO.Path]::DirectorySeparatorChar
                        if (-not $full.StartsWith($prefix,[StringComparison]::OrdinalIgnoreCase)) {
                            throw "Non-public path detected in scan result for $id; no report will be written."
                        }
                        $name = [IO.Path]::GetFileName($full)
                        if ($name -notin @('source.mp4','excerpt.mp4')) {
                            throw "Unknown media name in generated scan result for $id"
                        }
                        [pscustomobject]@{
                            clipId = [IO.Path]::GetFileNameWithoutExtension($name)
                            similarityPercent = [math]::Round([double]$item.Similarity,5)
                            flags = [string]$item.Flags
                            partialOffset = [string]$item.PartialClipOffset
                        }
                    }
                )
                [pscustomobject]@{
                    memberIds = @($members | ForEach-Object clipId | Sort-Object -Unique)
                    members = @($members | Sort-Object clipId)
                }
            }
        )
        $pairGroups = @($groups | Where-Object {
            $_.memberIds.Count -eq 2 -and
            $_.memberIds -contains 'source' -and
            $_.memberIds -contains 'excerpt'
        })
        $detected = $pairGroups.Count -gt 0
        $partial = $false
        if ($detected) {
            foreach ($group in $pairGroups) {
                if (@($group.members | Where-Object {
                    $_.flags -match 'PartialClip' -and $_.flags -match 'AiMatched'
                }).Count -gt 0) {
                    $partial = $true
                }
            }
        }
        $outcomes.Add([pscustomobject]@{
            id = $id
            expectedMatch = [bool]$case.expectedMatch
            expectedOffsetSeconds = $case.expectedOffsetSeconds
            detectedAnyGroup = $detected
            detectedAiPartial = $partial
            groupCount = $groups.Count
            groups = $groups
            elapsedSeconds = [math]::Round(([DateTimeOffset]::UtcNow - $started).TotalSeconds,3)
        })
        Remove-Item -LiteralPath $raw -Force
    }
    $allOutcomes = @($outcomes.ToArray())
    $modeReports[$mode] = [pscustomobject]@{
        schemaVersion = 1
        corpusType = 'ffmpeg-partial-temporal-v1'
        mode = $mode
        model = $env:VDF_AI_MODEL
        provider = $env:VDF_AI_PROVIDER
        optimization = $env:VDF_AI_CUDA_GRAPH_OPT
        caseCount = $allOutcomes.Count
        positiveAiPartialDetected = @($allOutcomes | Where-Object { $_.expectedMatch -and $_.detectedAiPartial }).Count
        positiveCount = 9
        negativeFalseAiPartial = @($allOutcomes | Where-Object { -not $_.expectedMatch -and $_.detectedAiPartial }).Count
        negativeCount = 3
        cases = $allOutcomes
    }
}
# Only after ALL requested modes have completed and passed strict inference
# do we write final files. An interrupted test cannot leave valid-looking JSON.
foreach ($mode in $modeReports.Keys) {
    $out = Join-Path $OutputDirectory "shareable-partial-$mode.json"
    $modeReports[$mode] | ConvertTo-Json -Depth 16 | Set-Content -LiteralPath $out -Encoding utf8
    Write-Host ("{0}: AI partial positives {1}/9, negative false positives {2}/3" -f
        $mode,$modeReports[$mode].positiveAiPartialDetected,
        $modeReports[$mode].negativeFalseAiPartial) -ForegroundColor Green
}
if ($modeReports.ContainsKey('int8-directml') -and $modeReports.ContainsKey('fp16-cuda')) {
    $base = $modeReports['int8-directml']
    $candidate = $modeReports['fp16-cuda']
    $deltas = @(
        foreach ($b in $base.cases) {
            $c = @($candidate.cases | Where-Object { $_.id -eq $b.id })[0]
            [pscustomobject]@{
                id = $b.id
                expectedMatch = $b.expectedMatch
                baselineDetected = $b.detectedAiPartial
                candidateDetected = $c.detectedAiPartial
                changed = [bool]($b.detectedAiPartial -ne $c.detectedAiPartial)
                baselineGroups = $b.groupCount
                candidateGroups = $c.groupCount
            }
        }
    )
    $comparison = [ordered]@{
        schemaVersion = 1
        corpusType = 'ffmpeg-partial-temporal-v1'
        changedAiPartialDecisions = @($deltas | Where-Object changed).Count
        cases = $deltas
    }
    $comparison | ConvertTo-Json -Depth 12 |
        Set-Content -LiteralPath (Join-Path $OutputDirectory 'shareable-partial-comparison.json') -Encoding utf8
    Write-Host "Changed AI-partial decisions: $($comparison.changedAiPartialDecisions) / 12" -ForegroundColor Green
}
Write-Host "Share only shareable-partial-*.json and the generated manifest.json; exclude private-*."
