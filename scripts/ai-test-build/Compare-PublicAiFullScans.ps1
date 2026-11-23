# Run actual VDF scan-and-compare on SHA-verified FFmpeg-generated clips only.
# Emits shareable clip IDs and group decisions without full paths.
[CmdletBinding()]
param(
    [string]$CorpusDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.PublicAiCorpus'),
    [string]$OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.PublicAiFullScan'),
    [ValidateSet('int8-directml','fp16-cuda')]
    [string[]]$Modes = @('int8-directml','fp16-cuda')
)
$ErrorActionPreference = 'Stop'
$CorpusDirectory = [IO.Path]::GetFullPath($CorpusDirectory)
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
$cli = Join-Path $PSScriptRoot 'CLI\vdf-cli.exe'
if (-not (Test-Path -LiteralPath $cli -PathType Leaf)) {
    throw "CLI executable not found: $cli. Extract the whole test-build ZIP first."
}
if (-not (Get-Command ffmpeg -ErrorAction SilentlyContinue)) {
    throw 'ffmpeg.exe is required on PATH.'
}
$clips = Join-Path $CorpusDirectory 'clips'
$manifestPath = Join-Path $CorpusDirectory 'manifest.json'
if (-not (Test-Path -LiteralPath $manifestPath)) { throw "Missing generated corpus manifest: $manifestPath" }
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ($manifest.schemaVersion -ne 1 -or $manifest.source -ne 'FFmpeg lavfi testsrc2, mandelbrot, testsrc') {
    throw 'Unexpected corpus manifest: refusing to scan arbitrary video files.'
}
$expected = @{}
foreach ($clip in $manifest.clips) {
    $relative = [string]$clip.file
    if ($relative -notmatch '^clips/([a-z0-9-]+\.mp4)$') { throw "Invalid clip path in manifest: $relative" }
    $basename = $Matches[1]
    $id = [IO.Path]::GetFileNameWithoutExtension($basename)
    if ($expected.ContainsKey($id)) { throw "Duplicate clip ID: $id" }
    $clipPath = Join-Path $clips $basename
    if (-not (Test-Path -LiteralPath $clipPath -PathType Leaf)) { throw "Missing generated clip: $id" }
    $actualHash = (Get-FileHash -LiteralPath $clipPath -Algorithm SHA256).Hash
    if ($actualHash -ine [string]$clip.sha256) { throw "SHA-256 mismatch on clip: $id" }
    $expected[$id] = $true
}
$actualClips = @(Get-ChildItem -LiteralPath $clips -Filter '*.mp4' -File)
if ($expected.Count -ne 21 -or $actualClips.Count -ne $expected.Count) {
    throw 'Expected exactly 21 generated MP4 clips and no extras.'
}
foreach ($item in $actualClips) {
    if (-not $expected.ContainsKey($item.BaseName)) { throw "Unlisted video found: $($item.Name)" }
}
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$executionId = [Guid]::NewGuid().ToString('N')
$allGroupKeys = @{}
foreach ($mode in $Modes) {
    # Scratch settings, AI runtimes, model files, SQLite DB and sidecars are all
    # different from the installed VDF and from the other experimental mode.
    $work = Join-Path $OutputDirectory ("private-" + $mode + "-" + $executionId)
    $state = Join-Path $work 'state'
    $db = Join-Path $work 'database'
    New-Item -ItemType Directory -Path $state,$db -Force | Out-Null
    $raw = Join-Path $work 'raw-results.json'
    $env:VDF_DATA_DIR = $state
    $env:VDF_AI_MODEL = if ($mode -eq 'fp16-cuda') { 'fp16' } else { 'int8' }
    $env:VDF_AI_PROVIDER = if ($mode -eq 'fp16-cuda') { 'cuda' } else { 'directml' }
    $env:VDF_AI_DEVICE = 'gpu'
    $env:VDF_AI_CUDA_GRAPH_OPT = if ($mode -eq 'fp16-cuda') { 'extended' } else { 'basic' }
    $argsForCli = @('scan-and-compare','--include',$clips,'--db',$db,
        '--ai-matching','--ai-partial','--ai-percent','94','--ai-partial-hit-percent','89',
        '--format','json','--output',$raw)
    $started = [DateTimeOffset]::UtcNow
    Write-Host ("Running full VDF scan: {0}; {1} generated clips" -f $mode,$expected.Count) -ForegroundColor Cyan
    & $cli @argsForCli
    if ($LASTEXITCODE -ne 0) { throw "VDF CLI exited with $LASTEXITCODE during $mode" }
    if (-not (Test-Path -LiteralPath $raw)) { throw "Missing scan result JSON: $mode" }
    $sourceGroups = @(Get-Content -LiteralPath $raw -Raw | ConvertFrom-Json)
    $groups = @(
        foreach ($group in $sourceGroups) {
            $members = @(
                foreach ($entry in $group.Items) {
                    $fullPath = [IO.Path]::GetFullPath([string]$entry.Path)
                    $prefix = $clips.TrimEnd('\','/') + [IO.Path]::DirectorySeparatorChar
                    if (-not $fullPath.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
                        throw 'Scan result references a file outside the generated corpus; no report exported.'
                    }
                    $id = [IO.Path]::GetFileNameWithoutExtension($fullPath)
                    if (-not $expected.ContainsKey($id)) { throw "Unexpected result clip ID: $id" }
                    [pscustomobject]@{
                        clipId = $id
                        similarityPercent = [math]::Round([double]$entry.Similarity, 5)
                        flags = [string]$entry.Flags
                        partialClipOffset = [string]$entry.PartialClipOffset
                    }
                }
            )
            $ids = @($members | ForEach-Object clipId | Sort-Object -Unique)
            [pscustomobject]@{
                groupKey = $ids -join '|'
                members = @($members | Sort-Object clipId)
            }
        }
    )
    $groups = @($groups | Sort-Object groupKey)
    $report = [ordered]@{
        schemaVersion = 1
        corpusType = 'FFmpeg lavfi procedural videos'
        clipCount = $expected.Count
        mode = $mode
        model = $env:VDF_AI_MODEL
        provider = $env:VDF_AI_PROVIDER
        optimization = $env:VDF_AI_CUDA_GRAPH_OPT
        elapsedSeconds = [math]::Round(([DateTimeOffset]::UtcNow - $started).TotalSeconds,3)
        groupCount = $groups.Count
        groups = $groups
    }
    $sharePath = Join-Path $OutputDirectory ("shareable-" + $mode + ".json")
    $report | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath $sharePath -Encoding utf8
    $allGroupKeys[$mode] = @($groups | ForEach-Object groupKey)
    Remove-Item -LiteralPath $raw -Force
    Write-Host ("{0}: {1} group(s); {2}" -f $mode,$groups.Count,$sharePath) -ForegroundColor Green
}
if ($allGroupKeys.ContainsKey('int8-directml') -and $allGroupKeys.ContainsKey('fp16-cuda')) {
    $baseline = @($allGroupKeys['int8-directml'])
    $candidate = @($allGroupKeys['fp16-cuda'])
    $deltas = [ordered]@{
        schemaVersion = 1
        baseline = 'int8-directml'
        candidate = 'fp16-cuda'
        commonGroups = @($baseline | Where-Object { $candidate -contains $_ })
        baselineOnly = @($baseline | Where-Object { $candidate -notcontains $_ })
        candidateOnly = @($candidate | Where-Object { $baseline -notcontains $_ })
    }
    $diffPath = Join-Path $OutputDirectory 'shareable-group-differences.json'
    $deltas | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $diffPath -Encoding utf8
    Write-Host "Group comparison: $diffPath" -ForegroundColor Green
}
Write-Host 'Share only shareable-*.json and manifest.json, not private-* folders.'
