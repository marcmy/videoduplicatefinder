# FFmpeg-only temporal AI partial-duplicate test corpus. No personal videos.
# Produces 12 isolated two-video cases: nine positives, three hard negatives.
[CmdletBinding()]
param(
    [string]$OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.PublicAiPartialCorpus')
)
$ErrorActionPreference = 'Stop'
$ffmpeg = (Get-Command ffmpeg -ErrorAction Stop).Source
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
if ((Test-Path -LiteralPath $OutputDirectory) -and
    @(Get-ChildItem -LiteralPath $OutputDirectory -Force | Select-Object -First 1).Count -gt 0) {
    throw "Destination is not empty: $OutputDirectory. Choose another -OutputDirectory to avoid overwriting files."
}
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$assets = Join-Path $OutputDirectory 'generated-assets'
$casesRoot = Join-Path $OutputDirectory 'cases'
New-Item -ItemType Directory -Path $assets,$casesRoot -Force | Out-Null

function Invoke-Generate {
    param([string[]]$Arguments)
    & $ffmpeg -hide_banner -loglevel error -nostdin -y @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "FFmpeg generation failed with exit code $LASTEXITCODE."
    }
}
function File-Hash {
    param([string]$Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}
$sourceDuration = 90
$excerptStart = 20
$excerptDuration = 45
$graphs = [ordered]@{
    a = 'testsrc2=size=640x360:rate=24'
    b = 'mandelbrot=size=640x360:rate=24'
    c = 'testsrc=size=640x360:rate=24'
}
$sourcePaths = @{}
$excerptPaths = @{}
$variants = @(
    @{ name='trim'; crf=26; filter=$null },
    @{ name='compression'; crf=42; filter=$null },
    @{ name='crop'; crf=28; filter='crop=588:330:26:15,scale=640:360:flags=bicubic' }
)

foreach ($name in $graphs.Keys) {
    $sourcePath = Join-Path $assets "$name-source.mp4"
    Write-Host "Generating source $name ($sourceDuration seconds)"
    Invoke-Generate -Arguments @('-f','lavfi','-i',$graphs[$name],'-t',"$sourceDuration",
        '-an','-c:v','libx264','-preset','veryfast','-crf','18',
        '-pix_fmt','yuv420p','-threads','2',$sourcePath)
    $sourcePaths[$name] = $sourcePath
    foreach ($variant in $variants) {
        $key = "$name-$($variant.name)"
        $excerptPath = Join-Path $assets "$key-excerpt.mp4"
        $ffArgs = @('-ss',"$excerptStart",'-i',$sourcePath,'-t',"$excerptDuration",'-an')
        if ($null -ne $variant.filter) { $ffArgs += @('-vf',$variant.filter) }
        $ffArgs += @('-c:v','libx264','-preset','veryfast','-crf',"$($variant.crf)",
            '-pix_fmt','yuv420p','-threads','2',$excerptPath)
        Invoke-Generate -Arguments $ffArgs
        $excerptPaths[$key] = $excerptPath
    }
}

$manifestCases = [System.Collections.Generic.List[object]]::new()
function Add-Case {
    param(
        [string]$Id,
        [string]$SourceName,
        [string]$ExcerptName,
        [string]$Variant,
        [bool]$ExpectedMatch
    )
    $directory = Join-Path $casesRoot $Id
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
    $source = Join-Path $directory 'source.mp4'
    $excerpt = Join-Path $directory 'excerpt.mp4'
    Copy-Item -LiteralPath $sourcePaths[$SourceName] -Destination $source
    Copy-Item -LiteralPath $excerptPaths["$ExcerptName-$Variant"] -Destination $excerpt

    $manifestCases.Add([pscustomobject]@{
        id = $Id
        source = $SourceName
        excerptSource = $ExcerptName
        excerptVariant = $Variant
        expectedMatch = $ExpectedMatch
        expectedOffsetSeconds = if ($ExpectedMatch) { $excerptStart } else { $null }
        videos = @(
            [pscustomobject]@{ id='source'; file="cases/$Id/source.mp4"; sha256=(File-Hash $source) },
            [pscustomobject]@{ id='excerpt'; file="cases/$Id/excerpt.mp4"; sha256=(File-Hash $excerpt) }
        )
    })
}
foreach ($name in $graphs.Keys) {
    foreach ($variant in $variants) {
        Add-Case -Id "p-$name-$($variant.name)" -SourceName $name -ExcerptName $name -Variant $variant.name -ExpectedMatch $true
    }
}
$negativePairs = @(
    @{ source='a'; excerpt='b' },
    @{ source='b'; excerpt='c' },
    @{ source='c'; excerpt='a' }
)
foreach ($pair in $negativePairs) {
    Add-Case -Id "n-$($pair.source)-$($pair.excerpt)" -SourceName $pair.source -ExcerptName $pair.excerpt -Variant 'trim' -ExpectedMatch $false
}

$manifest = [ordered]@{
    schemaVersion = 1
    corpusType = 'ffmpeg-partial-temporal-v1'
    source = 'FFmpeg lavfi testsrc2, mandelbrot, testsrc'
    ffmpegVersion = [string](& $ffmpeg -version | Select-Object -First 1)
    sourceDurationSeconds = $sourceDuration
    excerptStartSeconds = $excerptStart
    excerptDurationSeconds = $excerptDuration
    explanation = 'Generated-only isolated source/excerpt pairs; no personal media'
    cases = @($manifestCases.ToArray())
}
$manifestPath = Join-Path $OutputDirectory 'manifest.json'
$manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $manifestPath -Encoding utf8
Write-Host "Generated $($manifestCases.Count) cases: 9 positives, 3 negatives." -ForegroundColor Green
Write-Host "Manifest: $manifestPath"
