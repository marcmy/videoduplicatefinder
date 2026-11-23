# Reproducible public-only VDF AI matching corpus. No user videos are read.
# Generates short FFmpeg filtergraph clips, transformed variants, and 224x224 RGB frames.
# Usage: pwsh -NoProfile -File .\scripts\New-ShareableAiCorpus.ps1
[CmdletBinding()]
param(
    [string]$OutputDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.PublicAiCorpus'),
    [ValidateRange(6, 60)][int]$DurationSeconds = 12,
    [switch]$Force
)
$ErrorActionPreference = 'Stop'
$ffmpeg = (Get-Command ffmpeg -ErrorAction Stop).Source
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
$manifestPath = Join-Path $OutputDirectory 'manifest.json'
if ((Test-Path $manifestPath) -and -not $Force) {
    throw "Corpus already exists: $manifestPath. Use -Force to regenerate it."
}
$clipsDir = Join-Path $OutputDirectory 'clips'
$framesDir = Join-Path $OutputDirectory 'frames'
New-Item -ItemType Directory -Path $clipsDir,$framesDir -Force | Out-Null

function Run-Ffmpeg {
    param([string[]]$Arguments)
    & $ffmpeg -hide_banner -loglevel error -nostdin -y @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "FFmpeg exited with code $LASTEXITCODE."
    }
}
function Hash-File {
    param([string]$Path)
    (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

$specs = @(
    @{ id='a'; graph='testsrc2=size=640x360:rate=24' },
    @{ id='b'; graph='mandelbrot=size=640x360:rate=24' },
    @{ id='c'; graph='testsrc=size=640x360:rate=24' }
)
# Positives are variants from the same source at the same sample time.
# The hard negatives include other sources and nonaligned positions.
$transformations = @(
    @{ variant='original'; filter=$null; crf=18 },
    @{ variant='compression'; filter='format=yuv420p'; crf=42 },
    @{ variant='resize'; filter='scale=320:180:flags=bicubic,scale=640:360:flags=bicubic'; crf=24 },
    @{ variant='crop08'; filter='crop=588:330:26:15,scale=640:360:flags=bicubic'; crf=24 },
    @{ variant='crop16'; filter='crop=536:300:52:30,scale=640:360:flags=bicubic'; crf=24 },
    @{ variant='brightness'; filter='eq=brightness=0.12:contrast=1.1:saturation=0.92'; crf=24 }
)
$clips = [System.Collections.Generic.List[object]]::new()
$frames = [System.Collections.Generic.List[object]]::new()
$seconds = @(2, [math]::Min(8, $DurationSeconds - 2))

foreach ($spec in $specs) {
    $original = Join-Path $clipsDir "$($spec.id)-original.mp4"
    Run-Ffmpeg -Arguments @('-f','lavfi','-i',$spec.graph,'-t',"$DurationSeconds",
        '-an','-pix_fmt','yuv420p','-c:v','libx264','-preset','veryfast',
        '-crf','18','-threads','2','-movflags','+faststart',$original)

    foreach ($t in $transformations) {
        $clipName = "$($spec.id)-$($t.variant).mp4"
        $clipPath = Join-Path $clipsDir $clipName
        if ($t.variant -ne 'original') {
            $ffArgs = @('-i',$original,'-an')
            if ($null -ne $t.filter) { $ffArgs += @('-vf', $t.filter) }
            $ffArgs += @('-c:v','libx264','-preset','veryfast','-crf',"$($t.crf)",
                '-pix_fmt','yuv420p','-threads','2','-movflags','+faststart',$clipPath)
            Run-Ffmpeg -Arguments $ffArgs
        }
        $clips.Add([pscustomobject]@{
            id = [IO.Path]::GetFileNameWithoutExtension($clipName)
            file = "clips/$clipName"
            source = $spec.id
            variant = $t.variant
            sha256 = Hash-File $clipPath
        })
        foreach ($second in $seconds) {
            $frameName = "$($spec.id)-$($t.variant)-t$second.rgb"
            $framePath = Join-Path $framesDir $frameName
            Run-Ffmpeg -Arguments @('-ss',"$second",'-i',$clipPath,
                '-frames:v','1','-vf','scale=224:224:flags=bicubic,format=rgb24',
                '-f','rawvideo','-pix_fmt','rgb24',$framePath)
            if ((Get-Item -LiteralPath $framePath).Length -ne 224*224*3) {
                throw "Unexpected RGB frame length: $frameName"
            }
            $frames.Add([pscustomobject]@{
                id = [IO.Path]::GetFileNameWithoutExtension($frameName)
                file = "frames/$frameName"
                source = $spec.id
                variant = $t.variant
                sampleSecond = $second
                sha256 = Hash-File $framePath
            })
        }
    }
    # Additional intentionally partial copy of each source. The full-video
    # matching pipeline should distinguish a temporal subset from unrelated footage.
    $partialName = "$($spec.id)-partial.mp4"
    $partialPath = Join-Path $clipsDir $partialName
    Run-Ffmpeg -Arguments @('-ss','4','-i',$original,'-t','5','-an',
        '-c:v','libx264','-preset','veryfast','-crf','27',
        '-pix_fmt','yuv420p','-threads','2',$partialPath)
    $clips.Add([pscustomobject]@{
        id = [IO.Path]::GetFileNameWithoutExtension($partialName)
        file = "clips/$partialName"
        source = $spec.id
        variant = 'partial'
        sha256 = Hash-File $partialPath
    })
}
$version = (& $ffmpeg -version | Select-Object -First 1)
$manifest = [ordered]@{
    schemaVersion = 1
    license = 'Benchmark clips and RGB frames procedurally generated by FFmpeg; no personal media'
    source = 'FFmpeg lavfi testsrc2, mandelbrot, testsrc'
    ffmpegVersion = [string]$version
    sampleWidth = 224
    sampleHeight = 224
    rgbChannels = 3
    durationSeconds = $DurationSeconds
    clips = @($clips.ToArray())
    frames = @($frames.ToArray())
}
$manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $manifestPath -Encoding utf8
Write-Host "Generated $($clips.Count) clips and $($frames.Count) RGB frame samples." -ForegroundColor Green
Write-Host "Manifest: $manifestPath"
Write-Host 'Only share the manifest and benchmark JSON reports; video clips are optional.'
