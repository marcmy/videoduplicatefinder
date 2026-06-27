[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string] $AutoGenRoot,

    [string] $FfmpegBuildRepository = 'marcmy/FFmpeg-Builds',

    [string] $WorkingDirectory = (Join-Path $env:RUNNER_TEMP 'vdf-ffmpeg-master')
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw 'GitHub CLI (gh) is required.'
}
if (-not (Test-Path -LiteralPath $AutoGenRoot -PathType Container)) {
    throw "FFmpeg.AutoGen checkout not found: $AutoGenRoot"
}

Remove-Item -LiteralPath $WorkingDirectory -Recurse -Force -ErrorAction SilentlyContinue
$downloadDirectory = Join-Path $WorkingDirectory 'download'
$extractDirectory = Join-Path $WorkingDirectory 'extracted'
New-Item -ItemType Directory -Force -Path $downloadDirectory, $extractDirectory | Out-Null

Write-Host "Finding latest Windows x64 shared FFmpeg release in $FfmpegBuildRepository..."
$releasesJson = gh api "repos/$FfmpegBuildRepository/releases?per_page=100"
if ($LASTEXITCODE -ne 0) {
    throw "Unable to query releases from $FfmpegBuildRepository."
}

$release = $releasesJson |
    ConvertFrom-Json |
    Where-Object {
        -not $_.draft -and
        $_.tag_name -match '^ffmpeg-[0-9]{8}\.[0-9]{6}-win64-marc-shared$'
    } |
    Select-Object -First 1

if ($null -eq $release) {
    throw "No matching win64-marc-shared release found in $FfmpegBuildRepository."
}

$asset = $release.assets |
    Where-Object { $_.name -match '^ffmpeg-[0-9]{8}\.[0-9]{6}-win64-marc-shared\.zip$' } |
    Select-Object -First 1

if ($null -eq $asset) {
    throw "Release $($release.tag_name) does not contain the expected shared ZIP asset."
}

Write-Host "Downloading $($release.tag_name) / $($asset.name)..."
gh release download $release.tag_name `
    --repo $FfmpegBuildRepository `
    --pattern $asset.name `
    --dir $downloadDirectory `
    --clobber
if ($LASTEXITCODE -ne 0) {
    throw "Failed to download $($asset.name)."
}

$archivePath = Join-Path $downloadDirectory $asset.name
Expand-Archive -LiteralPath $archivePath -DestinationPath $extractDirectory -Force

$avcodecHeader = Get-ChildItem -LiteralPath $extractDirectory -Recurse -File -Filter 'avcodec.h' |
    Where-Object { $_.Directory.Name -eq 'libavcodec' } |
    Select-Object -First 1
if ($null -eq $avcodecHeader) {
    throw 'The FFmpeg shared archive does not contain include/libavcodec/avcodec.h; bindings cannot be regenerated from this asset.'
}
$includeDirectory = Split-Path -Parent $avcodecHeader.Directory.FullName

$avcodecDll = Get-ChildItem -LiteralPath $extractDirectory -Recurse -File -Filter 'avcodec-*.dll' |
    Select-Object -First 1
if ($null -eq $avcodecDll) {
    throw 'The FFmpeg shared archive does not contain avcodec-*.dll.'
}
$binDirectory = $avcodecDll.Directory.FullName

$generatorProject = Join-Path $AutoGenRoot 'FFmpeg.AutoGen.CppSharpUnsafeGenerator/FFmpeg.AutoGen.CppSharpUnsafeGenerator.csproj'
$autoGenProject = Join-Path $AutoGenRoot 'FFmpeg.AutoGen/FFmpeg.AutoGen.csproj'
if (-not (Test-Path -LiteralPath $generatorProject -PathType Leaf)) {
    throw "FFmpeg.AutoGen generator project not found: $generatorProject"
}
if (-not (Test-Path -LiteralPath $autoGenProject -PathType Leaf)) {
    throw "FFmpeg.AutoGen project not found: $autoGenProject"
}

Write-Host "Generating bindings from headers in $includeDirectory and binaries in $binDirectory..."
dotnet run `
    --project $generatorProject `
    --configuration Release `
    --framework net9.0 `
    -- `
    --headers $includeDirectory `
    --bin $binDirectory `
    --output $AutoGenRoot `
    -v
if ($LASTEXITCODE -ne 0) {
    throw 'FFmpeg.AutoGen generation failed.'
}

$versionMap = Join-Path $AutoGenRoot 'FFmpeg.AutoGen/generated/ffmpeg.libraries.g.cs'
if (-not (Test-Path -LiteralPath $versionMap -PathType Leaf)) {
    throw 'Generator completed without producing ffmpeg.libraries.g.cs.'
}

Write-Host 'Generated FFmpeg library map:'
Get-Content -LiteralPath $versionMap | Write-Host

$resolvedAutoGenProject = (Resolve-Path -LiteralPath $autoGenProject).Path
$resolvedBinDirectory = (Resolve-Path -LiteralPath $binDirectory).Path

if ($env:GITHUB_PATH) {
    $resolvedBinDirectory | Out-File -FilePath $env:GITHUB_PATH -Encoding utf8 -Append
}
if ($env:GITHUB_OUTPUT) {
    "ffmpeg_tag=$($release.tag_name)" | Out-File -FilePath $env:GITHUB_OUTPUT -Encoding utf8 -Append
    "ffmpeg_asset=$($asset.name)" | Out-File -FilePath $env:GITHUB_OUTPUT -Encoding utf8 -Append
    "ffmpeg_bin=$resolvedBinDirectory" | Out-File -FilePath $env:GITHUB_OUTPUT -Encoding utf8 -Append
    "autogen_project=$resolvedAutoGenProject" | Out-File -FilePath $env:GITHUB_OUTPUT -Encoding utf8 -Append
}

if ($env:GITHUB_STEP_SUMMARY) {
    @"
### FFmpeg native binding input

- Release: ``$($release.tag_name)``
- Asset: ``$($asset.name)``
- Binary directory: ``$resolvedBinDirectory``
- Generated project: ``$resolvedAutoGenProject``
"@ | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Encoding utf8 -Append
}
