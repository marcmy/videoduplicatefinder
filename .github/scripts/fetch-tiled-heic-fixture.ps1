[CmdletBinding()]
param(
    [string]$Destination = 'artifacts/test-media/C007.heic',
    [switch]$RunTests
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (Get-Variable PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

# Nokia's HEIF conformance repository is the provenance source used by FFmpeg's
# tiled-HEIF/grid tests. Pin an immutable commit rather than following master.
$sourceCommit = 'f17e517f7518984b4450349a88edc09519082c74'
$expectedGitBlobSha1 = '0f901627ebb5be41c80516d035e1cdc612a377fa'
$expectedLength = 535814
$sourceUrl = "https://raw.githubusercontent.com/nokiatech/heif_conformance/$sourceCommit/conformance_files/C007.heic"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$destinationPath = if ([IO.Path]::IsPathRooted($Destination)) { $Destination } else { Join-Path $repoRoot $Destination }
$destinationDir = Split-Path -Parent $destinationPath
New-Item -ItemType Directory -Path $destinationDir -Force | Out-Null

Write-Host "Fetching tiled HEIF conformance fixture C007.heic from pinned Nokia commit $sourceCommit..."
Invoke-WebRequest -Uri $sourceUrl -OutFile $destinationPath -UseBasicParsing

$bytes = [IO.File]::ReadAllBytes($destinationPath)
if ($bytes.Length -ne $expectedLength) {
    Remove-Item -LiteralPath $destinationPath -Force -ErrorAction SilentlyContinue
    throw "C007.heic length mismatch: got $($bytes.Length), expected $expectedLength."
}

# Verify the exact Git blob identity (SHA-1 over "blob <length>\0" + file bytes),
# which pins the downloaded binary without requiring the fixture to be vendored here.
$header = [Text.Encoding]::ASCII.GetBytes("blob $($bytes.Length)`0")
$payload = [byte[]]::new($header.Length + $bytes.Length)
[Buffer]::BlockCopy($header, 0, $payload, 0, $header.Length)
[Buffer]::BlockCopy($bytes, 0, $payload, $header.Length, $bytes.Length)
$sha1 = [Security.Cryptography.SHA1]::Create()
try {
    $actualGitBlobSha1 = [Convert]::ToHexString($sha1.ComputeHash($payload)).ToLowerInvariant()
}
finally {
    $sha1.Dispose()
}
if ($actualGitBlobSha1 -ne $expectedGitBlobSha1) {
    Remove-Item -LiteralPath $destinationPath -Force -ErrorAction SilentlyContinue
    throw "C007.heic Git blob mismatch: got $actualGitBlobSha1, expected $expectedGitBlobSha1."
}

$resolvedFixture = (Resolve-Path $destinationPath).Path
$env:VDF_TEST_TILED_HEIC = $resolvedFixture
Write-Host "Verified fixture: $resolvedFixture"
Write-Host "VDF_TEST_TILED_HEIC is set for this PowerShell process."
Write-Host 'The fixture is fetched on demand and is not redistributed in this repository.'

if ($RunTests) {
    Push-Location $repoRoot
    try {
        & dotnet test VDF.IntegrationTests/VDF.IntegrationTests.csproj -c Release --nologo --filter 'FullyQualifiedName~HeicSupportTests.TiledHeic_ExternalConformanceFixture_MatchesProcessFallback'
        if ($LASTEXITCODE -ne 0) {
            throw "Tiled-HEIC integration test failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        Pop-Location
    }
}
