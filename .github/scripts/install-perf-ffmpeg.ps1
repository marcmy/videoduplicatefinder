# Verified Windows FFmpeg fixtures for release tests and paired performance probes.
function Install-PerfFfmpeg {
    param([Parameter(Mandatory)] [string]$BindingMajor)
    if (!$IsWindows) { throw 'Automatic perf FFmpeg provisioning supports Windows; supply a matching BaselineFfmpegBin on other platforms.' }
    switch ($BindingMajor) {
      '8' {
        $version = '8.1'
        $nativeLibraries = @('avcodec-62.dll', 'avformat-62.dll', 'avutil-60.dll', 'swscale-9.dll', 'swresample-6.dll')
      }
      '9' {
        $version = '9.0'
        $nativeLibraries = @('avcodec-63.dll', 'avformat-63.dll', 'avutil-61.dll', 'swscale-10.dll', 'swresample-7.dll')
      }
      default { throw 'No verified shared FFmpeg fixture is configured for this binding major.' }
    }
    $archive = "ffmpeg-n$version-latest-win64-gpl-shared-$version.zip"
    $baseUrl = 'https://github.com/FFmpeg-Builds/FFmpeg-Builds/releases/download/latest'
    $zipPath = Join-Path $env:RUNNER_TEMP $archive
    $extractRoot = Join-Path $env:RUNNER_TEMP "vdf-ffmpeg-shared-$version"
    $checksumPath = Join-Path $env:RUNNER_TEMP 'ffmpeg-checksums.sha256'

    curl.exe --fail --location --retry 3 --retry-delay 2 "$baseUrl/$archive" --output $zipPath
    if ($LASTEXITCODE -ne 0) { throw "Failed to download $archive" }
    curl.exe --fail --location --retry 3 --retry-delay 2 "$baseUrl/checksums.sha256" --output $checksumPath
    if ($LASTEXITCODE -ne 0) { throw 'Failed to download BtbN checksums.sha256' }

    $checksumLine = Get-Content $checksumPath |
      Where-Object { $_ -match "\s+$([Regex]::Escape($archive))\s*$" } |
      Select-Object -First 1
    if (!$checksumLine) { throw "No checksum entry found for $archive" }
    $expected = ($checksumLine -split '\s+')[0].ToLowerInvariant()
    $actual = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actual -ne $expected) {
      throw "FFmpeg archive checksum mismatch: expected $expected, got $actual"
    }

    if (Test-Path $extractRoot) { Remove-Item $extractRoot -Recurse -Force }
    Expand-Archive -LiteralPath $zipPath -DestinationPath $extractRoot -Force
    $ffmpeg = Get-ChildItem $extractRoot -Filter ffmpeg.exe -File -Recurse | Select-Object -First 1
    $ffprobe = Get-ChildItem $extractRoot -Filter ffprobe.exe -File -Recurse | Select-Object -First 1
    if (!$ffmpeg -or !$ffprobe) { throw 'Shared archive did not contain ffmpeg.exe and ffprobe.exe' }
    if ($ffmpeg.DirectoryName -ne $ffprobe.DirectoryName) { throw 'ffmpeg.exe and ffprobe.exe were not co-located' }

    $bin = $ffmpeg.DirectoryName
    foreach ($dll in $nativeLibraries) {
      if (!(Test-Path (Join-Path $bin $dll) -PathType Leaf)) {
        throw "Shared FFmpeg build is missing required native library: $dll"
      }
    }
    Write-Host "Using verified FFmpeg $version from $bin"
    return $bin
}
