# Dot-source from both launchers and full-scan runner. Windows CUDA tests only.
# We never copy or download cuDNN; it must be installed separately.
function Initialize-VdfCudaDependencies {
    [CmdletBinding()]
    param()
    if (-not $IsWindows) { throw 'Experimental CUDA test build requires Windows x64.' }

    $filename = 'cudnn64_9.dll'
    $separator = [IO.Path]::PathSeparator

    # Explicit test-only path is preferred. Users can point this at their
    # existing NVIDIA, Python or conda cuDNN bin folder.
    if (-not [string]::IsNullOrWhiteSpace($env:VDF_CUDNN_BIN)) {
        $explicit = [IO.Path]::GetFullPath($env:VDF_CUDNN_BIN.Trim('"'))
        if (-not (Test-Path -LiteralPath (Join-Path $explicit $filename) -PathType Leaf)) {
            throw "VDF_CUDNN_BIN='$explicit' does not contain $filename."
        }
        $env:PATH = "$explicit$separator$env:PATH"
    }

    # Never infer that CUDA is ready just because the provider session created.
    # The actual C# preflight executes one convolution before scanning.
    $searchDirs = @($env:PATH -split [regex]::Escape([string]$separator) |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        ForEach-Object { $_.Trim('"') })
    $present = @($searchDirs | Where-Object {
        Test-Path -LiteralPath (Join-Path $_ $filename) -PathType Leaf
    })
    if ($present.Count -gt 0) {
        Write-Host "cuDNN 9 DLL found: $($present[0])" -ForegroundColor Green
        return
    }

    # NVIDIA's documented Windows installation can have additional CUDA
    # version/x64 subfolders under CUDNN/v9.x/bin. Search only narrow
    # NVIDIA/CUDA roots, never the user's personal video directories.
    $roots = @(
        $env:CUDNN_PATH,
        $env:CUDNN_HOME,
        $env:CUDNN_ROOT,
        $env:CUDA_PATH,
        (Join-Path $env:ProgramFiles 'NVIDIA\CUDNN')
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) -and (Test-Path -LiteralPath $_ -PathType Container) } |
        Select-Object -Unique
    $matches = @(
        foreach ($root in $roots) {
            Get-ChildItem -LiteralPath $root -Recurse -Depth 6 -File -Filter $filename -ErrorAction SilentlyContinue |
                ForEach-Object { $_.DirectoryName }
        }
    )
    $matches = @($matches | Sort-Object -Unique)
    if ($matches.Count -eq 1) {
        $env:PATH = "$($matches[0])$separator$env:PATH"
        Write-Host "cuDNN 9 discovered and added for this process: $($matches[0])" -ForegroundColor Green
        return
    }

    if ($matches.Count -gt 1) {
        Write-Host 'Multiple cuDNN 9 installations were found:' -ForegroundColor Yellow
        $matches | ForEach-Object { Write-Host "  $_" }
        throw "Set VDF_CUDNN_BIN to the correct bin directory for your installed CUDA version, then rerun."
    }

    throw @"
CUDA test prerequisite missing: $filename was not found on PATH or under the common NVIDIA installation directories.

The CUDA provider can initialize even without cuDNN, but the first DINOv2
convolution will fail. Install a compatible cuDNN 9 build, or point to your
existing installation with:
    `$env:VDF_CUDNN_BIN = 'C:\path\to\cuDNN\bin'
Then rerun the same script. This changes only the current process PATH;
it does not change your machine-wide environment or copy DLLs.
"@
}
