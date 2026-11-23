# VDF ONNX GPU validation build: run an isolated GUI or CLI configuration.
# Requires the extracted GitHub Actions test artifact (GUI/ and CLI/ subfolders).
[CmdletBinding()]
param(
    [ValidateSet('fp16-cuda','int8-directml')]
    [string]$Mode = 'fp16-cuda',
    [switch]$Cli,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CommandArguments = @()
)
$ErrorActionPreference = 'Stop'
$exe = if ($Cli) { Join-Path $PSScriptRoot 'CLI\vdf-cli.exe' }
       else { Join-Path $PSScriptRoot 'GUI\VDF.GUI.exe' }
if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) {
    throw "The test build executable was not found: $exe. Extract the entire ZIP first."
}
# Never reuse the user's installed VDF settings, scanned-files database, AI
# runtimes, sidecars or logs. Keep control/candidate state mutually isolated.
$stateRoot = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'VDF-ONNX-Test'
$env:VDF_DATA_DIR = Join-Path $stateRoot $Mode
New-Item -ItemType Directory -Path $env:VDF_DATA_DIR -Force | Out-Null
$env:VDF_AI_MODEL = if ($Mode -eq 'fp16-cuda') { 'fp16' } else { 'int8' }
$env:VDF_AI_PROVIDER = if ($Mode -eq 'fp16-cuda') { 'cuda' } else { 'directml' }
$env:VDF_AI_DEVICE = 'gpu' # fail rather than silently fall back to CPU
$env:VDF_AI_CUDA_GRAPH_OPT = if ($Mode -eq 'fp16-cuda') { 'extended' } else { 'basic' }
Write-Host ("VDF ONNX TEST: {0}; state {1}" -f $Mode, $env:VDF_DATA_DIR) -ForegroundColor Cyan
Write-Host 'Experimental build. Test only generated media; do not use your normal scan database.'
& $exe @CommandArguments
if ($LASTEXITCODE -ne 0) { throw "VDF test process exited with code $LASTEXITCODE" }
