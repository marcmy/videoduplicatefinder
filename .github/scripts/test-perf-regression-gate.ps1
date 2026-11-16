$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $PSScriptRoot 'perf-regression-gate.ps1'), [ref]$tokens, [ref]$parseErrors)
if ($parseErrors) { throw ($parseErrors | Out-String) }
foreach ($name in @('Get-BenchmarkDll', 'Invoke-RegressionProbe')) {
    $function = $ast.Find({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name }, $true)
    if (!$function) { throw "Missing function: $name" }
    . ([scriptblock]::Create($function.Extent.Text))
}
$root = Join-Path ([IO.Path]::GetTempPath()) ([Guid]::NewGuid().ToString('N'))
$originalPath = $env:PATH
try {
    $project = Join-Path $root 'VDF.Benchmarks'
    New-Item -ItemType Directory -Path $project -Force > $null
    foreach ($framework in @('net10.0', 'net11.0')) {
        "<Project><PropertyGroup><TargetFramework>$framework</TargetFramework></PropertyGroup></Project>" |
            Set-Content (Join-Path $project 'VDF.Benchmarks.csproj')
        $output = Join-Path $project "bin/Release/$framework"
        New-Item -ItemType Directory -Path $output -Force > $null
        $expected = Join-Path $output 'VDF.Benchmarks.dll'
        New-Item -ItemType File -Path $expected -Force > $null
        if ((Get-BenchmarkDll -WorkingDirectory $root) -ne $expected) { throw "Wrong DLL for $framework" }
        Remove-Item $expected
        $rejected = $false
        try { Get-BenchmarkDll -WorkingDirectory $root > $null } catch { $rejected = $true }
        if (!$rejected) { throw 'Missing output was accepted.' }
        New-Item -ItemType File -Path $expected -Force > $null
    }
    $Modes = 'process,native-cpu'
    $Iterations = 9
    $WarmupIterations = 2
    $MaxRegressionPercent = 15
    $script:observedPath = ''
    function dotnet { $script:observedPath = $env:PATH; $global:LASTEXITCODE = 7 }
    $bin = Join-Path $root 'baseline-ffmpeg'
    $code = Invoke-RegressionProbe -WorkingDirectory $root -OutputPath (Join-Path $root 'out.json') -LogPath (Join-Path $root 'probe.log') -FfmpegBin $bin
    if ($code -ne 7) { throw 'Probe exit code was not propagated.' }
    if (!$script:observedPath.StartsWith("$bin$([IO.Path]::PathSeparator)")) { throw 'Baseline FFmpeg was not preferred.' }
    if ($env:PATH -ne $originalPath) { throw 'Baseline PATH leaked into the candidate.' }
    function dotnet { throw 'simulated process launch failure' }
    $rejected = $false
    try { Invoke-RegressionProbe -WorkingDirectory $root -OutputPath (Join-Path $root 'out.json') -LogPath (Join-Path $root 'probe.log') -FfmpegBin $bin > $null } catch { $rejected = $true }
    if (!$rejected -or $env:PATH -ne $originalPath) { throw 'Failed launch did not restore PATH.' }
    Write-Host 'Performance gate helper tests passed.'
}
finally {
    $env:PATH = $originalPath
    Remove-Item $root -Recurse -Force
}
# The intentional nonzero mock exit must not become the CI shell's exit status.
exit 0
