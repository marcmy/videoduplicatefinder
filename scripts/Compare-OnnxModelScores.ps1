# Compare score calibration between real DINOv2 benchmark variants.
# All scores are fractions in JSON; displayed differences are percentage points.
[CmdletBinding()]
param(
    [ValidateSet('cuda','directml','cpu')]
    [string]$Provider = 'cuda',
    [ValidateSet('int8','fp32','fp16')]
    [string]$BaselineModel = 'int8',
    [ValidateSet('int8','fp32','fp16')]
    [string]$CandidateModel = 'fp32',
    [int]$Run = 1,
    [string]$ReportDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.OnnxBenchmark\reports'),
    [double[]]$Thresholds = @(89,94)
)
$ErrorActionPreference = 'Stop'
$baseFile = Join-Path $ReportDirectory ("{0}-{1}-run{2}.json" -f $BaselineModel,$Provider,$Run)
if (-not (Test-Path $baseFile) -and $BaselineModel -eq 'int8') {
    $baseFile = Join-Path $ReportDirectory ("{0}-run{1}.json" -f $Provider,$Run)
}
$otherFile = Join-Path $ReportDirectory ("{0}-{1}-run{2}.json" -f $CandidateModel,$Provider,$Run)
if (-not (Test-Path $baseFile)) { throw "Baseline JSON missing: $baseFile" }
if (-not (Test-Path $otherFile)) { throw "Candidate JSON missing: $otherFile" }
$a = Get-Content -LiteralPath $baseFile -Raw | ConvertFrom-Json
$b = Get-Content -LiteralPath $otherFile -Raw | ConvertFrom-Json
if ($a.modelSha256 -eq $b.modelSha256) {
    Write-Warning 'Both reports identify the same ONNX model SHA-256.'
}
if ($a.pairs.Count -ne $b.pairs.Count) { throw 'The reports do not contain the same sample pairs.' }
$rows = @()
for ($i = 0; $i -lt $a.pairs.Count; $i++) {
    $left = $a.pairs[$i]; $right = $b.pairs[$i]
    if ($left.a -ne $right.a -or $left.b -ne $right.b) {
        throw 'Mismatched sample pairs: cannot compare models safely.'
    }
    $leftScore = [double]$left.quantizedSimilarity * 100
    $rightScore = [double]$right.quantizedSimilarity * 100
    $rows += [pscustomobject]@{
        Pair = "$($left.a)-$($left.b)"
        BaselinePct = [math]::Round($leftScore, 3)
        CandidatePct = [math]::Round($rightScore, 3)
        DeltaPoints = [math]::Round($rightScore - $leftScore, 3)
        AbsoluteDelta = [math]::Abs($rightScore - $leftScore)
    }
}
$summary = $rows | Sort-Object AbsoluteDelta -Descending
Write-Host "Baseline: $BaselineModel/$Provider; candidate: $CandidateModel/$Provider" -ForegroundColor Cyan
Write-Host ("Max absolute difference: {0:N3} percentage points; mean absolute difference: {1:N3}" -f
    $summary[0].AbsoluteDelta, (($rows | Measure-Object -Property AbsoluteDelta -Average).Average))
foreach ($threshold in $Thresholds) {
    $flips = @($rows | Where-Object {
        ($_.BaselinePct -ge $threshold) -ne ($_.CandidatePct -ge $threshold)
    })
    $near = @($rows | Where-Object {
        [math]::Abs($_.BaselinePct - $threshold) -lt 5 -or
        [math]::Abs($_.CandidatePct - $threshold) -lt 5
    })
    Write-Host ("Threshold {0}%: {1} flips in {2} pairs; {3} pair(s) within 5 points of threshold" -f
        $threshold, $flips.Count, $rows.Count, $near.Count)
}
$summary | Select-Object -First 15 Pair,BaselinePct,CandidatePct,DeltaPoints | Format-Table -AutoSize
Write-Warning 'Synthetic samples are not a labeled positive/negative video validation set. Never use these results alone to recalibrate production thresholds.'
