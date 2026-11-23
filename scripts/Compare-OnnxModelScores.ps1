# Compare score calibration between real DINOv2 benchmark variants.
# All scores are fractions in JSON; displayed differences are percentage points.
[CmdletBinding()]
param(
    [ValidateSet('cuda','directml','cpu')]
    [string]$Provider = 'cuda',
    [ValidateSet('','directml','cuda','cpu')]
    [string]$BaselineProvider = '',
    [ValidateSet('','directml','cuda','cpu')]
    [string]$CandidateProvider = '',
    [ValidateSet('int8','fp32','fp16')]
    [string]$BaselineModel = 'int8',
    [ValidateSet('int8','fp32','fp16')]
    [string]$CandidateModel = 'fp32',
    [ValidateSet('basic','extended','all','disabled')]
    [string]$BaselineOptimization = 'basic',
    [ValidateSet('basic','extended','all','disabled')]
    [string]$CandidateOptimization = 'basic',
    [ValidateSet('','public')][string]$CorpusTag = '',
    [int]$Run = 1,
    [string]$ReportDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'VDF.OnnxBenchmark\reports'),
    [double[]]$Thresholds = @(89,94)
)
$ErrorActionPreference = 'Stop'
$baseProvider = if ($BaselineProvider) { $BaselineProvider } else { $Provider }
$candidateProvider = if ($CandidateProvider) { $CandidateProvider } else { $Provider }
$baseTag = if ($baseProvider -eq 'cuda' -and $BaselineOptimization -ne 'basic') { "-$BaselineOptimization" } else { '' }
$otherTag = if ($candidateProvider -eq 'cuda' -and $CandidateOptimization -ne 'basic') { "-$CandidateOptimization" } else { '' }
if ($CorpusTag) { $baseTag += "-$CorpusTag"; $otherTag += "-$CorpusTag" }
$baseFile = Join-Path $ReportDirectory ("{0}-{1}{2}-run{3}.json" -f $BaselineModel,$baseProvider,$baseTag,$Run)
if (-not (Test-Path $baseFile) -and $BaselineModel -eq 'int8' -and -not $CorpusTag) {
    $baseFile = Join-Path $ReportDirectory ("{0}-run{1}.json" -f $baseProvider,$Run)
}
$otherFile = Join-Path $ReportDirectory ("{0}-{1}{2}-run{3}.json" -f $CandidateModel,$candidateProvider,$otherTag,$Run)
if (-not (Test-Path $baseFile)) { throw "Baseline JSON missing: $baseFile" }
if (-not (Test-Path $otherFile)) { throw "Candidate JSON missing: $otherFile" }
$a = Get-Content -LiteralPath $baseFile -Raw | ConvertFrom-Json
$b = Get-Content -LiteralPath $otherFile -Raw | ConvertFrom-Json
if ($a.modelSha256 -eq $b.modelSha256 -and $baseFile -eq $otherFile) {
    Write-Warning 'Both reports identify the same model and the same run.'
}
if ($a.pairs.Count -ne $b.pairs.Count) { throw 'The reports do not contain the same sample pairs.' }
if ($null -ne $a.frameIds -and $null -ne $b.frameIds) {
    if (($a.frameIds -join '|') -ne ($b.frameIds -join '|')) {
        throw 'Different frame IDs. Comparisons require identical sample order.'
    }
}
if ($null -ne $a.frameSha256 -and $null -ne $b.frameSha256) {
    if (($a.frameSha256 -join '|') -ne ($b.frameSha256 -join '|')) {
        throw 'Different RGB frame SHA-256 hashes. Regenerate/run both variants on the same corpus.'
    }
}
$rows = @()
for ($i = 0; $i -lt $a.pairs.Count; $i++) {
    $left = $a.pairs[$i]; $right = $b.pairs[$i]
    if ($left.a -ne $right.a -or $left.b -ne $right.b) {
        throw 'Mismatched sample pairs: cannot compare models safely.'
    }
    $leftScore = [double]$left.quantizedSimilarity * 100
    $rightScore = [double]$right.quantizedSimilarity * 100
    $rows += [pscustomobject]@{
        Pair = if ($null -ne $left.idA -and $null -ne $left.idB) {
            "$($left.idA) / $($left.idB)" } else { "$($left.a)-$($left.b)" }
        ExpectedAligned = $left.expectedAligned
        BaselinePct = $leftScore
        CandidatePct = $rightScore
        DeltaPoints = [math]::Round($rightScore - $leftScore, 3)
        AbsoluteDelta = [math]::Abs($rightScore - $leftScore)
    }
}
$summary = $rows | Sort-Object AbsoluteDelta -Descending
Write-Host "Baseline: $BaselineModel/$baseProvider ($BaselineOptimization); candidate: $CandidateModel/$candidateProvider ($CandidateOptimization)" -ForegroundColor Cyan
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
    if ($CorpusTag -eq 'public') {
        $aligned = @($rows | Where-Object { $_.ExpectedAligned -eq $true })
        $changedAligned = @($flips | Where-Object { $_.ExpectedAligned -eq $true })
        $changedOther = @($flips | Where-Object { $_.ExpectedAligned -ne $true })
        Write-Host ("  Aligned synthetic-source pairs: {0}; decision flips: {1}; other pairs flipping: {2}" -f
            $aligned.Count, $changedAligned.Count, $changedOther.Count)
    }
}
$summary | Select-Object -First 15 Pair,
    @{Name='BaselinePct';Expression={[math]::Round($_.BaselinePct,3)}},
    @{Name='CandidatePct';Expression={[math]::Round($_.CandidatePct,3)}},
    DeltaPoints | Format-Table -AutoSize
Write-Warning 'Synthetic samples are not a labeled positive/negative video validation set. Never use these results alone to recalibrate production thresholds.'
