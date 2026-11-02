# Review helper for deploy/migrations/. Lists files and checks forward/rollback pairing.
# Usage: .\deploy\list-sql.ps1 [-Since YYYY-MM-DD] [-Strict]
#   -Since:  only files modified on/after date
#   -Strict: exit 1 on any unpaired file
[CmdletBinding()]
param([string]$Since, [switch]$Strict)

$ErrorActionPreference = "Stop"
$dir = Join-Path $PSScriptRoot "migrations"
if (-not (Test-Path -LiteralPath $dir)) {
    Write-Host "ERROR: $dir not found" -ForegroundColor Red
    exit 2
}

$files = @(Get-ChildItem -LiteralPath $dir -Filter "*.sql" -File)
if ($Since) {
    try { $cut = [datetime]::Parse($Since) }
    catch { Write-Host "ERROR: bad date: $Since" -ForegroundColor Red; exit 2 }
    $files = @($files | Where-Object { $_.LastWriteTime -ge $cut })
}
if ($files.Count -eq 0) { Write-Host "No .sql files found"; exit 0 }

function Get-PairKey([string]$n) {
    $b = $n -replace "\.sql`$",""
    $b = $b -replace "[_\\-]?rollback[_\\-]?",""
    return ($b -replace "[_\\-]`$","")
}

$fwd = @($files | Where-Object { $_.Name -notmatch "rollback" })
$rbk = @($files | Where-Object { $_.Name -match "rollback" })

$fKeys = @{}; $rKeys = @{}
foreach ($f in $fwd) { $fKeys[(Get-PairKey $f.Name)] = $f.Name }
foreach ($r in $rbk) { $rKeys[(Get-PairKey $r.Name)] = $r.Name }

Write-Host ""
Write-Host "=== Forwards ($($fwd.Count)) ==="
foreach ($f in ($fwd | Sort-Object Name)) {
    $k = Get-PairKey $f.Name
    $paired = $rKeys.ContainsKey($k)
    $tag = if ($paired) { "paired" } else { "NO ROLLBACK" }
    $col = if ($paired) { "Green" } else { "Yellow" }
    Write-Host ("  {0,-60}  {1}" -f $f.Name, $tag) -ForegroundColor $col
}
Write-Host ""
Write-Host "=== Rollbacks ($($rbk.Count)) ==="
foreach ($r in ($rbk | Sort-Object Name)) {
    $k = Get-PairKey $r.Name
    $paired = $fKeys.ContainsKey($k)
    $tag = if ($paired) { "paired" } else { "NO FORWARD" }
    $col = if ($paired) { "Green" } else { "Yellow" }
    Write-Host ("  {0,-60}  {1}" -f $r.Name, $tag) -ForegroundColor $col
}

$uF = @($fwd | Where-Object { -not $rKeys.ContainsKey((Get-PairKey $_.Name)) })
$uR = @($rbk | Where-Object { -not $fKeys.ContainsKey((Get-PairKey $_.Name)) })
Write-Host ""
Write-Host "=== Summary ==="
Write-Host "  Forwards without rollback: $($uF.Count)"
$uF | ForEach-Object { Write-Host "    - $($_.Name)" -ForegroundColor Yellow }
Write-Host "  Rollbacks without forward: $($uR.Count)"
$uR | ForEach-Object { Write-Host "    - $($_.Name)" -ForegroundColor Yellow }
if ($Strict -and ($uF.Count + $uR.Count) -gt 0) { exit 1 }
exit 0