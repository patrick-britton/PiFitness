# Review helper for deploy/migrations/. Lists files and checks forward/rollback pairing.
# Forward files may declare a policy marker "-- NO ROLLBACK: <reason>" in their
# first 20 lines. Marked forwards are treated as policy-accepted: shown as paired
# in green with the reason, and not counted by -Strict.
# Usage: .\deploy\list-sql.ps1 [-Since YYYY-MM-DD] [-Strict]
#   -Since:  only files modified on/after date
#   -Strict: exit 1 on unpaired forward without marker, or unpaired rollback
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

function Get-NoRollbackMarker([string]$path) {
    $lines = Get-Content -LiteralPath $path -TotalCount 20 -ErrorAction SilentlyContinue
    foreach ($line in $lines) {
        if ($line -match "^\s*--\s*NO ROLLBACK:\s*(.+)$") {
            return $Matches[1].Trim()
        }
    }
    return $null
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
    if ($rKeys.ContainsKey($k)) {
        $tag = "paired"
        $col = "Green"
    } else {
        $marker = Get-NoRollbackMarker $f.FullName
        if ($marker) {
            $tag = "NO ROLLBACK (policy): $marker"
            $col = "Green"
        } else {
            $tag = "NO ROLLBACK"
            $col = "Yellow"
        }
    }
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

$uF = @($fwd | Where-Object {
    $k = Get-PairKey $_.Name
    if ($rKeys.ContainsKey($k)) { return $false }
    $marker = Get-NoRollbackMarker $_.FullName
    return (-not $marker)
})
$policyF = @($fwd | Where-Object {
    $k = Get-PairKey $_.Name
    if ($rKeys.ContainsKey($k)) { return $false }
    $marker = Get-NoRollbackMarker $_.FullName
    return [bool]$marker
})
$uR = @($rbk | Where-Object { -not $fKeys.ContainsKey((Get-PairKey $_.Name)) })

Write-Host ""
Write-Host "=== Summary ==="
Write-Host "  Forwards without rollback (strict failures): $($uF.Count)"
$uF | ForEach-Object { Write-Host "    - $($_.Name)" -ForegroundColor Yellow }
Write-Host "  Forwards without rollback (policy-accepted via marker): $($policyF.Count)"
$policyF | ForEach-Object { Write-Host "    - $($_.Name)" -ForegroundColor Green }
Write-Host "  Rollbacks without forward: $($uR.Count)"
$uR | ForEach-Object { Write-Host "    - $($_.Name)" -ForegroundColor Yellow }
if ($Strict -and ($uF.Count + $uR.Count) -gt 0) { exit 1 }
exit 0
