<#
.SYNOPSIS
    Lists SQL files in deploy/sql for pre-deployment review.

.DESCRIPTION
    Prints every .sql file in the target directory, split into forward and
    rollback lists and sorted by filename so NNN-MMM ordering is preserved.
    Flags forward migrations that have no matching rollback by prefix.

.PARAMETER Path
    Directory containing .sql files. Defaults to sql/ next to this script.

.PARAMETER Since
    Only list files modified on or after this date.

.EXAMPLE
    .\deploy\list-sql.ps1

.EXAMPLE
    .\deploy\list-sql.ps1 -Since '2026-10-01'
#>
param(
    [string]$Path = (Join-Path $PSScriptRoot 'sql'),
    [datetime]$Since
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $Path)) {
    Write-Error "Directory not found: $Path"
    exit 1
}

$files = Get-ChildItem -LiteralPath $Path -Filter '*.sql' -File

if ($Since) {
    $files = $files | Where-Object { $_.LastWriteTime -ge $Since }
}

if (-not $files) {
    $suffix = if ($Since) { " modified since $($Since.ToString('yyyy-MM-dd'))" } else { '' }
    Write-Host "No .sql files found in $Path$suffix"
    exit 0
}

$forward  = @($files | Where-Object { $_.Name -notmatch 'rollback' } | Sort-Object Name)
$rollback = @($files | Where-Object { $_.Name -match 'rollback' }    | Sort-Object Name)

function Write-Row($file) {
    '{0,-72} {1,10:N0}  {2:yyyy-MM-dd HH:mm}' -f $file.Name, $file.Length, $file.LastWriteTime
}

Write-Host ''
Write-Host "Forward migrations ($($forward.Count))" -ForegroundColor Cyan
Write-Host ('-' * 100)
if ($forward) {
    $forward | ForEach-Object { Write-Row $_ }
} else {
    Write-Host '  (none)'
}

Write-Host ''
Write-Host "Rollback scripts ($($rollback.Count))" -ForegroundColor Yellow
Write-Host ('-' * 100)
if ($rollback) {
    $rollback | ForEach-Object { Write-Row $_ }
} else {
    Write-Host '  (none)'
}

Write-Host ''
Write-Host 'Pair check' -ForegroundColor Green
Write-Host ('-' * 100)

$unpaired = @()
foreach ($f in $forward) {
    $prefix = $f.Name.Split('_')[0]
    $hasRollback = $rollback | Where-Object { $_.Name.StartsWith($prefix) }
    if (-not $hasRollback) { $unpaired += $f.Name }
}

if ($unpaired) {
    Write-Host '  Forward migrations without a paired rollback (review whether each is reversible):' -ForegroundColor Yellow
    $unpaired | ForEach-Object { Write-Host "    - $_" }
} else {
    Write-Host '  Every forward migration has a matching rollback.' -ForegroundColor Green
}