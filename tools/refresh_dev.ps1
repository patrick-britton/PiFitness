<#
.SYNOPSIS
    tools/refresh_dev.ps1 - Refresh local development environment and agent context.

.DESCRIPTION
    Automates the "start of session" workflow:
      1. Optionally synchronizes the local DEV container from production (sync-prod-to-dev.ps1)
      2. Re-generates docs/data/schema.generated.md from the DEV database catalog (schema_map.py)
      3. Re-builds the data lineage graph and SQL definitions in docs/lineage/ (lineage.py build)

.PARAMETER SyncPrd
    If specified, executes tools/db/sync-prod-to-dev.ps1 first to pull the latest PRD backup
    from the Raspberry Pi and restore it into the local Docker container.

.EXAMPLE
    # Full session startup: Sync PRD to DEV, then rebuild schema & lineage
    powershell -ExecutionPolicy Bypass -File tools/refresh_dev.ps1 -SyncPrd

.EXAMPLE
    # Quick context refresh: Rebuild schema & lineage against existing local DEV DB
    powershell -ExecutionPolicy Bypass -File tools/refresh_dev.ps1
#>

[CmdletBinding()]
param(
    [switch]$SyncPrd
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$pythonExe = "$repoRoot\.venv\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python virtual environment not found at: $pythonExe"
    exit 1
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  PiFitness Dev Environment & Agent Context Refresh" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Step 1: Optional PRD -> DEV database synchronization
if ($SyncPrd) {
    Write-Host "`n[Step 1/3] Synchronizing DEV database from production..." -ForegroundColor Yellow
    $syncScript = "$repoRoot\tools\db\sync-prod-to-dev.ps1"
    if (Test-Path $syncScript) {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $syncScript
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Database sync failed with exit code $LASTEXITCODE."
            exit $LASTEXITCODE
        }
    } else {
        Write-Error "Sync script not found at $syncScript"
        exit 1
    }
} else {
    Write-Host "`n[Step 1/3] Skipping PRD sync (pass -SyncPrd to pull latest production backup)." -ForegroundColor Gray
}

# Step 2: Regenerate schema documentation
Write-Host "`n[Step 2/3] Regenerating docs/data/schema.generated.md from DEV database..." -ForegroundColor Yellow
& $pythonExe "$repoRoot\tools\db\schema_map.py"
if ($LASTEXITCODE -ne 0) {
    Write-Error "schema_map.py failed with exit code $LASTEXITCODE."
    exit $LASTEXITCODE
}
Write-Host "  -> Successfully updated docs/data/schema.generated.md" -ForegroundColor Green

# Step 3: Rebuild data lineage graph
Write-Host "`n[Step 3/3] Rebuilding docs/lineage/ graph and definitions..." -ForegroundColor Yellow
& $pythonExe "$repoRoot\tools\data_dictionary\lineage.py" build
if ($LASTEXITCODE -ne 0) {
    Write-Error "lineage.py build failed with exit code $LASTEXITCODE."
    exit $LASTEXITCODE
}
Write-Host "  -> Successfully updated docs/lineage/graph.json and defs/" -ForegroundColor Green

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  Context refresh complete! Ready for feature development." -ForegroundColor Cyan
Write-Host "==========================================================`n" -ForegroundColor Cyan
