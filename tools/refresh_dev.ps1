<#
.SYNOPSIS
    tools/refresh_dev.ps1 - Refresh local development environment and agent context.

.DESCRIPTION
    Automates the "start of session" workflow:
      1. Synchronizes the local DEV container from production (sync-prod-to-dev.ps1)
         by pulling the latest PRD backup from the Raspberry Pi. This is the default;
         use -SkipSync to bypass it.
      2. Re-generates docs/data/schema.generated.md from the DEV database catalog (schema_map.py)
      3. Re-builds the data lineage graph and SQL definitions in docs/lineage/ (lineage.py build)

.NOTES
    Compatible with Windows PowerShell 5.1 and PowerShell 7+.
    Keep this file ASCII-only: Windows PowerShell 5.1 reads BOM-less files as ANSI.

.PARAMETER SkipSync
    Skip the PRD -> DEV sync entirely and only rebuild schema & lineage against the
    existing local DEV database.

.PARAMETER SkipPull
    Still run the sync, but reuse the existing local prod-latest.dump instead of
    downloading the latest backup from the Pi. Ignored if -SkipSync is specified.

.PARAMETER SyncPrd
    DEPRECATED. Syncing is now the default behavior, so this switch is a no-op kept
    only so older callers do not break. Use -SkipSync to opt out instead.

.EXAMPLE
    # Default: Pull latest PRD backup, restore into DEV, then rebuild schema & lineage
    .\tools\refresh_dev.ps1

.EXAMPLE
    # Re-run the sync using the already-downloaded local dump (no Pi download)
    .\tools\refresh_dev.ps1 -SkipPull

.EXAMPLE
    # Quick context refresh: Rebuild schema & lineage against existing local DEV DB
    .\tools\refresh_dev.ps1 -SkipSync
#>

[CmdletBinding()]
param(
    [switch]$SkipSync,
    [switch]$SkipPull,
    [switch]$SyncPrd   # Deprecated no-op; syncing is now the default.
)

$ErrorActionPreference = "Stop"

# Print an error and exit with the given code. (Write-Error + exit does not work
# under $ErrorActionPreference = 'Stop': the error terminates before 'exit' runs,
# and the original exit code is lost.)
function Stop-Refresh {
    param([string]$Message, [int]$Code = 1)
    if ($Code -eq 0) { $Code = 1 }
    Write-Host "ERROR: $Message" -ForegroundColor Red
    exit $Code
}

$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$pythonExe = "$repoRoot\.venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $pythonExe)) {
    Stop-Refresh "Python virtual environment not found at: $pythonExe"
}

if ($SyncPrd) {
    Write-Warning "-SyncPrd is deprecated: syncing is now the default. Use -SkipSync to opt out."
}

if ($SkipSync -and $SkipPull) {
    Write-Warning "-SkipPull has no effect when -SkipSync is specified."
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  PiFitness Dev Environment & Agent Context Refresh" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Step 1: PRD -> DEV database synchronization (default; use -SkipSync to bypass)
if (-not $SkipSync) {
    Write-Host "`n[Step 1/3] Synchronizing DEV database from production..." -ForegroundColor Yellow
    $syncScript = "$repoRoot\tools\db\sync-prod-to-dev.ps1"
    if (-not (Test-Path -LiteralPath $syncScript)) {
        Stop-Refresh "Sync script not found at $syncScript"
    }

    # Launch the child with whichever PowerShell is running this script
    # (powershell.exe on 5.1, pwsh.exe on 7+) instead of hard-coding one.
    $psExe = (Get-Process -Id $PID).Path
    $syncArgs = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $syncScript)
    if ($SkipPull) { $syncArgs += '-SkipPull' }

    & $psExe @syncArgs
    if ($LASTEXITCODE -ne 0) {
        Stop-Refresh "Database sync failed with exit code $LASTEXITCODE." $LASTEXITCODE
    }
} else {
    Write-Host "`n[Step 1/3] Skipping PRD sync (-SkipSync specified)." -ForegroundColor Gray
}

# Step 2: Regenerate schema documentation
Write-Host "`n[Step 2/3] Regenerating docs/data/schema.generated.md from DEV database..." -ForegroundColor Yellow
& $pythonExe "$repoRoot\tools\db\schema_map.py"
if ($LASTEXITCODE -ne 0) {
    Stop-Refresh "schema_map.py failed with exit code $LASTEXITCODE." $LASTEXITCODE
}
Write-Host "  -> Successfully updated docs/data/schema.generated.md" -ForegroundColor Green

# Step 3: Rebuild data lineage graph
Write-Host "`n[Step 3/3] Rebuilding docs/lineage/ graph and definitions..." -ForegroundColor Yellow
& $pythonExe "$repoRoot\tools\data_dictionary\lineage.py" build
if ($LASTEXITCODE -ne 0) {
    Stop-Refresh "lineage.py build failed with exit code $LASTEXITCODE." $LASTEXITCODE
}
Write-Host "  -> Successfully updated docs/lineage/graph.json and defs/" -ForegroundColor Green

Write-Host "`n==========================================================" -ForegroundColor Cyan
Write-Host "  Context refresh complete! Ready for feature development." -ForegroundColor Cyan
Write-Host "==========================================================`n" -ForegroundColor Cyan
