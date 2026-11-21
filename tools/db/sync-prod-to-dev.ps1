<#
.SYNOPSIS
    Replicate the production database into the local dev container.

.DESCRIPTION
    Automates the full PRD -> dev workflow:
      Preflight: Docker daemon, container running, client tools on PATH
      1. Pull the most recent backup from the Pi via SSH
      2. Verify the dump is a valid PG 18 custom-format archive
      3. Restore the raw dump into a staging database (the only restore)
      4. Truncate live OAuth tables in staging (sanitization)
      5. Swap: drop pifitness_dev, rename staging to pifitness_dev (instant)
      6. Re-stamp the pifitness.env marker on dev
      7. Re-snapshot pifitness_dev_template from dev
      8. Verify tables/views/row counts
      9. Clean container temp files

    The dev database is preserved on failure: it is only dropped after
    staging has been successfully restored and sanitized. Because staging is
    renamed into place, no second dump or restore is needed.

.NOTES
    Compatible with Windows PowerShell 5.1 and PowerShell 7+.
    Keep this file ASCII-only: Windows PowerShell 5.1 reads BOM-less files as ANSI.

.PARAMETER SshHost
    SSH target for the Pi. Default: god@pfpi

.PARAMETER RemoteBackupDir
    Directory on the Pi containing the backups.
    Default: /home/god/Documents/DB_Backups

.PARAMETER BackupFile
    Specific backup to pull (name only, or full remote path).
    Default: the most recently modified *.dump in RemoteBackupDir

.PARAMETER SkipPull
    Use the existing local prod-latest.dump instead of pulling from the Pi.

.PARAMETER SkipSanitize
    Do NOT truncate the OAuth tables. The resulting dev database will
    contain live production OAuth tokens. Not recommended.

.PARAMETER DeleteDumps
    Delete the local dump file after the restore succeeds.
    By default, the dump is kept for faster re-runs (-SkipPull).

.PARAMETER ContainerName
    Docker container name. Default: pifitness-dev-db

.EXAMPLE
    .\tools\db\sync-prod-to-dev.ps1

    Full workflow: pull latest backup, sanitize, restore into dev.

.EXAMPLE
    .\tools\db\sync-prod-to-dev.ps1 -BackupFile personal_fitness_20261020_010044.dump

    Pull a specific backup instead of the most recent.

.EXAMPLE
    .\tools\db\sync-prod-to-dev.ps1 -SkipPull

    Re-run using the existing local dump, without re-fetching from the Pi.
#>
[CmdletBinding()]
param(
    [string]$SshHost         = 'god@pfpi',
    [string]$RemoteBackupDir = '/home/god/Documents/DB_Backups',
    [string]$BackupFile      = '',
    [switch]$SkipPull,
    [switch]$SkipSanitize,
    [switch]$DeleteDumps,
    [string]$ContainerName   = 'pifitness-dev-db'
)

$ErrorActionPreference = 'Stop'

# ---- Fixed local config (edit only if compose.yaml changes) ----
$LocalUser     = 'dev'
$LocalPassword = 'devpassword'
$LocalHostPort = 15432
$LocalAdminDb  = 'postgres'
$DevDb         = 'pifitness_dev'
$StagingDb     = 'pifitness_staging'

# Anchor paths to the repo root, regardless of current directory.
# This script lives in <repo>\tools\db, so the repo root is two levels up.
# NOTE: Join-Path is called with two arguments only; the 3+ argument form
# (-AdditionalChildPath) does not exist in Windows PowerShell 5.1.
$RepoRoot      = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$RestoreDir    = Join-Path $RepoRoot '.db_restore'
$LocalDump     = Join-Path $RestoreDir 'prod-latest.dump'
$LogFile       = Join-Path $PSScriptRoot 'sync-prod-to-dev.log'

# ---- Logging ----
Start-Transcript -Path $LogFile -Force | Out-Null

function Write-Step($m)  { Write-Host ''; Write-Host "==> $m" -ForegroundColor Cyan }
function Write-Info($m)  { Write-Host "    $m" }
function Write-Warn2($m) { Write-Host "    WARNING: $m" -ForegroundColor Yellow }
function Write-Ok($m)    { Write-Host "    $m" -ForegroundColor Green }

# Run a native command and capture stdout+stderr as plain strings.
# Windows PowerShell 5.1 turns redirected stderr text into a terminating error
# when $ErrorActionPreference is 'Stop', so relax it for the duration of the call.
# Check $LASTEXITCODE afterwards for success/failure.
function Invoke-NativeCapture {
    param(
        [Parameter(Mandatory)][string]$File,
        [string[]]$Arguments = @()
    )
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & $File @Arguments 2>&1 | ForEach-Object { "$_" }
    } finally {
        $ErrorActionPreference = $prev
    }
    return @($out)
}

function Invoke-Psql {
    param([string]$Database, [string]$Sql, [switch]$IgnoreExit)
    $a = @('exec','-i',$ContainerName,'psql','-U',$LocalUser,'-d',$Database,'-v','ON_ERROR_STOP=1','-c',$Sql)
    & docker @a
    if (-not $IgnoreExit -and $LASTEXITCODE -ne 0) { throw "psql failed (exit $LASTEXITCODE): $Sql" }
}

function Reset-Database {
    param([string]$Database)
    Write-Info "Dropping $Database (WITH FORCE)"
    Invoke-Psql -Database $LocalAdminDb -Sql "DROP DATABASE IF EXISTS $Database WITH (FORCE)" | Out-Null
    Write-Info "Creating $Database"
    Invoke-Psql -Database $LocalAdminDb -Sql "CREATE DATABASE $Database" | Out-Null
}

try {

# ============================== Preflight ==============================
Write-Step 'Preflight'

Write-Info 'Checking client tools on PATH'
$missing = @()
foreach ($t in @('docker','psql','pg_restore','scp','ssh')) {
    if (-not (Get-Command $t -ErrorAction SilentlyContinue)) { $missing += $t }
}
if ($missing.Count -gt 0) { throw "Missing tools on PATH: $($missing -join ', ')" }
Write-Ok 'docker, psql, pg_restore, scp, ssh all present'

Write-Info 'Checking Docker daemon'
$di = Invoke-NativeCapture -File 'docker' -Arguments @('info')
if ($LASTEXITCODE -ne 0 -or (($di -join "`n") -match 'failed to connect')) {
    throw 'Docker daemon is not running. Start Docker Desktop and retry.'
}
Write-Ok 'Docker daemon reachable'

Write-Info "Checking container $ContainerName is running"
$running = @(& docker ps --filter "name=$ContainerName" --filter 'status=running' --format '{{.Names}}')
if ($running -notcontains $ContainerName) {
    throw "Container '$ContainerName' is not running. Run 'docker compose up -d' and retry."
}
Write-Ok "Container $ContainerName running"

Write-Info 'Checking container PostgreSQL version'
$v = ([string](& docker exec -i $ContainerName psql -U $LocalUser -d $LocalAdminDb -t -c 'SHOW server_version')).Trim()
Write-Info "Container version: $v"
if ($v -notmatch '^18\.') { Write-Warn2 "Container is not PostgreSQL 18.x. Restore may fail." }

# Make sure the local download/work directory exists
if (-not (Test-Path -LiteralPath $RestoreDir)) {
    New-Item -ItemType Directory -Path $RestoreDir -Force | Out-Null
}

# ============================== Step 1: Pull ==============================
if ($SkipPull) {
    Write-Step 'Step 1: Skip pull (using existing local dump)'
    if (-not (Test-Path -LiteralPath $LocalDump)) { throw "SkipPull specified but $LocalDump does not exist." }
    $size = (Get-Item -LiteralPath $LocalDump).Length
    $age  = (Get-Date) - (Get-Item -LiteralPath $LocalDump).LastWriteTime
    Write-Info "Using existing: $LocalDump"
    Write-Info "Size: $size bytes"
    Write-Info "Age:  $($age.ToString('d\d\ h\h\ m\m'))"
    if ($age.TotalHours -gt 48) { Write-Warn2 'Dump is more than 48 hours old.' }
} else {
    Write-Step 'Step 1: Pull latest backup from Pi'

    if (-not $BackupFile) {
        Write-Info "Finding most recent backup in $RemoteBackupDir"
        $raw = & ssh $SshHost "ls -t $RemoteBackupDir/*.dump 2>/dev/null | head -1"
        if ($LASTEXITCODE -ne 0) { throw "ssh to $SshHost failed (exit $LASTEXITCODE)" }
        # [string] cast keeps .Trim() safe when ssh returns nothing ($null)
        $BackupFile = ([string]$raw).Trim()
        if (-not $BackupFile) { throw "No .dump files found in $RemoteBackupDir on $SshHost" }
        Write-Info "Most recent: $BackupFile"
    } else {
        if ($BackupFile -notmatch '/') { $BackupFile = "$RemoteBackupDir/$BackupFile" }
        Write-Info "Using specified backup: $BackupFile"
    }

    $rawSize = & ssh $SshHost "stat -c %s '$BackupFile' 2>/dev/null"
    $remoteSize = ([string]$rawSize).Trim()
    if (-not $remoteSize) { throw "Backup '$BackupFile' not found on $SshHost" }
    Write-Info "Remote size: $remoteSize bytes"

    Write-Info 'Downloading (this may take several minutes)'
    & scp "${SshHost}:${BackupFile}" $LocalDump
    if ($LASTEXITCODE -ne 0) { throw 'scp failed' }

    $localSize = (Get-Item -LiteralPath $LocalDump).Length
    if ($localSize -ne [long]$remoteSize) {
        throw "Size mismatch: remote $remoteSize, local $localSize. Transfer truncated."
    }
    Write-Ok "Download complete: $localSize bytes"
}

# ============================== Step 2: Verify ==============================
Write-Step 'Step 2: Verify dump integrity'
$header = Invoke-NativeCapture -File 'pg_restore' -Arguments @('--list', $LocalDump)
if ($LASTEXITCODE -ne 0) { throw 'pg_restore --list failed; dump may be corrupt' }

$fromMatch = $header | Select-String 'Dumped from database version:' | Select-Object -First 1
if (-not $fromMatch) {
    Write-Warn2 'Could not determine the PostgreSQL version that produced the dump.'
} else {
    $fromLine = $fromMatch.ToString()
    Write-Info $fromLine.Trim()
    if ($fromLine -notmatch '18\.6') {
        Write-Warn2 'Dump was produced by a PostgreSQL version other than 18.6. Restore may fail.'
    } else {
        Write-Ok 'Dump is PG 18.6 custom-format archive'
    }
}

# ============================== Step 3: Restore staging ==============================
Write-Step 'Step 3: Restore dump into staging'
Reset-Database -Database $StagingDb

Write-Info "Copying dump into container"
& docker cp $LocalDump "${ContainerName}:/tmp/prod-latest.dump"
if ($LASTEXITCODE -ne 0) { throw 'docker cp failed' }

Write-Info "Restoring into $StagingDb (several minutes)"
& docker exec -i $ContainerName pg_restore -U $LocalUser -d $StagingDb --no-owner --no-privileges /tmp/prod-latest.dump
if ($LASTEXITCODE -ne 0) { throw 'pg_restore into staging failed' }
Write-Ok 'Staging restore complete'

# ============================== Step 4: Sanitize ==============================
if ($SkipSanitize) {
    Write-Step 'Step 4: Skip sanitize'
    Write-Warn2 'LIVE PRODUCTION OAUTH TOKENS WILL REMAIN IN DEV'
} else {
    Write-Step 'Step 4: Truncate live OAuth tables in staging'
    Invoke-Psql -Database $StagingDb -Sql 'TRUNCATE api_services.api_tokens, api_services.spotify_auth_metadata' | Out-Null
    Write-Ok 'OAuth tables truncated (re-authorize Garmin/Spotify in dev as needed)'
}

# ============================== Step 5: Swap into dev ==============================
# Dev is only dropped now that staging is fully restored and sanitized.
# Renaming is instant, so there is no second dump or restore.
Write-Step 'Step 5: Swap staging into dev'
Write-Info "Dropping $DevDb (WITH FORCE)"
Invoke-Psql -Database $LocalAdminDb -Sql "DROP DATABASE IF EXISTS $DevDb WITH (FORCE)" | Out-Null
Write-Info "Renaming $StagingDb to $DevDb"
Invoke-Psql -Database $LocalAdminDb -Sql "ALTER DATABASE $StagingDb RENAME TO $DevDb" | Out-Null
Write-Ok "Dev database replaced from staging"

# ============================== Step 6: Re-stamp marker ==============================
Write-Step 'Step 6: Re-stamp pifitness.env marker on dev'
Write-Info "Stamping pifitness.env = 'dev' on $DevDb"
Invoke-Psql -Database $LocalAdminDb -Sql "ALTER DATABASE $DevDb SET pifitness.env = 'dev'" | Out-Null
Write-Ok 'Marker pifitness.env = dev stamped'

# ============================== Step 7: Re-snapshot template ==============================
Write-Step 'Step 7: Re-snapshot pifitness_dev_template from dev'
Write-Info "Re-snapshot template database from $DevDb"
Invoke-Psql -Database $LocalAdminDb -Sql "DROP DATABASE IF EXISTS pifitness_dev_template WITH (FORCE)" | Out-Null
Invoke-Psql -Database $LocalAdminDb -Sql "CREATE DATABASE pifitness_dev_template TEMPLATE = $DevDb" | Out-Null
Write-Ok 'Template pifitness_dev_template re-snapshotted from dev'

# ============================== Step 8: Verify ==============================
Write-Step 'Step 8: Verify dev database'
Invoke-Psql -Database $DevDb -Sql @'
SELECT 'tables' AS metric, count(*)::text AS value FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL SELECT 'views', count(*)::text FROM pg_views WHERE schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL SELECT 'api_tokens', count(*)::text FROM api_services.api_tokens
UNION ALL SELECT 'spotify_auth_metadata', count(*)::text FROM api_services.spotify_auth_metadata
'@

# ============================== Step 9: Cleanup ==============================
Write-Step 'Step 9: Cleanup'

Write-Info 'Removing container temp files'
& docker exec -i $ContainerName rm -f /tmp/prod-latest.dump

if ($DeleteDumps) {
    Write-Info 'Deleting local dump'
    Remove-Item -LiteralPath $LocalDump -ErrorAction SilentlyContinue
} else {
    Write-Info 'Keeping local dump for faster re-runs:'
    Write-Info "  $LocalDump"
    Write-Info 'Pass -DeleteDumps to remove it.'
}

Write-Step 'Done'
Write-Host 'Dev database refreshed from production.' -ForegroundColor Green
Write-Host "Connection: postgresql://${LocalUser}:${LocalPassword}@localhost:${LocalHostPort}/${DevDb}" -ForegroundColor Green

} catch {
    Write-Host ''
    Write-Host "FAILED: $_" -ForegroundColor Red
    Write-Host "Log: $LogFile" -ForegroundColor Red
    exit 1
} finally {
    Stop-Transcript | Out-Null
}
