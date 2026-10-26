<#
.SYNOPSIS
    Replicate the production database into the local dev container.

.DESCRIPTION
    Automates the full PRD -> dev workflow:
      1. Preflight: Docker daemon, container running, client tools on PATH
      2. Pull the most recent backup from the Pi via SSH
      3. Verify the dump is a valid PG 18 custom-format archive
      4. Restore the raw dump into a staging database
      5. Truncate live OAuth tables (sanitization)
      6. Dump the sanitized staging database to disk
      7. Restore the sanitized dump into pifitness_dev
      8. Verify tables/views/row counts
      9. Drop staging, clean container temp files

    The dev database is preserved on failure: it is only dropped after
    staging has been successfully restored and sanitized.

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
    Delete the local dump files after the restore succeeds.
    By default, dumps are kept for faster re-runs (-SkipPull).

.PARAMETER ContainerName
    Docker container name. Default: pifitness-dev-db

.EXAMPLE
    .\scripts\sync-prod-to-dev.ps1

    Full workflow: pull latest backup, sanitize, restore into dev.

.EXAMPLE
    .\scripts\sync-prod-to-dev.ps1 -BackupFile personal_fitness_20261020_010044.dump

    Pull a specific backup instead of the most recent.

.EXAMPLE
    .\scripts\sync-prod-to-dev.ps1 -SkipPull

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

# Anchor paths to the repo root, regardless of current directory
$RepoRoot      = Split-Path -Parent $PSScriptRoot
$LocalDump     = Join-Path $RepoRoot 'prod-latest.dump'
$SanitizedDump = Join-Path $RepoRoot 'prod-sanitized.dump'
$LogFile       = Join-Path $PSScriptRoot 'sync-prod-to-dev.log'

# ---- Logging ----
Start-Transcript -Path $LogFile -Force | Out-Null

function Write-Step($m)  { Write-Host ''; Write-Host "==> $m" -ForegroundColor Cyan }
function Write-Info($m)  { Write-Host "    $m" }
function Write-Warn2($m) { Write-Host "    WARNING: $m" -ForegroundColor Yellow }
function Write-Ok($m)    { Write-Host "    $m" -ForegroundColor Green }

function Invoke-Psql {
    param([string]$Database, [string]$Sql, [switch]$IgnoreExit)
    $a = @('exec','-i',$ContainerName,'psql','-U',$LocalUser,'-d',$Database,'-v','ON_ERROR_STOP=1','-c',$Sql)
    & docker @a
    $rc = $LASTEXITCODE
    if (-not $IgnoreExit -and $rc -ne 0) { throw "psql failed (exit $rc): $Sql" }
    return $rc
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

Write-Info 'Checking Docker daemon'
$di = & docker info 2>&1
if ($LASTEXITCODE -ne 0 -or ($di -join "`n") -match 'failed to connect') {
    throw 'Docker daemon is not running. Start Docker Desktop and retry.'
}
Write-Ok 'Docker daemon reachable'

Write-Info "Checking container $ContainerName is running"
$running = & docker ps --filter "name=$ContainerName" --filter 'status=running' --format '{{.Names}}'
if ($running -notcontains $ContainerName) {
    throw "Container '$ContainerName' is not running. Run 'docker compose up -d' and retry."
}
Write-Ok "Container $ContainerName running"

Write-Info 'Checking container PostgreSQL version'
$v = (& docker exec -i $ContainerName psql -U $LocalUser -d $LocalAdminDb -t -c 'SHOW server_version').Trim()
Write-Info "Container version: $v"
if ($v -notmatch '^18\.') { Write-Warn2 "Container is not PostgreSQL 18.x. Restore may fail." }

Write-Info 'Checking client tools on PATH'
$missing = @()
foreach ($t in @('psql','pg_dump','pg_restore','scp','ssh')) {
    if (-not (Get-Command $t -ErrorAction SilentlyContinue)) { $missing += $t }
}
if ($missing.Count -gt 0) { throw "Missing tools on PATH: $($missing -join ', ')" }
Write-Ok 'psql, pg_dump, pg_restore, scp, ssh all present'

# ============================== Step 1: Pull ==============================
if ($SkipPull) {
    Write-Step 'Step 1: Skip pull (using existing local dump)'
    if (-not (Test-Path -LiteralPath $LocalDump)) { throw "SkipPull specified but $LocalDump does not exist." }
    $size = (Get-Item $LocalDump).Length
    $age  = (Get-Date) - (Get-Item $LocalDump).LastWriteTime
    Write-Info "Using existing: $LocalDump"
    Write-Info "Size: $size bytes"
    Write-Info "Age:  $($age.ToString('d\d\ h\h\ m\m'))"
    if ($age.TotalHours -gt 48) { Write-Warn2 'Dump is more than 48 hours old.' }
} else {
    Write-Step 'Step 1: Pull latest backup from Pi'

    if (-not $BackupFile) {
        Write-Info "Finding most recent backup in $RemoteBackupDir"
        $BackupFile = (& ssh $SshHost "ls -t $RemoteBackupDir/*.dump 2>/dev/null | head -1").Trim()
        if (-not $BackupFile) { throw "No .dump files found in $RemoteBackupDir on $SshHost" }
        Write-Info "Most recent: $BackupFile"
    } else {
        if ($BackupFile -notmatch '/') { $BackupFile = "$RemoteBackupDir/$BackupFile" }
        Write-Info "Using specified backup: $BackupFile"
    }

    $remoteSize = (& ssh $SshHost "stat -c %s '$BackupFile' 2>/dev/null").Trim()
    if (-not $remoteSize) { throw "Backup '$BackupFile' not found on $SshHost" }
    Write-Info "Remote size: $remoteSize bytes"

    Write-Info 'Downloading (this may take several minutes)'
    & scp "${SshHost}:${BackupFile}" $LocalDump
    if ($LASTEXITCODE -ne 0) { throw 'scp failed' }

    $localSize = (Get-Item $LocalDump).Length
    if ($localSize -ne [long]$remoteSize) {
        throw "Size mismatch: remote $remoteSize, local $localSize. Transfer truncated."
    }
    Write-Ok "Download complete: $localSize bytes"
}

# ============================== Step 2: Verify ==============================
Write-Step 'Step 2: Verify dump integrity'
$header = & pg_restore --list $LocalDump 2>&1
if ($LASTEXITCODE -ne 0) { throw 'pg_restore --list failed; dump may be corrupt' }

$fromLine = ($header | Select-String 'Dumped from database version:').ToString()
Write-Info $fromLine.Trim()
if ($fromLine -notmatch '18\.6') {
    Write-Warn2 'Dump was produced by a PostgreSQL version other than 18.6. Restore may fail.'
} else {
    Write-Ok 'Dump is PG 18.6 custom-format archive'
}

# ============================== Step 3: Restore staging ==============================
Write-Step 'Step 3: Restore raw dump into staging'
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
    Invoke-Psql -Database $StagingDb -Sql 'TRUNCATE _migration.api_tokens, _migration.spotify_auth_metadata' | Out-Null

    Write-Info 'Verifying tables are empty'
    Invoke-Psql -Database $StagingDb -Sql @'
SELECT 'api_tokens' AS t, count(*) FROM _migration.api_tokens
UNION ALL SELECT 'spotify_auth_metadata', count(*) FROM _migration.spotify_auth_metadata
'@
    Write-Ok 'OAuth tables truncated'
}

# ============================== Step 5: Dump sanitized ==============================
Write-Step 'Step 5: Dump sanitized staging'
& docker exec -i $ContainerName pg_dump -U $LocalUser -d $StagingDb -Fc -f /tmp/prod-sanitized.dump
if ($LASTEXITCODE -ne 0) { throw 'pg_dump of sanitized staging failed' }

& docker cp "${ContainerName}:/tmp/prod-sanitized.dump" $SanitizedDump
if ($LASTEXITCODE -ne 0) { throw 'docker cp of sanitized dump failed' }

Write-Ok "Sanitized dump: $SanitizedDump ($((Get-Item $SanitizedDump).Length) bytes)"

# ============================== Step 6: Restore dev ==============================
Write-Step 'Step 6: Restore sanitized dump into dev'
Reset-Database -Database $DevDb

Write-Info 'Copying sanitized dump into container'
& docker cp $SanitizedDump "${ContainerName}:/tmp/prod-sanitized.dump"
if ($LASTEXITCODE -ne 0) { throw 'docker cp of sanitized dump into container failed' }

Write-Info "Restoring into $DevDb (several minutes)"
& docker exec -i $ContainerName pg_restore -U $LocalUser -d $DevDb --no-owner --no-privileges /tmp/prod-sanitized.dump
if ($LASTEXITCODE -ne 0) { throw 'pg_restore into dev failed' }
Write-Ok 'Dev restore complete'

# ============================== Step 7: Verify ==============================
Write-Step 'Step 7: Verify dev database'
Invoke-Psql -Database $DevDb -Sql @'
SELECT 'tables' AS metric, count(*)::text AS value FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL SELECT 'views', count(*)::text FROM pg_views WHERE schemaname NOT IN ('pg_catalog','information_schema')
UNION ALL SELECT 'api_tokens', count(*)::text FROM _migration.api_tokens
UNION ALL SELECT 'spotify_auth_metadata', count(*)::text FROM _migration.spotify_auth_metadata
'@

# ============================== Step 8: Cleanup ==============================
Write-Step 'Step 8: Cleanup'

Write-Info "Dropping $StagingDb"
Invoke-Psql -Database $LocalAdminDb -Sql "DROP DATABASE IF EXISTS $StagingDb WITH (FORCE)" | Out-Null

Write-Info 'Removing container temp files'
& docker exec -i $ContainerName rm -f /tmp/prod-latest.dump /tmp/prod-sanitized.dump

if ($DeleteDumps) {
    Write-Info 'Deleting local dumps'
    Remove-Item $LocalDump, $SanitizedDump -ErrorAction SilentlyContinue
} else {
    Write-Info 'Keeping local dumps for faster re-runs:'
    Write-Info "  $LocalDump"
    Write-Info "  $SanitizedDump"
    Write-Info 'Pass -DeleteDumps to remove them.'
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