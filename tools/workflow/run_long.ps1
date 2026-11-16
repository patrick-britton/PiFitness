<#
.SYNOPSIS
Starts a long-running command detached from the caller and returns immediately.

.DESCRIPTION
Each run_commands call is a separate PowerShell process with a 30s hard limit,
so long work must outlive the call. This script launches the command in a
hidden PowerShell process, captures stdout and stderr to files, and writes the
command's exit code to exit.txt when it finishes. Check progress with
poll_long.ps1. Job files live in $env:TEMP\agent-jobs\<Name>\ (outside the repo).

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\run_long.ps1 -Name verify -Command 'C:\users\rickb\vsprojects\pifitness\.venv\Scripts\python.exe tools/verify.py --scope all'
#>
param(
    [string]$Name,
    [string]$Command,
    [string]$Preset,
    [string]$WorkingDirectory,
    [switch]$Worker,
    [string]$JobDir,
    [string]$WorkDir
)

$ErrorActionPreference = 'Stop'

# Worker mode: runs the saved command, then records its exit code.
if ($Worker) {
    $exitFile = Join-Path $JobDir 'exit.txt'
    $exitTmp  = Join-Path $JobDir 'exit.tmp'
    $code = 1
    try {
        Set-Location -LiteralPath $WorkDir
        $text = Get-Content -LiteralPath (Join-Path $JobDir 'command.txt') -Raw
        $sb = [scriptblock]::Create($text)
        $ErrorActionPreference = 'Continue'
        $global:LASTEXITCODE = $null
        & $sb
        $ok = $?
        $code = $LASTEXITCODE
        if ($null -eq $code) {
            if ($ok) { $code = 0 } else { $code = 1 }
        } elseif ($code -eq 0 -and -not $ok) {
            $code = 1
        }
    } catch {
        [Console]::Error.WriteLine($_.Exception.Message)
        $code = 1
    }
    Set-Content -LiteralPath $exitTmp -Value $code -Encoding ASCII
    Move-Item -LiteralPath $exitTmp -Destination $exitFile -Force
    exit $code
}

# Launcher mode
if ($Preset) {
    $presetFile = Join-Path $PSScriptRoot 'long_commands.json'
    if (-not (Test-Path -LiteralPath $presetFile)) {
        Write-Output "ERROR: missing $presetFile"
        exit 2
    }
    $presets = Get-Content -LiteralPath $presetFile -Raw | ConvertFrom-Json
    $Command = $presets.$Preset
    if (-not $Command) {
        Write-Output "ERROR: no preset named '$Preset' in long_commands.json"
        exit 2
    }
    if (-not $Name) { $Name = $Preset }
}
if (-not $Name -or -not $Command) {
    Write-Output 'USAGE: run_long.ps1 -Preset <name> | -Name <job> -Command <command line> [-WorkingDirectory <dir>]'
    exit 2
}
if ($Name -notmatch '^[A-Za-z0-9_.-]+$') {
    Write-Output 'ERROR: -Name may contain only letters, digits, underscore, dot, hyphen.'
    exit 2
}
if (-not $WorkingDirectory) {
    $WorkingDirectory = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}
$WorkingDirectory = $WorkingDirectory.TrimEnd('\')

$root   = Join-Path $env:TEMP 'agent-jobs'
$jobDir = Join-Path $root $Name

if (Test-Path -LiteralPath $jobDir) {
    $pidFile  = Join-Path $jobDir 'pid.txt'
    $exitFile = Join-Path $jobDir 'exit.txt'
    if ((Test-Path -LiteralPath $pidFile) -and -not (Test-Path -LiteralPath $exitFile)) {
        $oldPid = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
        $proc = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
        if ($proc -and $proc.ProcessName -eq 'powershell') {
            Write-Output "ALREADY RUNNING: job '$Name' (pid $oldPid). Poll it, or stop it with poll_long.ps1 -Name $Name -Stop."
            exit 2
        }
    }
    Remove-Item -LiteralPath $jobDir -Recurse -Force
}
New-Item -ItemType Directory -Path $jobDir -Force | Out-Null

Set-Content -LiteralPath (Join-Path $jobDir 'command.txt') -Value $Command -Encoding UTF8
Set-Content -LiteralPath (Join-Path $jobDir 'start.txt') -Value (Get-Date -Format o) -Encoding ASCII

$argString = '-NoProfile -ExecutionPolicy Bypass -File "{0}" -Worker -JobDir "{1}" -WorkDir "{2}"' -f $PSCommandPath, $jobDir, $WorkingDirectory
$p = Start-Process -FilePath 'powershell.exe' `
    -ArgumentList $argString `
    -WorkingDirectory $WorkingDirectory `
    -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $jobDir 'out.txt') `
    -RedirectStandardError (Join-Path $jobDir 'err.txt') `
    -PassThru
Set-Content -LiteralPath (Join-Path $jobDir 'pid.txt') -Value $p.Id -Encoding ASCII

Write-Output "STARTED job=$Name pid=$($p.Id)"
Write-Output "POLL: powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\poll_long.ps1 -Name $Name"
exit 0