<#
.SYNOPSIS
Reports the state of a job started by run_long.ps1.

.DESCRIPTION
Waits up to -WaitSec seconds (max 25, default 20) for the job to finish, then
prints STATE (RUNNING, FINISHED, DEAD), EXIT (exit code of the job's command),
elapsed time, and the last -Tail lines of stdout and stderr. This script always
exits 0; the job's own exit code is on the EXIT line.
DEAD means the worker process vanished without recording an exit code.

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\poll_long.ps1 -Name verify -Tail 20
powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\poll_long.ps1 -List
powershell -NoProfile -ExecutionPolicy Bypass -File tools\workflow\poll_long.ps1 -Name verify -Stop
#>
param(
    [string]$Name,
    [int]$WaitSec = 20,
    [int]$Tail = 20,
    [switch]$Stop,
    [switch]$List
)

$ErrorActionPreference = 'Stop'
$root = Join-Path $env:TEMP 'agent-jobs'

function Get-JobInfo([string]$dir) {
    $info = @{ State = 'DEAD'; ExitCode = $null; JobPid = $null }
    $exitFile = Join-Path $dir 'exit.txt'
    $pidFile  = Join-Path $dir 'pid.txt'

    if (Test-Path -LiteralPath $pidFile) {
        $info.JobPid = [int](Get-Content -LiteralPath $pidFile -Raw).Trim()
    }
    if (Test-Path -LiteralPath $exitFile) {
        $info.State = 'FINISHED'
        $info.ExitCode = (Get-Content -LiteralPath $exitFile -Raw).Trim()
        return $info
    }
    if ($info.JobPid) {
        $proc = Get-Process -Id $info.JobPid -ErrorAction SilentlyContinue
        if ($proc -and $proc.ProcessName -eq 'powershell') {
            $info.State = 'RUNNING'
            return $info
        }
    }
    # Worker may have exited between the two checks above.
    if (Test-Path -LiteralPath $exitFile) {
        $info.State = 'FINISHED'
        $info.ExitCode = (Get-Content -LiteralPath $exitFile -Raw).Trim()
    }
    return $info
}

if ($List) {
    if (-not (Test-Path -LiteralPath $root)) { Write-Output 'No jobs.'; exit 0 }
    foreach ($d in Get-ChildItem -LiteralPath $root -Directory) {
        $i = Get-JobInfo $d.FullName
        Write-Output ("{0}  STATE={1}  EXIT={2}" -f $d.Name, $i.State, $i.ExitCode)
    }
    exit 0
}

if (-not $Name) { Write-Output 'USAGE: poll_long.ps1 -Name <job> [-WaitSec 20] [-Tail 20] [-Stop] | -List'; exit 0 }
$dir = Join-Path $root $Name
if (-not (Test-Path -LiteralPath $dir)) { Write-Output "STATE: UNKNOWN (no job named '$Name')"; exit 0 }

if ($Stop) {
    $i = Get-JobInfo $dir
    if ($i.State -eq 'RUNNING') {
        & taskkill.exe /F /T /PID $i.JobPid | Out-Null
        Write-Output "STOPPED job=$Name pid=$($i.JobPid)"
        Start-Sleep -Seconds 1
    }
}

if ($WaitSec -gt 25) { $WaitSec = 25 }
if ($WaitSec -lt 0)  { $WaitSec = 0 }
$deadline = (Get-Date).AddSeconds($WaitSec)
$info = Get-JobInfo $dir
while ($info.State -eq 'RUNNING' -and (Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 1
    $info = Get-JobInfo $dir
}

$elapsed = ''
$startFile = Join-Path $dir 'start.txt'
if (Test-Path -LiteralPath $startFile) {
    $start = [datetime]::Parse((Get-Content -LiteralPath $startFile -Raw).Trim())
    $exitFile = Join-Path $dir 'exit.txt'
    if ($info.State -eq 'FINISHED' -and (Test-Path -LiteralPath $exitFile)) {
        $end = (Get-Item -LiteralPath $exitFile).LastWriteTime
    } else {
        $end = Get-Date
    }
    $elapsed = '{0}s' -f [int]($end - $start).TotalSeconds
}

Write-Output "JOB: $Name"
Write-Output "STATE: $($info.State)"
if ($info.State -eq 'FINISHED') { Write-Output "EXIT: $($info.ExitCode)" }
Write-Output "ELAPSED: $elapsed"
$cmdFile = Join-Path $dir 'command.txt'
if (Test-Path -LiteralPath $cmdFile) { Write-Output ("COMMAND: " + (Get-Content -LiteralPath $cmdFile -Raw).Trim()) }

foreach ($pair in @(@('stdout', 'out.txt'), @('stderr', 'err.txt'))) {
    $f = Join-Path $dir $pair[1]
    Write-Output "--- $($pair[0]) (last $Tail lines) ---"
    if (Test-Path -LiteralPath $f) {
        Get-Content -LiteralPath $f -Tail $Tail -ErrorAction SilentlyContinue
    }
}
exit 0