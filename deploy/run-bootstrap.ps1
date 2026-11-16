<#
.SYNOPSIS
    Runs the PiFitness bootstrap shell on the Pi via SSH and automatically pulls the evergreen log on completion.
.DESCRIPTION
    Opens an interactive SSH session to execute /home/god/pifitness-bootstrap/pi_bootstrap_shell.sh,
    streaming progress directly in the local PowerShell terminal. When the session finishes (pass or fail),
    it automatically pulls the evergreen log (/home/god/pifitness-logs/bootstrap.log) to deploy/bootstrap.log.
.PARAMETER HostName
    Target host (default: pfpi).
.PARAMETER User
    SSH user (default: god).
.PARAMETER RemoteLauncher
    Path to the launcher shell script on the Pi (default: /home/god/pifitness-bootstrap/pi_bootstrap_shell.sh).
.PARAMETER RemoteLogPath
    Path to the evergreen log on the Pi (default: /home/god/pifitness-logs/bootstrap.log).
.PARAMETER LocalPath
    Destination file path for the pulled log (default: deploy/bootstrap.log).
#>
[CmdletBinding()]
param(
    [string]$HostName = "pfpi",
    [string]$User = "god",
    [string]$RemoteLauncher = "/home/god/pifitness-bootstrap/pi_bootstrap_shell.sh",
    [string]$RemoteLogPath = "/home/god/pifitness-logs/bootstrap.log",
    [string]$LocalPath = "deploy\bootstrap.log"
)

$target = "$User@$HostName"

Write-Host "=== PiFitness Bootstrap Runner ===" -ForegroundColor Cyan
Write-Host "Target:   $target"
Write-Host "Launcher: $RemoteLauncher"
Write-Host "Log Dest: $LocalPath"
Write-Host "Starting SSH session with terminal allocation (-t)...`n" -ForegroundColor DarkGray

# Launch interactive SSH session with -t (pseudo-terminal allocation) so prompts and menus work seamlessly
ssh -t "$target" "bash $RemoteLauncher"
$sessionExitCode = $LASTEXITCODE

Write-Host "`n=== Bootstrap Session Finished (Exit Code: $sessionExitCode) ===" -ForegroundColor Cyan
Write-Host "Pulling evergreen log: $target`:$RemoteLogPath -> $LocalPath ..." -ForegroundColor Cyan

scp "$target`:$RemoteLogPath" "$LocalPath"

if ($LASTEXITCODE -eq 0) {
    Write-Host "Evergreen log successfully updated at $LocalPath" -ForegroundColor Green
} else {
    Write-Warning "Could not pull bootstrap log from $target (scp exited with code $LASTEXITCODE)."
}

exit $sessionExitCode
