<#
.SYNOPSIS
    Pulls the evergreen bootstrap log from the Raspberry Pi over SCP.
.DESCRIPTION
    Retrieves /home/god/pifitness-logs/bootstrap.log from the Pi to deploy/bootstrap.log.
.PARAMETER HostName
    Target host (default: pfpi).
.PARAMETER User
    SSH user (default: god).
.PARAMETER RemoteLogPath
    Remote log path on the Pi (default: /home/god/pifitness-logs/bootstrap.log).
.PARAMETER LocalPath
    Destination file path (default: deploy/bootstrap.log).
#>
[CmdletBinding()]
param(
    [string]$HostName = "pfpi",
    [string]$User = "god",
    [string]$RemoteLogPath = "/home/god/pifitness-logs/bootstrap.log",
    [string]$LocalPath = "deploy\bootstrap.log"
)

$target = "$User@$HostName"
Write-Host "Fetching evergreen bootstrap log from $target`:$RemoteLogPath -> $LocalPath ..." -ForegroundColor Cyan
scp "$target`:$RemoteLogPath" "$LocalPath"

if ($LASTEXITCODE -eq 0) {
    Write-Host "Bootstrap log successfully saved to $LocalPath" -ForegroundColor Green
} else {
    Write-Error "Failed to pull bootstrap log from $target (scp exited with code $LASTEXITCODE)."
}
