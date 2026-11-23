<#
.SYNOPSIS
    Creates a Windows desktop shortcut for PiFitness DevHub that can be pinned to the taskbar.
.DESCRIPTION
    Generates a .lnk shortcut pointing to the windowless DevHub launcher.
    When opened, it starts the DevHub helper server (if not already running)
    and opens the DevHub in a standalone desktop window (--app mode).
.PARAMETER ShortcutPath
    Destination file path for the .lnk shortcut (default: Desktop\PiFitness DevHub.lnk).
#>
[CmdletBinding()]
param(
    [string]$ShortcutPath = [System.IO.Path]::Combine([System.Environment]::GetFolderPath([System.Environment+SpecialFolder]::Desktop), "PiFitness DevHub.lnk")
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path "$scriptDir\..").Path
$batPath = Join-Path $repoRoot "dev_hub.bat"

if (-not (Test-Path $batPath)) {
    throw "dev_hub.bat not found at $batPath"
}

# Determine browser icon location (prefer Chrome)
$iconLocation = ""
$chromePath = "$env:ProgramFiles\Google\Chrome\Application\chrome.exe"
if (-not (Test-Path $chromePath)) {
    $chromePath = "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe"
}
if (-not (Test-Path $chromePath)) {
    $chromePath = "$env:LocalAppData\Google\Chrome\Application\chrome.exe"
}
if (Test-Path $chromePath) {
    $iconLocation = "$chromePath,0"
} else {
    $iconLocation = "shell32.dll,220"
}

$wshShell = New-Object -ComObject WScript.Shell
$shortcut = $wshShell.CreateShortcut($ShortcutPath)

# Point to cmd.exe running dev_hub.bat so Windows allows pinning to the taskbar
$shortcut.TargetPath = "$env:ComSpec"
$shortcut.Arguments = "/c start `"`" `"$batPath`""
$shortcut.WorkingDirectory = $repoRoot
$shortcut.Description = "PiFitness Developer Hub"
$shortcut.IconLocation = $iconLocation
$shortcut.WindowStyle = 7 # Minimized to avoid console window popup

$shortcut.Save()

Write-Host "Created DevHub shortcut at: $ShortcutPath" -ForegroundColor Green
Write-Host "`nPIN TO TASKBAR INSTRUCTIONS:" -ForegroundColor Yellow
Write-Host "1. Locate 'PiFitness DevHub' on your Windows Desktop."
Write-Host "2. Right-click the shortcut -> select 'Pin to taskbar' (On Windows 11: select 'Show more options' -> 'Pin to taskbar')."
Write-Host "3. DevHub is now pinned to your taskbar! Click the taskbar icon anytime to open DevHub in standalone app mode.`n" -ForegroundColor Cyan
