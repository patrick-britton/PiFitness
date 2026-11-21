<#
.SYNOPSIS
    tools/dev_hub.ps1 - Launch or connect to the PiFitness Developer Hub.

.DESCRIPTION
    Checks if DevHub server is already running on localhost:8760.
    If already running, opens the browser to it.
    If not running, starts the DevHub in a dedicated background/new window
    (or in the foreground if -Foreground is specified) and opens the browser.

.EXAMPLE
    .\tools\dev_hub.ps1
    .\tools\dev_hub.ps1 -Foreground
#>

[CmdletBinding()]
param(
    [int]$Port = 8760,
    [switch]$NoBrowser,
    [switch]$Foreground
)

$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
$python = "$repoRoot\.venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    $python = "python"
}

# 1. Check if DevHub is already running on this port
$alreadyRunning = $false
try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $iar = $tcp.BeginConnect("127.0.0.1", $Port, $null, $null)
    $wait = $iar.AsyncWaitHandle.WaitOne(300, $false)
    if ($wait -and $tcp.Connected) {
        $tcp.EndConnect($iar)
        $alreadyRunning = $true
    }
    $tcp.Close()
} catch {}

if ($alreadyRunning) {
    Write-Host "PiFitness DevHub is already running at http://127.0.0.1:$Port/" -ForegroundColor Green
    if (-not $NoBrowser) {
        Start-Process "http://127.0.0.1:$Port/"
    }
    return
}

# 2. If not running and foreground not requested, launch in a dedicated console window
if (-not $Foreground) {
    Write-Host "Starting PiFitness DevHub in a dedicated window..." -ForegroundColor Cyan
    $devHubScript = "$PSScriptRoot\workflow\dev_hub.py"
    $cmd = "cd `"$repoRoot`"; & `"$python`" `"$devHubScript`" --port $Port"
    Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $cmd
    Start-Sleep -Milliseconds 800
    if (-not $NoBrowser) {
        Start-Process "http://127.0.0.1:$Port/"
    }
    return
}

# 3. Running in foreground (if requested)
Write-Host "Starting PiFitness DevHub server on port $Port..." -ForegroundColor Cyan
Set-Location $repoRoot
& $python "tools\workflow\dev_hub.py" --port $Port
