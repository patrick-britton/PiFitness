@echo off
cd /d "%~dp0"

:: 1. Start DevHub server in background if not already running
set "PYTHON_EXE=.venv\Scripts\pythonw.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=pythonw.exe"

powershell -NoProfile -Command "try { $s = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 8760); $s.Close(); exit 0 } catch { exit 1 }"
if %ERRORLEVEL% neq 0 (
    start "" "%PYTHON_EXE%" tools\workflow\dev_hub.py --port 8760 --no-browser
    timeout /t 1 /nobreak >nul
)

:: 2. Launch browser in app mode (prefer Google Chrome)
set "APP_URL=http://127.0.0.1:8760/"
set "BROWSER_EXE="
if exist "%ProgramFiles%\Google\Chrome\Application\chrome.exe" set "BROWSER_EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if not defined BROWSER_EXE if exist "%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe" set "BROWSER_EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
if not defined BROWSER_EXE if exist "%LocalAppData%\Google\Chrome\Application\chrome.exe" set "BROWSER_EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"

if defined BROWSER_EXE (
    start "" "%BROWSER_EXE%" --app=%APP_URL%
) else (
    start "" %APP_URL%
)
exit /b 0
