@echo off
title PiFitness DevHub Server
cd /d "%~dp0"
echo Starting PiFitness DevHub...
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" tools\workflow\dev_hub.py
) else (
    python tools\workflow\dev_hub.py
)
pause
