@echo off
cd /d "%~dp0"
pwsh.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0Launch-AiTest.ps1" -Mode int8-directml
if errorlevel 1 pause
