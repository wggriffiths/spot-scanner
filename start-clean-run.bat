@echo off
setlocal EnableExtensions EnableDelayedExpansion
title Spot Scanner

REM --------------------------------------------------
REM Start server
REM --------------------------------------------------
echo.
echo ========================================
echo   Starting Server
echo ========================================
echo.
echo Open: http://localhost:8000
echo Press Ctrl+C to stop
echo.

deno run --allow-net --allow-run=cmd.exe,powershell.exe --allow-read --allow-write=alerts_history.json --allow-write=performance_log.json scanner.ts

if errorlevel 1 (
    echo.
    echo [!] Server stopped with error
    pause
)
