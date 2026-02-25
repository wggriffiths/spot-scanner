@echo off
deno compile --allow-net --allow-run=cmd.exe,powershell.exe --allow-read --include dashboard.html --output scanner scanner.ts
pause
