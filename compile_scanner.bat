@echo off
deno compile --allow-net --allow-run=cmd.exe,powershell.exe,sc.exe,net --allow-read --allow-write --allow-ffi --include dashboard.html --output scanner scanner.ts
pause
