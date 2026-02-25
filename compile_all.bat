@echo off
REM Windows: binary names resolve on this machine, so we can be specific.
deno compile --allow-net --allow-run=cmd.exe,powershell.exe --allow-read --include dashboard.html --output scanner-windows-x64.exe --target x86_64-pc-windows-msvc scanner.ts

REM Linux / macOS: cross-compiled from Windows, so xdg-open / open cannot be
REM resolved here. --allow-run without arguments grants run permission at runtime.
deno compile --allow-net --allow-run --allow-read --include dashboard.html --output scanner-linux-x64   --target x86_64-unknown-linux-gnu scanner.ts
deno compile --allow-net --allow-run --allow-read --include dashboard.html --output scanner-macos-x64   --target x86_64-apple-darwin scanner.ts
deno compile --allow-net --allow-run --allow-read --include dashboard.html --output scanner-macos-arm64 --target aarch64-apple-darwin scanner.ts
pause
