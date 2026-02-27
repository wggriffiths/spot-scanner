@echo off
REM Windows: can resolve binary names on this machine.
deno compile --allow-net --allow-run=cmd.exe,powershell.exe,sc.exe,net --allow-read --allow-write --allow-ffi --include dashboard.html --output scanner-windows-x64.exe --target x86_64-pc-windows-msvc scanner.ts

REM Linux / macOS: cross-compiled from Windows; xdg-open / open / systemctl cannot be
REM resolved here so --allow-run is unrestricted for these targets.
deno compile --allow-net --allow-run --allow-read --allow-write --include dashboard.html --output scanner-linux-x64   --target x86_64-unknown-linux-gnu scanner.ts
deno compile --allow-net --allow-run --allow-read --allow-write --include dashboard.html --output scanner-macos-x64   --target x86_64-apple-darwin scanner.ts
deno compile --allow-net --allow-run --allow-read --allow-write --include dashboard.html --output scanner-macos-arm64 --target aarch64-apple-darwin scanner.ts
pause
