@echo off
setlocal
echo [ResilientBank] Launching full Docker stack...
if exist "C:\Program Files\Git\bin\bash.exe" (
    "C:\Program Files\Git\bin\bash.exe" "%~dp0scripts\docker-up.sh" %*
) else (
    bash "%~dp0scripts\docker-up.sh" %*
)
