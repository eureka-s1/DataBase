@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
call "%SCRIPT_DIR%update_from_github.bat" %*
endlocal
