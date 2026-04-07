@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================================================
REM DataBase one-click downloader/updater/runner (Windows)
REM Distribute this file only, then users can sync and run.
REM
REM Usage:
REM   更新.bat                 sync + start
REM   更新.bat --sync-only     sync only
REM   更新.bat --start-only    start only
REM =========================================================

set "REPO_URL=https://github.com/eureka-s1/DataBase"
set "ZIP_URL=https://github.com/eureka-s1/DataBase/archive/refs/heads/main.zip"
set "ZIP_URL_ALT=https://codeload.github.com/eureka-s1/DataBase/zip/refs/heads/main"
set "REPO_DIR_NAME=DataBase"

set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "PROJECT_DIR="
set "LOG_FILE=%SCRIPT_DIR%\更新.log"
set "MODE=sync_start"

echo.>"%LOG_FILE%"
echo [%DATE% %TIME%] start>>"%LOG_FILE%"

if /I "%~1"=="--sync-only" set "MODE=sync_only"
if /I "%~1"=="--start-only" set "MODE=start_only"

if not "%MODE%"=="sync_start" if not "%MODE%"=="sync_only" if not "%MODE%"=="start_only" (
  echo [ERROR] Unsupported argument: %~1
  echo [ERROR] Unsupported argument: %~1>>"%LOG_FILE%"
  call :finish 2
)

REM If current folder already looks like project root, use it directly.
if exist "%SCRIPT_DIR%\run.py" (
  set "PROJECT_DIR=%SCRIPT_DIR%"
) else if exist "%SCRIPT_DIR%\.git" (
  set "PROJECT_DIR=%SCRIPT_DIR%"
) else (
  set "PROJECT_DIR=%SCRIPT_DIR%\%REPO_DIR_NAME%"
)

echo.
echo [INFO] Mode: %MODE%
echo [INFO] Repo: %REPO_URL%
echo [INFO] Target: %PROJECT_DIR%
echo [INFO] Log: %LOG_FILE%
echo.

if "%MODE%"=="start_only" goto :start_app
call :sync_with_zip

if not "%ERRORLEVEL%"=="0" (
  echo.
  echo [ERROR] Update failed. Check network or permissions.
  echo [ERROR] Update failed.>>"%LOG_FILE%"
  call :finish 1
)

echo.
echo [OK] Project is up to date.

call :resolve_project_dir
if not "%ERRORLEVEL%"=="0" (
  echo [ERROR] Cannot locate project root (run.py).
  echo [ERROR] cannot locate project root>>"%LOG_FILE%"
  call :finish 9
)

if "%MODE%"=="sync_only" call :finish 0

:start_app
if not exist "%PROJECT_DIR%\run.py" (
  echo [ERROR] run.py not found in %PROJECT_DIR%.
  echo [ERROR] run.py missing>>"%LOG_FILE%"
  call :finish 10
)

echo [INFO] Starting app (inline bootstrap)...
echo [INFO] inline bootstrap start>>"%LOG_FILE%"
pushd "%PROJECT_DIR%" >nul

where py >nul 2>nul
if errorlevel 1 (
  where python >nul 2>nul
  if errorlevel 1 (
    popd >nul
    echo [ERROR] Python launcher not found (py/python).
    echo [ERROR] python launcher missing>>"%LOG_FILE%"
    call :finish 12
  ) else (
    set "PY_CMD=python"
  )
) else (
  set "PY_CMD=py"
)

if not exist ".venv\Scripts\python.exe" (
  echo [INFO] Creating virtual environment...
  %PY_CMD% -m venv .venv >>"%LOG_FILE%" 2>&1
  if errorlevel 1 (
    set "START_RC=%ERRORLEVEL%"
    popd >nul
    echo [ERROR] Failed to create virtual environment. rc=%START_RC%
    echo [ERROR] venv create failed rc=%START_RC%>>"%LOG_FILE%"
    call :finish %START_RC%
  )
)

echo [INFO] Installing dependencies...
".venv\Scripts\python.exe" -m pip install -r requirements.txt >>"%LOG_FILE%" 2>&1
if errorlevel 1 (
  set "START_RC=%ERRORLEVEL%"
  popd >nul
  echo [ERROR] Dependency installation failed. rc=%START_RC%
  echo [ERROR] pip install failed rc=%START_RC%>>"%LOG_FILE%"
  call :finish %START_RC%
)

echo [INFO] Initializing database...
".venv\Scripts\python.exe" scripts\init_db.py >>"%LOG_FILE%" 2>&1
if errorlevel 1 (
  set "START_RC=%ERRORLEVEL%"
  popd >nul
  echo [ERROR] Database initialization failed. rc=%START_RC%
  echo [ERROR] init_db failed rc=%START_RC%>>"%LOG_FILE%"
  call :finish %START_RC%
)

echo [INFO] Launching web server...
echo [INFO] open http://127.0.0.1:5000/login
".venv\Scripts\python.exe" run.py
set "START_RC=%ERRORLEVEL%"
popd >nul
if not "%START_RC%"=="0" (
  echo [ERROR] run.py exited with code %START_RC%.
  echo [ERROR] run.py failed rc=%START_RC%>>"%LOG_FILE%"
  call :finish %START_RC%
)
call :finish 0

:sync_with_zip
echo [INFO] Syncing from GitHub ZIP (git-free mode).
echo [INFO] sync_with_zip>>"%LOG_FILE%"

set "TMP_ROOT=%TEMP%\canyu_sync_%RANDOM%_%RANDOM%"
set "ZIP_FILE=%TMP_ROOT%\repo.zip"
set "UNZIP_DIR=%TMP_ROOT%\unzipped"
set "SRC_DIR=%UNZIP_DIR%\DataBase-main"

mkdir "%TMP_ROOT%" >nul 2>nul
mkdir "%UNZIP_DIR%" >nul 2>nul

powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri '%ZIP_URL%' -OutFile '%ZIP_FILE%'" >>"%LOG_FILE%" 2>&1
if errorlevel 1 (
  echo [WARN] Primary ZIP URL failed. Trying codeload URL...
  echo [WARN] zip primary failed, trying alt>>"%LOG_FILE%"
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri '%ZIP_URL_ALT%' -OutFile '%ZIP_FILE%'" >>"%LOG_FILE%" 2>&1
  if errorlevel 1 exit /b 5
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -LiteralPath '%ZIP_FILE%' -DestinationPath '%UNZIP_DIR%' -Force" >>"%LOG_FILE%" 2>&1
if errorlevel 1 exit /b 6

if not exist "%SRC_DIR%" exit /b 7
if not exist "%PROJECT_DIR%" mkdir "%PROJECT_DIR%"

echo [INFO] Copying files (preserve local data and runtime folders)...
robocopy "%SRC_DIR%" "%PROJECT_DIR%" /E /R:1 /W:1 /NFL /NDL /NP /NJH /NJS ^
  /XD ".git" ".venv" ".canyu_data" "backups" "exports" "dist" "__pycache__" ^
  /XF ".env" "shipping.db" "update_from_github.log" >>"%LOG_FILE%" 2>&1

set "RC=%ERRORLEVEL%"
if %RC% GEQ 8 exit /b 8

rmdir /S /Q "%TMP_ROOT%" >nul 2>nul
exit /b 0

:resolve_project_dir
if exist "%PROJECT_DIR%\run.py" exit /b 0

if exist "%SCRIPT_DIR%\run.py" (
  set "PROJECT_DIR=%SCRIPT_DIR%"
  exit /b 0
)

if exist "%PROJECT_DIR%\%REPO_DIR_NAME%\run.py" (
  set "PROJECT_DIR=%PROJECT_DIR%\%REPO_DIR_NAME%"
  exit /b 0
)

for /d %%D in ("%SCRIPT_DIR%\*") do (
  if exist "%%~fD\run.py" (
    set "PROJECT_DIR=%%~fD"
    exit /b 0
  )
)

exit /b 9

:finish
set "RC=%~1"
if "%RC%"=="" set "RC=0"
echo.
echo [INFO] Log file: %LOG_FILE%
echo [INFO] Script finished. Press any key to close.
pause >nul
exit /b %RC%
