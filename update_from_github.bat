@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM =========================================================
REM DataBase 一键下载 / 一键更新 / 一键启动（Windows）
REM - 首次运行：自动拉取项目代码
REM - 后续运行：自动更新到 GitHub 最新 main
REM - 更新完成后：默认自动启动 start_windows.bat
REM
REM 用法：
REM   双击运行：更新并启动
REM   update_from_github.bat --sync-only   仅更新，不启动
REM =========================================================

set "REPO_URL=https://github.com/eureka-s1/DataBase.git"
set "ZIP_URL=https://github.com/eureka-s1/DataBase/archive/refs/heads/main.zip"
set "REPO_DIR_NAME=DataBase"
set "BRANCH=main"

set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "PROJECT_DIR="

if exist "%SCRIPT_DIR%\.git" (
  set "PROJECT_DIR=%SCRIPT_DIR%"
) else (
  set "PROJECT_DIR=%SCRIPT_DIR%\%REPO_DIR_NAME%"
)

set "SYNC_ONLY=0"
if /I "%~1"=="--sync-only" set "SYNC_ONLY=1"

echo.
echo [INFO] Repo: %REPO_URL%
echo [INFO] Target: %PROJECT_DIR%
echo.

where git >nul 2>nul
if %ERRORLEVEL%==0 (
  call :sync_with_git
) else (
  call :sync_with_zip
)

if not "%ERRORLEVEL%"=="0" (
  echo.
  echo [ERROR] 更新失败，请检查网络或权限。
  pause
  exit /b 1
)

echo.
echo [OK] 代码已是最新。

if "%SYNC_ONLY%"=="1" (
  echo [INFO] 已按 --sync-only 模式完成，仅更新不启动。
  pause
  exit /b 0
)

if exist "%PROJECT_DIR%\start_windows.bat" (
  echo [INFO] 准备启动系统...
  pushd "%PROJECT_DIR%" >nul
  call start_windows.bat
  popd >nul
) else (
  echo [WARN] 未找到 start_windows.bat，请手动检查项目目录。
  pause
)
exit /b 0

:sync_with_git
echo [INFO] 检测到 Git，使用 git clone / git pull 更新。

if exist "%PROJECT_DIR%\.git" (
  echo [INFO] 已存在仓库，正在拉取更新...
  git -C "%PROJECT_DIR%" fetch --all --prune
  if not "%ERRORLEVEL%"=="0" exit /b 2
  git -C "%PROJECT_DIR%" pull --ff-only origin %BRANCH%
  if not "%ERRORLEVEL%"=="0" (
    echo [WARN] pull --ff-only 失败，尝试普通 pull...
    git -C "%PROJECT_DIR%" pull origin %BRANCH%
    if not "%ERRORLEVEL%"=="0" exit /b 3
  )
) else (
  if not exist "%PROJECT_DIR%" mkdir "%PROJECT_DIR%"
  echo [INFO] 首次下载，正在克隆仓库...
  git clone --branch %BRANCH% --depth 1 "%REPO_URL%" "%PROJECT_DIR%"
  if not "%ERRORLEVEL%"=="0" exit /b 4
)
exit /b 0

:sync_with_zip
echo [INFO] 未检测到 Git，使用 ZIP 下载方式更新。
echo [INFO] 该模式不会保留 Git 历史，但可正常更新运行。

set "TMP_ROOT=%TEMP%\canyu_sync_%RANDOM%_%RANDOM%"
set "ZIP_FILE=%TMP_ROOT%\repo.zip"
set "UNZIP_DIR=%TMP_ROOT%\unzipped"
set "SRC_DIR=%UNZIP_DIR%\DataBase-main"

mkdir "%TMP_ROOT%" >nul 2>nul
mkdir "%UNZIP_DIR%" >nul 2>nul

powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri '%ZIP_URL%' -OutFile '%ZIP_FILE%'"
if not "%ERRORLEVEL%"=="0" exit /b 5

powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -LiteralPath '%ZIP_FILE%' -DestinationPath '%UNZIP_DIR%' -Force"
if not "%ERRORLEVEL%"=="0" exit /b 6

if not exist "%SRC_DIR%" exit /b 7
if not exist "%PROJECT_DIR%" mkdir "%PROJECT_DIR%"

echo [INFO] 正在覆盖更新文件（保留本地数据目录）...
robocopy "%SRC_DIR%" "%PROJECT_DIR%" /E /R:1 /W:1 /NFL /NDL /NP /NJH /NJS ^
  /XD ".git" ".venv" ".canyu_data" "backups" "exports" "dist" "__pycache__"

set "RC=%ERRORLEVEL%"
if %RC% GEQ 8 exit /b 8

rmdir /S /Q "%TMP_ROOT%" >nul 2>nul
exit /b 0
