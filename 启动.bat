@echo off
setlocal EnableExtensions

where py >nul 2>nul
if errorlevel 1 (
  where python >nul 2>nul
  if errorlevel 1 (
    echo [ERROR] Python launcher not found (py/python).
    pause
    exit /b 1
  ) else (
    set "PY_CMD=python"
  )
) else (
  set "PY_CMD=py"
)

if not exist .venv\Scripts\python.exe (
  %PY_CMD% -m venv .venv
  if errorlevel 1 (
    echo [ERROR] Failed to create virtual environment.
    pause
    exit /b 1
  )
)

.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 (
  echo [ERROR] Failed to install dependencies.
  pause
  exit /b 1
)

.venv\Scripts\python.exe scripts\init_db.py
if errorlevel 1 (
  echo [ERROR] Failed to initialize database.
  pause
  exit /b 1
)

.venv\Scripts\python.exe run.py
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" (
  echo [ERROR] run.py exited with code %RC%.
  pause
  exit /b %RC%
)
