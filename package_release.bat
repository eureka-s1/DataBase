@echo off
setlocal

if not exist .venv (
  py -m venv .venv
)

call .venv\Scripts\activate
pip install -r requirements.txt
python scripts\package_release.py %*

echo.
echo Package completed. Check the dist folder.
pause
