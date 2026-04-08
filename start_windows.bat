@echo off
setlocal

if not exist .venv (
  py -m venv .venv
)

call .venv\Scripts\activate
pip install -r requirements.txt
python scripts\init_db.py
python run.py
