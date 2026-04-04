from __future__ import annotations

import os
from pathlib import Path


def _default_data_root() -> Path:
    project_root = Path(__file__).resolve().parents[1]
    project_data = project_root / ".canyu_data"
    # Prefer repo-local data dir so different terminals use the same database by default.
    return project_data


DATA_ROOT = Path(os.environ.get("CANYU_DATA_DIR", _default_data_root()))
DB_PATH = Path(os.environ.get("CANYU_DB_PATH", DATA_ROOT / "shipping.db"))
BACKUP_DIR = Path(os.environ.get("CANYU_BACKUP_DIR", DATA_ROOT / "backups"))
IMPORT_UPLOAD_DIR = Path(os.environ.get("CANYU_IMPORT_UPLOAD_DIR", DATA_ROOT / "imports"))
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schema" / "schema.sql"
SECRET_KEY = os.environ.get("CANYU_SECRET_KEY", "change-this-in-production")


def ensure_dirs() -> None:
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    IMPORT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
