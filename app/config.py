from __future__ import annotations

import os
from pathlib import Path


def _default_data_root() -> Path:
    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata) / "CanyuShipping"
    return Path.home() / ".canyu_shipping"


DATA_ROOT = Path(os.environ.get("CANYU_DATA_DIR", _default_data_root()))
DB_PATH = Path(os.environ.get("CANYU_DB_PATH", DATA_ROOT / "shipping.db"))
BACKUP_DIR = Path(os.environ.get("CANYU_BACKUP_DIR", DATA_ROOT / "backups"))
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schema" / "schema.sql"
SECRET_KEY = os.environ.get("CANYU_SECRET_KEY", "change-this-in-production")


def ensure_dirs() -> None:
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
