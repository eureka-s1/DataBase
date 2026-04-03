from __future__ import annotations

import sqlite3
from pathlib import Path

from .common import now_ts


def backup_sqlite(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts_name = now_ts().replace(':', '').replace('-', '').replace(' ', '_')
    out = backup_dir / f'shipping_backup_{ts_name}.db'

    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(out)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return out
