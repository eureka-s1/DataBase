from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import datetime

from .common import now_ts


def backup_sqlite(db_path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts_name = now_ts().replace(':', '').replace('-', '').replace(' ', '_')
    out = backup_dir / f'{ts_name}.db'

    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(out)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return out


def list_backup_files(backup_dir: Path) -> list[dict]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    files = [p for p in backup_dir.glob("*.db") if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    out: list[dict] = []
    for p in files:
        st = p.stat()
        out.append(
            {
                "file_name": p.name,
                "file_path": str(p),
                "version": p.stem,
                "size_bytes": int(st.st_size),
                "modified_at": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return out


def restore_sqlite_from_backup(db_path: Path, backup_file: Path) -> None:
    if not backup_file.exists() or not backup_file.is_file():
        raise ValueError("backup file not found")
    if backup_file.suffix.lower() != ".db":
        raise ValueError("invalid backup file type")
    backup_resolved = backup_file.resolve()
    db_resolved = db_path.resolve()
    if backup_resolved == db_resolved:
        raise ValueError("cannot restore from current database file")
    src = sqlite3.connect(str(backup_file))
    dst = sqlite3.connect(str(db_path))
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
