from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .config import DB_PATH, SCHEMA_PATH, ensure_dirs
from .services.auth import ensure_default_admin
from .services.common import now_ts


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    ensure_dirs()
    target = db_path or DB_PATH
    conn = sqlite3.connect(target)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def db_session(db_path: Path | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path | None = None) -> None:
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with db_session(db_path) as conn:
        conn.executescript(sql)
        ensure_default_admin(conn)
        row = conn.execute("SELECT id FROM warehouses LIMIT 1").fetchone()
        if not row:
            conn.execute(
                "INSERT INTO warehouses(name, location, is_active) VALUES (?, ?, 1)",
                ("Main Warehouse", "Default"),
            )
        # lightweight schema migration for existing DBs
        cols = {str(r["name"]) for r in conn.execute("PRAGMA table_info(containers)").fetchall()}
        if "master_customer_id" not in cols:
            conn.execute("ALTER TABLE containers ADD COLUMN master_customer_id INTEGER")
