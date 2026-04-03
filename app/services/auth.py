from __future__ import annotations

from sqlite3 import Connection
from werkzeug.security import check_password_hash, generate_password_hash

from .common import now_ts


def ensure_default_admin(conn: Connection, username: str = 'admin', password: str = 'admin123') -> None:
    row = conn.execute('SELECT id FROM users WHERE username=?', (username,)).fetchone()
    if row:
        return
    ts = now_ts()
    conn.execute(
        '''
        INSERT INTO users(username, password_hash, role, is_active, created_at, updated_at)
        VALUES (?, ?, 'admin', 1, ?, ?)
        ''',
        (username, generate_password_hash(password), ts, ts),
    )


def authenticate(conn: Connection, username: str, password: str) -> dict | None:
    row = conn.execute(
        'SELECT id, username, password_hash, role, is_active FROM users WHERE username=? LIMIT 1',
        (username,),
    ).fetchone()
    if not row or int(row['is_active']) != 1:
        return None
    if not check_password_hash(row['password_hash'], password):
        return None
    return {'id': int(row['id']), 'username': row['username'], 'role': row['role']}


def change_password(conn: Connection, user_id: int, new_password: str) -> None:
    conn.execute(
        'UPDATE users SET password_hash=?, updated_at=? WHERE id=?',
        (generate_password_hash(new_password), now_ts(), user_id),
    )
