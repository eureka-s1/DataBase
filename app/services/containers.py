from __future__ import annotations

from sqlite3 import Connection

from .common import now_ts, to_float


def create_container(conn: Connection, payload: dict, user_id: int) -> int:
    ts = now_ts()
    cur = conn.execute(
        '''
        INSERT INTO containers(
          container_no, container_type, capacity_cbm, eta_date, status,
          price_mode, default_price_per_m3, remark, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'DRAFT', ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['container_no'],
            payload.get('container_type', '40HQ'),
            to_float(payload.get('capacity_cbm'), 68.0),
            payload.get('eta_date'),
            payload.get('price_mode', 'BY_CUSTOMER_RULE'),
            payload.get('default_price_per_m3'),
            payload.get('remark'),
            user_id,
            ts,
            ts,
        ),
    )
    return int(cur.lastrowid)


def container_usage(conn: Connection, container_id: int) -> dict:
    row = conn.execute(
        '''
        SELECT c.id, c.capacity_cbm,
               COALESCE(SUM(ci.cbm_at_load), 0) AS used_cbm
        FROM containers c
        LEFT JOIN container_items ci ON ci.container_id = c.id
        WHERE c.id = ?
        GROUP BY c.id
        ''',
        (container_id,),
    ).fetchone()
    if not row:
        raise ValueError('container not found')
    capacity = float(row['capacity_cbm'])
    used = float(row['used_cbm'])
    return {
        'container_id': int(row['id']),
        'capacity_cbm': capacity,
        'used_cbm': round(used, 6),
        'remain_cbm': round(capacity - used, 6),
    }


def add_item_to_container(conn: Connection, container_id: int, inbound_item_id: int,
                          cbm_override_at_load: float | None = None) -> None:
    c = conn.execute('SELECT id, status FROM containers WHERE id=?', (container_id,)).fetchone()
    if not c:
        raise ValueError('container not found')
    if c['status'] != 'DRAFT':
        raise ValueError('container is not editable')

    i = conn.execute(
        '''
        SELECT id, status, COALESCE(cbm_override, cbm_calculated) AS cbm_final
        FROM inbound_items
        WHERE id=?
        ''',
        (inbound_item_id,),
    ).fetchone()
    if not i:
        raise ValueError('inbound item not found')
    if i['status'] != 'IN_STOCK':
        raise ValueError('inbound item is not in stock')

    cbm = to_float(cbm_override_at_load, to_float(i['cbm_final']))
    usage = container_usage(conn, container_id)
    if usage['used_cbm'] + cbm > usage['capacity_cbm'] + 1e-9:
        raise ValueError('container capacity exceeded')

    ts = now_ts()
    conn.execute(
        'INSERT INTO container_items(container_id, inbound_item_id, cbm_at_load, created_at) VALUES (?, ?, ?, ?)',
        (container_id, inbound_item_id, cbm, ts),
    )
    conn.execute(
        "UPDATE inbound_items SET status='ALLOCATED', container_id=?, updated_at=? WHERE id=?",
        (container_id, ts, inbound_item_id),
    )


def remove_item_from_container(conn: Connection, container_id: int, inbound_item_id: int) -> int:
    c = conn.execute('SELECT id, status FROM containers WHERE id=?', (container_id,)).fetchone()
    if not c:
        raise ValueError('container not found')
    if c['status'] != 'DRAFT':
        raise ValueError('container is not editable')

    cur = conn.execute('DELETE FROM container_items WHERE container_id=? AND inbound_item_id=?', (container_id, inbound_item_id))
    if cur.rowcount > 0:
        conn.execute(
            "UPDATE inbound_items SET status='IN_STOCK', container_id=NULL, updated_at=? WHERE id=?",
            (now_ts(), inbound_item_id),
        )
    return cur.rowcount


def confirm_container(conn: Connection, container_id: int) -> None:
    c = conn.execute('SELECT status FROM containers WHERE id=?', (container_id,)).fetchone()
    if not c:
        raise ValueError('container not found')
    if c['status'] != 'DRAFT':
        raise ValueError('container status must be DRAFT')

    ts = now_ts()
    conn.execute(
        "UPDATE containers SET status='CONFIRMED', confirmed_at=?, updated_at=? WHERE id=?",
        (ts, ts, container_id),
    )
    conn.execute(
        "UPDATE inbound_items SET status='SHIPPED', updated_at=? WHERE container_id=?",
        (ts, container_id),
    )


def revoke_container(conn: Connection, container_id: int) -> None:
    c = conn.execute('SELECT status FROM containers WHERE id=?', (container_id,)).fetchone()
    if not c:
        raise ValueError('container not found')
    if c['status'] != 'CONFIRMED':
        raise ValueError('container status must be CONFIRMED')

    ts = now_ts()
    conn.execute(
        "UPDATE containers SET status='REVOKED', revoked_at=?, updated_at=? WHERE id=?",
        (ts, ts, container_id),
    )
    conn.execute(
        "UPDATE inbound_items SET status='IN_STOCK', container_id=NULL, updated_at=? WHERE container_id=?",
        (ts, container_id),
    )
    conn.execute('DELETE FROM container_items WHERE container_id=?', (container_id,))


def list_containers(conn: Connection) -> list[dict]:
    rows = conn.execute(
        '''
        SELECT c.*,
               COALESCE(SUM(ci.cbm_at_load), 0) AS used_cbm,
               COUNT(ci.id) AS item_count
        FROM containers c
        LEFT JOIN container_items ci ON ci.container_id = c.id
        GROUP BY c.id
        ORDER BY c.id DESC
        '''
    ).fetchall()
    return [dict(row) for row in rows]
