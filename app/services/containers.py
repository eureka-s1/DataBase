from __future__ import annotations

from sqlite3 import Connection

from .common import now_ts, to_float


def create_container(conn: Connection, payload: dict, user_id: int) -> int:
    ts = now_ts()
    capacity_cbm = to_float(payload.get('capacity_cbm'), 68.0)
    default_price_per_m3 = payload.get('default_price_per_m3')
    if default_price_per_m3 is None:
        default_price_per_m3 = round(6100.0 / 68.0, 6)
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
            capacity_cbm,
            payload.get('eta_date'),
            payload.get('price_mode', 'BY_CUSTOMER_RULE'),
            to_float(default_price_per_m3),
            payload.get('remark'),
            user_id,
            ts,
            ts,
        ),
    )
    return int(cur.lastrowid)


def update_container_no(conn: Connection, container_id: int, container_no: str) -> None:
    no = (container_no or '').strip()
    if not no:
        raise ValueError('container_no is required')
    row = conn.execute('SELECT id FROM containers WHERE id=?', (container_id,)).fetchone()
    if not row:
        raise ValueError('container not found')
    dup = conn.execute('SELECT id FROM containers WHERE container_no=? AND id!=? LIMIT 1', (no, container_id)).fetchone()
    if dup:
        raise ValueError('container_no already exists')
    conn.execute(
        'UPDATE containers SET container_no=?, updated_at=? WHERE id=?',
        (no, now_ts(), container_id),
    )


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


def list_container_items(conn: Connection, container_id: int) -> list[dict]:
    rows = conn.execute(
        '''
        SELECT ci.container_id, ci.inbound_item_id, ci.cbm_at_load, ci.created_at,
               i.inbound_date, i.status, i.shop_no, i.item_no, i.item_name_cn, i.material,
               i.carton_count, i.qty, i.unit_price, i.total_price, i.deposit_hint,
               i.cbm_calculated, i.cbm_override,
               c.name AS customer_name
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        JOIN customers c ON c.id = i.customer_id
        WHERE ci.container_id=?
        ORDER BY i.inbound_date, c.name, i.id
        ''',
        (container_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def update_item_cbm_at_load(conn: Connection, container_id: int, inbound_item_id: int, cbm_at_load: float) -> None:
    c = conn.execute('SELECT id, status, capacity_cbm FROM containers WHERE id=?', (container_id,)).fetchone()
    if not c:
        raise ValueError('container not found')
    if c['status'] != 'DRAFT':
        raise ValueError('container is not editable')

    row = conn.execute(
        'SELECT cbm_at_load FROM container_items WHERE container_id=? AND inbound_item_id=?',
        (container_id, inbound_item_id),
    ).fetchone()
    if not row:
        raise ValueError('inbound item is not in this container')

    new_cbm = to_float(cbm_at_load)
    if new_cbm <= 0:
        raise ValueError('cbm_at_load must be > 0')

    used_other = conn.execute(
        '''
        SELECT COALESCE(SUM(cbm_at_load), 0) AS x
        FROM container_items
        WHERE container_id=? AND inbound_item_id!=?
        ''',
        (container_id, inbound_item_id),
    ).fetchone()
    total_after = float(used_other['x']) + new_cbm
    if total_after > float(c['capacity_cbm']) + 1e-9:
        raise ValueError('container capacity exceeded')

    conn.execute(
        'UPDATE container_items SET cbm_at_load=? WHERE container_id=? AND inbound_item_id=?',
        (new_cbm, container_id, inbound_item_id),
    )


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


def container_manifest(conn: Connection, container_id: int) -> tuple[dict, list[dict], list[dict]]:
    head = conn.execute(
        '''
        SELECT c.id, c.container_no, c.status, c.capacity_cbm, c.default_price_per_m3,
               COALESCE(SUM(ci.cbm_at_load), 0) AS used_cbm, COUNT(ci.id) AS item_count
        FROM containers c
        LEFT JOIN container_items ci ON ci.container_id=c.id
        WHERE c.id=?
        GROUP BY c.id
        ''',
        (container_id,),
    ).fetchone()
    if not head:
        raise ValueError('container not found')

    unit_price = to_float(head['default_price_per_m3'])
    rows = conn.execute(
        '''
        SELECT ci.inbound_item_id, ci.cbm_at_load, i.inbound_date, i.item_no, i.item_name_cn, i.shop_no, i.status AS item_status,
               cu.id AS customer_id, cu.name AS customer_name
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        JOIN customers cu ON cu.id = i.customer_id
        WHERE ci.container_id=?
        ORDER BY cu.name, i.inbound_date, i.id
        ''',
        (container_id,),
    ).fetchall()
    items: list[dict] = []
    for r in rows:
        d = dict(r)
        d['freight_amount'] = round(to_float(d.get('cbm_at_load')) * unit_price, 2)
        items.append(d)

    customer_summary_rows = conn.execute(
        '''
        SELECT cu.id AS customer_id, cu.name AS customer_name, COALESCE(SUM(ci.cbm_at_load), 0) AS cbm_total
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        JOIN customers cu ON cu.id = i.customer_id
        WHERE ci.container_id=?
        GROUP BY cu.id, cu.name
        ORDER BY cu.name
        ''',
        (container_id,),
    ).fetchall()
    customer_summary: list[dict] = []
    for r in customer_summary_rows:
        d = dict(r)
        d['freight_amount'] = round(to_float(d.get('cbm_total')) * unit_price, 2)
        customer_summary.append(d)

    return dict(head), items, customer_summary
