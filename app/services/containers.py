from __future__ import annotations

from sqlite3 import Connection

from .common import now_ts, to_float, to_int


def _capacity_with_tolerance(capacity_cbm: float) -> float:
    cap = to_float(capacity_cbm, 0.0)
    # Business rule: allow loading up to 71 CBM.
    return max(cap, 71.0)


def _ensure_master_customer_valid(conn: Connection, container_id: int) -> None:
    row = conn.execute(
        'SELECT master_customer_id FROM containers WHERE id=?',
        (container_id,),
    ).fetchone()
    if not row or row['master_customer_id'] is None:
        return
    master_customer_id = int(row['master_customer_id'])
    hit = conn.execute(
        '''
        SELECT 1
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        WHERE ci.container_id=? AND i.customer_id=?
        LIMIT 1
        ''',
        (container_id, master_customer_id),
    ).fetchone()
    if not hit:
        conn.execute(
            'UPDATE containers SET master_customer_id=NULL, updated_at=? WHERE id=?',
            (now_ts(), container_id),
        )


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


def update_container_master_customer(conn: Connection, container_id: int, master_customer_id: int | None) -> None:
    row = conn.execute('SELECT id FROM containers WHERE id=?', (container_id,)).fetchone()
    if not row:
        raise ValueError('container not found')
    cid = None
    if master_customer_id is not None:
        cid = to_int(master_customer_id, 0)
        if cid <= 0:
            cid = None
        else:
            hit = conn.execute('SELECT id FROM customers WHERE id=? AND is_active=1', (cid,)).fetchone()
            if not hit:
                raise ValueError('master customer not found')
            in_container = conn.execute(
                '''
                SELECT 1
                FROM container_items ci
                JOIN inbound_items i ON i.id = ci.inbound_item_id
                WHERE ci.container_id=? AND i.customer_id=?
                LIMIT 1
                ''',
                (container_id, cid),
            ).fetchone()
            if not in_container:
                raise ValueError('master customer must have items in current container')
    conn.execute(
        'UPDATE containers SET master_customer_id=?, updated_at=? WHERE id=?',
        (cid, now_ts(), container_id),
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
    if usage['used_cbm'] + cbm > _capacity_with_tolerance(usage['capacity_cbm']) + 1e-9:
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


def _next_split_inbound_no(conn: Connection, base_no: str) -> str:
    i = 1
    while True:
        cand = f'{base_no}-S{i}'
        hit = conn.execute('SELECT 1 FROM inbound_items WHERE inbound_no=? LIMIT 1', (cand,)).fetchone()
        if not hit:
            return cand
        i += 1


def _split_float_keep_total(total: float, ratio: float, digits: int) -> tuple[float, float]:
    part = round(float(total) * ratio, digits)
    keep = round(float(total) - part, digits)
    return keep, part


def _split_int_keep_total(total: int, ratio: float) -> tuple[int, int]:
    part = int(round(float(total) * ratio))
    part = max(0, min(int(total), part))
    keep = int(total) - part
    return keep, part


def split_inbound_item_by_cartons(
    conn: Connection,
    inbound_item_id: int,
    split_cartons: int,
    length_cm: float | None = None,
    width_cm: float | None = None,
    height_cm: float | None = None,
) -> dict:
    row = conn.execute(
        '''
        SELECT *
        FROM inbound_items
        WHERE id=?
        ''',
        (inbound_item_id,),
    ).fetchone()
    if not row:
        raise ValueError('inbound item not found')
    if row['status'] != 'IN_STOCK':
        raise ValueError('only IN_STOCK item can be split')
    if row['container_id'] is not None:
        raise ValueError('allocated item cannot be split')

    total_cartons = to_int(row['carton_count'], 0)
    split_cartons = to_int(split_cartons, 0)
    if total_cartons <= 1:
        raise ValueError('carton_count must be > 1 to split')
    if split_cartons <= 0 or split_cartons >= total_cartons:
        raise ValueError(f'split_cartons must be between 1 and {total_cartons - 1}')

    ratio = float(split_cartons) / float(total_cartons)
    source_is_merged = str(row['item_name_cn'] or '').strip().endswith('*') or ('MERGED_CARTON' in str(row['remark'] or ''))

    qty_keep, qty_split = _split_int_keep_total(to_int(row['qty'], 0), ratio)
    unit_keep, unit_split = _split_float_keep_total(to_float(row['unit_price'], 0.0), ratio, 2)
    total_keep, total_split = _split_float_keep_total(to_float(row['total_price'], 0.0), ratio, 2)
    cbm_keep, cbm_split = _split_float_keep_total(to_float(row['cbm_calculated'], 0.0), ratio, 6)
    cbm_override_src = row['cbm_override']
    cbm_override_keep = None
    cbm_override_split = None
    if cbm_override_src is not None:
        cbm_override_keep, cbm_override_split = _split_float_keep_total(to_float(cbm_override_src, 0.0), ratio, 6)

    length_final = to_float(length_cm, to_float(row['length_cm']))
    width_final = to_float(width_cm, to_float(row['width_cm']))
    height_final = to_float(height_cm, to_float(row['height_cm']))

    remark_src = str(row['remark'] or '').strip()
    if source_is_merged:
        if '[SPLIT_FROM_MERGED]' not in remark_src:
            remark_src = f'{remark_src};[SPLIT_FROM_MERGED]' if remark_src else '[SPLIT_FROM_MERGED]'

    ts = now_ts()
    split_no = _next_split_inbound_no(conn, str(row['inbound_no']))
    conn.execute(
        '''
        UPDATE inbound_items
        SET carton_count=?, qty=?, unit_price=?, total_price=?, cbm_calculated=?, cbm_override=?,
            length_cm=?, width_cm=?, height_cm=?, remark=?, updated_at=?
        WHERE id=?
        ''',
        (
            total_cartons - split_cartons, qty_keep, unit_keep, total_keep, cbm_keep, cbm_override_keep,
            length_final, width_final, height_final, remark_src, ts, inbound_item_id
        ),
    )

    cur = conn.execute(
        '''
        INSERT INTO inbound_items(
          inbound_no, import_batch_id, customer_id, customer_name_imported, warehouse_id, inbound_date,
          shop_no, position_or_tel, item_no, item_name_cn, material,
          carton_count, qty, unit_price, total_price, deposit_hint,
          length_cm, width_cm, height_cm, cbm_calculated, cbm_override,
          status, container_id, remark, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'IN_STOCK', NULL, ?, ?, ?)
        ''',
        (
            split_no,
            row['import_batch_id'],
            row['customer_id'],
            row['customer_name_imported'],
            row['warehouse_id'],
            row['inbound_date'],
            row['shop_no'],
            row['position_or_tel'],
            row['item_no'],
            row['item_name_cn'],
            row['material'],
            split_cartons,
            qty_split,
            unit_split,
            total_split,
            row['deposit_hint'],
            length_final,
            width_final,
            height_final,
            cbm_split,
            cbm_override_split,
            remark_src,
            ts,
            ts,
        ),
    )
    new_id = int(cur.lastrowid)
    return {
        'source_item_id': int(inbound_item_id),
        'new_item_id': new_id,
        'source_cartons': int(total_cartons - split_cartons),
        'new_cartons': int(split_cartons),
        'source_qty': int(qty_keep),
        'new_qty': int(qty_split),
        'source_unit_price': unit_keep,
        'new_unit_price': unit_split,
        'source_total_price': total_keep,
        'new_total_price': total_split,
        'source_cbm': cbm_keep,
        'new_cbm': cbm_split,
        'source_is_merged': bool(source_is_merged),
        'length_cm': length_final,
        'width_cm': width_final,
        'height_cm': height_final,
    }


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
        _ensure_master_customer_valid(conn, container_id)
    return cur.rowcount


def list_container_items(conn: Connection, container_id: int) -> list[dict]:
    rows = conn.execute(
        '''
        SELECT ci.container_id, ci.inbound_item_id, ci.cbm_at_load, ci.created_at,
               i.inbound_date, i.status, i.shop_no, i.item_no, i.item_name_cn, i.material,
               i.carton_count, i.qty, i.unit_price, i.total_price, i.deposit_hint,
               i.cbm_calculated, i.cbm_override,
               COALESCE(NULLIF(i.customer_name_imported, ''), c.name) AS customer_name
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
    if total_after > _capacity_with_tolerance(float(c['capacity_cbm'])) + 1e-9:
        raise ValueError('container capacity exceeded')

    conn.execute(
        'UPDATE container_items SET cbm_at_load=? WHERE container_id=? AND inbound_item_id=?',
        (new_cbm, container_id, inbound_item_id),
    )


def confirm_container(conn: Connection, container_id: int) -> None:
    c = conn.execute('SELECT status, master_customer_id FROM containers WHERE id=?', (container_id,)).fetchone()
    if not c:
        raise ValueError('container not found')
    if c['status'] != 'DRAFT':
        raise ValueError('container status must be DRAFT')
    if c['master_customer_id'] is None:
        raise ValueError('master customer is required before confirm')
    hit = conn.execute(
        '''
        SELECT 1
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        WHERE ci.container_id=? AND i.customer_id=?
        LIMIT 1
        ''',
        (container_id, int(c['master_customer_id'])),
    ).fetchone()
    if not hit:
        raise ValueError('master customer must have items in current container')

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
    st = conn.execute(
        """
        SELECT id, status
        FROM settlement_statements
        WHERE container_id=? AND status IN ('DRAFT', 'POSTED')
        ORDER BY id DESC
        LIMIT 1
        """,
        (container_id,),
    ).fetchone()
    if st:
        raise ValueError('container has settlement statement, revoke settlement first')

    ts = now_ts()
    conn.execute(
        "UPDATE containers SET status='DRAFT', confirmed_at=NULL, revoked_at=NULL, updated_at=? WHERE id=?",
        (ts, container_id),
    )
    conn.execute(
        "UPDATE inbound_items SET status='ALLOCATED', updated_at=? WHERE container_id=?",
        (ts, container_id),
    )
    _ensure_master_customer_valid(conn, container_id)


def list_containers(conn: Connection) -> list[dict]:
    rows = conn.execute(
        '''
        SELECT c.*,
               mc.name AS master_customer_name,
               COALESCE(SUM(ci.cbm_at_load), 0) AS used_cbm,
               COUNT(ci.id) AS item_count,
               CASE
                 WHEN c.status='CONFIRMED' AND EXISTS(
                   SELECT 1 FROM settlement_statements s
                   WHERE s.container_id=c.id AND s.status='POSTED'
                 ) THEN 'POSTED'
                 WHEN c.status='CONFIRMED' AND EXISTS(
                   SELECT 1 FROM settlement_statements s2
                   WHERE s2.container_id=c.id AND s2.status='DRAFT'
                 ) THEN 'SETTLING'
                 ELSE c.status
               END AS status_display
        FROM containers c
        LEFT JOIN container_items ci ON ci.container_id = c.id
        LEFT JOIN customers mc ON mc.id = c.master_customer_id
        GROUP BY c.id
        ORDER BY c.id DESC
        '''
    ).fetchall()
    return [dict(row) for row in rows]


def container_manifest(conn: Connection, container_id: int) -> tuple[dict, list[dict], list[dict]]:
    head = conn.execute(
        '''
        SELECT c.id, c.container_no, c.status, c.capacity_cbm, c.default_price_per_m3, c.master_customer_id,
               mc.name AS master_customer_name,
               COALESCE(SUM(ci.cbm_at_load), 0) AS used_cbm, COUNT(ci.id) AS item_count
        FROM containers c
        LEFT JOIN container_items ci ON ci.container_id=c.id
        LEFT JOIN customers mc ON mc.id = c.master_customer_id
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
        SELECT ci.inbound_item_id, ci.cbm_at_load, i.inbound_date, i.item_no, i.item_name_cn, i.shop_no, i.position_or_tel, i.status AS item_status,
               i.material, i.carton_count, i.qty, i.unit_price, i.total_price, i.deposit_hint,
               i.length_cm, i.width_cm, i.height_cm,
               cu.id AS customer_id, COALESCE(NULLIF(i.customer_name_imported, ''), cu.name) AS customer_name
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        JOIN customers cu ON cu.id = i.customer_id
        WHERE ci.container_id=?
        ORDER BY COALESCE(NULLIF(i.customer_name_imported, ''), cu.name), i.inbound_date, i.id
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
        SELECT cu.id AS customer_id,
               COALESCE(
                 (
                   SELECT i2.customer_name_imported
                   FROM container_items ci2
                   JOIN inbound_items i2 ON i2.id = ci2.inbound_item_id
                   WHERE ci2.container_id = ci.container_id
                     AND i2.customer_id = cu.id
                     AND NULLIF(TRIM(i2.customer_name_imported), '') IS NOT NULL
                   GROUP BY i2.customer_name_imported
                   ORDER BY COUNT(*) DESC, MAX(i2.id) DESC
                   LIMIT 1
                 ),
                 cu.name
               ) AS customer_name,
               COALESCE(cu.phone, '') AS customer_phone,
               COALESCE(SUM(i.carton_count), 0) AS ctns,
               COALESCE(SUM(ci.cbm_at_load), 0) AS cbm_total
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        JOIN customers cu ON cu.id = i.customer_id
        WHERE ci.container_id=?
        GROUP BY cu.id, cu.name, cu.phone
        ORDER BY customer_name
        ''',
        (container_id,),
    ).fetchall()
    customer_summary: list[dict] = []
    for r in customer_summary_rows:
        d = dict(r)
        d['freight_amount'] = round(to_float(d.get('cbm_total')) * unit_price, 2)
        customer_summary.append(d)

    return dict(head), items, customer_summary
