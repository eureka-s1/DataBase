from __future__ import annotations

from sqlite3 import Connection

from .common import calc_cbm, now_ts, to_float, to_int


def create_inbound_item(conn: Connection, payload: dict) -> int:
    ts = now_ts()
    length_cm = to_float(payload.get('length_cm'))
    width_cm = to_float(payload.get('width_cm'))
    height_cm = to_float(payload.get('height_cm'))

    cbm_file = to_float(payload.get('cbm_calculated'))
    cbm_calc = calc_cbm(length_cm, width_cm, height_cm)
    cbm_final_calc = cbm_file if cbm_file > 0 else cbm_calc

    cur = conn.execute(
        '''
        INSERT INTO inbound_items(
          inbound_no, import_batch_id, customer_id, customer_name_imported, warehouse_id, inbound_date,
          shop_no, position_or_tel, item_no, item_name_cn, material,
          carton_count, qty, unit_price, total_price, deposit_hint,
          length_cm, width_cm, height_cm, cbm_calculated, cbm_override,
          status, container_id, remark, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['inbound_no'],
            payload.get('import_batch_id'),
            payload['customer_id'],
            payload.get('customer_name_imported'),
            payload.get('warehouse_id'),
            payload['inbound_date'],
            payload.get('shop_no'),
            payload.get('position_or_tel'),
            payload.get('item_no'),
            payload.get('item_name_cn'),
            payload.get('material'),
            to_int(payload.get('carton_count'), 0),
            to_int(payload.get('qty'), 0),
            to_float(payload.get('unit_price')),
            to_float(payload.get('total_price')),
            to_float(payload.get('deposit_hint')),
            length_cm,
            width_cm,
            height_cm,
            cbm_final_calc,
            to_float(payload.get('cbm_override'), None),
            payload.get('status', 'IN_STOCK'),
            payload.get('container_id'),
            payload.get('remark'),
            ts,
            ts,
        ),
    )
    return int(cur.lastrowid)


def update_inbound_item(conn: Connection, item_id: int, payload: dict) -> None:
    ts = now_ts()
    if 'length_cm' in payload or 'width_cm' in payload or 'height_cm' in payload:
        old = conn.execute(
            'SELECT length_cm, width_cm, height_cm FROM inbound_items WHERE id=?',
            (item_id,),
        ).fetchone()
        if old:
            length_cm = to_float(payload.get('length_cm'), to_float(old['length_cm']))
            width_cm = to_float(payload.get('width_cm'), to_float(old['width_cm']))
            height_cm = to_float(payload.get('height_cm'), to_float(old['height_cm']))
            cbm_calc = calc_cbm(length_cm, width_cm, height_cm)
            conn.execute(
                'UPDATE inbound_items SET length_cm=?, width_cm=?, height_cm=?, cbm_calculated=?, updated_at=? WHERE id=?',
                (length_cm, width_cm, height_cm, cbm_calc, ts, item_id),
            )

    simple_fields = [
        'shop_no', 'position_or_tel', 'item_no', 'item_name_cn', 'material', 'remark',
        'status', 'inbound_date', 'container_id', 'customer_id', 'warehouse_id'
    ]
    numeric_fields = ['carton_count', 'qty', 'unit_price', 'total_price', 'deposit_hint', 'cbm_override']

    for key in simple_fields:
        if key in payload:
            conn.execute(f'UPDATE inbound_items SET {key}=?, updated_at=? WHERE id=?', (payload[key], ts, item_id))
    for key in numeric_fields:
        if key in payload:
            val = to_int(payload[key]) if key in ('carton_count', 'qty') else to_float(payload[key])
            conn.execute(f'UPDATE inbound_items SET {key}=?, updated_at=? WHERE id=?', (val, ts, item_id))


def delete_inbound_item(conn: Connection, item_id: int) -> int:
    row = conn.execute('SELECT id, status FROM inbound_items WHERE id=?', (item_id,)).fetchone()
    if not row:
        return 0
    if row['status'] != 'IN_STOCK':
        return 0

    # Keep import trace rows but detach FK before deleting inbound item.
    conn.execute('UPDATE inbound_import_rows SET inbound_item_id=NULL WHERE inbound_item_id=?', (item_id,))
    # Defensive cleanup when historical inconsistent rows still reference this item.
    conn.execute('DELETE FROM container_items WHERE inbound_item_id=?', (item_id,))
    cur = conn.execute('DELETE FROM inbound_items WHERE id=? AND status=?', (item_id, 'IN_STOCK'))
    return int(cur.rowcount)


def list_inbound(
    conn: Connection,
    inbound_date: str | None = None,
    only_in_stock: bool = False,
    import_batch_id: int | None = None,
) -> list[dict]:
    where = []
    args: list = []
    if inbound_date:
        where.append('i.inbound_date=?')
        args.append(inbound_date)
    if only_in_stock:
        where.append("i.status='IN_STOCK'")
    if import_batch_id is not None:
        where.append('i.import_batch_id=?')
        args.append(import_batch_id)
    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''

    rows = conn.execute(
        f'''
        SELECT i.*, COALESCE(NULLIF(i.customer_name_imported, ''), c.name) AS customer_name,
               COALESCE(i.cbm_override, i.cbm_calculated) AS cbm_final
        FROM inbound_items i
        JOIN customers c ON c.id = i.customer_id
        {where_sql}
        ORDER BY i.inbound_date DESC, i.id DESC
        ''',
        args,
    ).fetchall()
    return [dict(row) for row in rows]


def list_customer_items(
    conn: Connection,
    customer_id: int,
    status: str | None = None,
    sort_by: str = 'inbound_date',
    sort_dir: str = 'desc',
) -> list[dict]:
    where = ['i.customer_id=?']
    args: list = [customer_id]
    if status and status in ('IN_STOCK', 'ALLOCATED', 'SHIPPED'):
        where.append('i.status=?')
        args.append(status)

    sort_map = {
        'inbound_date': 'i.inbound_date',
        'status': 'i.status',
        'container_no': 'COALESCE(c.container_no, "")',
        'item_name': 'COALESCE(i.item_name_cn, "")',
        'item_no': 'COALESCE(i.item_no, "")',
    }
    order_col = sort_map.get(sort_by, 'i.inbound_date')
    order_dir = 'ASC' if str(sort_dir).lower() == 'asc' else 'DESC'

    rows = conn.execute(
        f'''
        SELECT i.*, COALESCE(NULLIF(i.customer_name_imported, ''), cu.name) AS customer_name,
               COALESCE(i.cbm_override, i.cbm_calculated) AS cbm_final,
               c.container_no,
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
               END AS container_status
        FROM inbound_items i
        JOIN customers cu ON cu.id = i.customer_id
        LEFT JOIN containers c ON c.id = i.container_id
        WHERE {' AND '.join(where)}
        ORDER BY {order_col} {order_dir}, i.id DESC
        ''',
        args,
    ).fetchall()
    return [dict(r) for r in rows]
