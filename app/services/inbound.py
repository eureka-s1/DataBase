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
          inbound_no, import_batch_id, customer_id, warehouse_id, inbound_date,
          shop_no, position_or_tel, item_no, item_name_cn, material,
          carton_count, qty, unit_price, total_price, deposit_hint,
          length_cm, width_cm, height_cm, cbm_calculated, cbm_override,
          status, container_id, remark, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['inbound_no'],
            payload.get('import_batch_id'),
            payload['customer_id'],
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
    length_cm = to_float(payload.get('length_cm')) if 'length_cm' in payload else None
    width_cm = to_float(payload.get('width_cm')) if 'width_cm' in payload else None
    height_cm = to_float(payload.get('height_cm')) if 'height_cm' in payload else None

    if length_cm is not None and width_cm is not None and height_cm is not None:
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
    cur = conn.execute('DELETE FROM inbound_items WHERE id=? AND status=?', (item_id, 'IN_STOCK'))
    return cur.rowcount


def list_inbound(conn: Connection, inbound_date: str | None = None, only_in_stock: bool = False) -> list[dict]:
    where = []
    args: list = []
    if inbound_date:
        where.append('i.inbound_date=?')
        args.append(inbound_date)
    if only_in_stock:
        where.append("i.status='IN_STOCK'")
    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''

    rows = conn.execute(
        f'''
        SELECT i.*, c.name AS customer_name,
               COALESCE(i.cbm_override, i.cbm_calculated) AS cbm_final
        FROM inbound_items i
        JOIN customers c ON c.id = i.customer_id
        {where_sql}
        ORDER BY i.inbound_date DESC, i.id DESC
        ''',
        args,
    ).fetchall()
    return [dict(row) for row in rows]
