from __future__ import annotations

from sqlite3 import Connection

from .common import now_ts, today_str


def add_payment(conn: Connection, payload: dict, user_id: int) -> int:
    ts = now_ts()
    cur = conn.execute(
        '''
        INSERT INTO payment_transactions(
          payment_no, customer_id, payment_date, amount, currency, method, reference_no, remark, created_by, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            payload['payment_no'],
            payload['customer_id'],
            payload.get('payment_date', today_str()),
            payload['amount'],
            payload.get('currency', 'CNY'),
            payload.get('method', 'WECHAT'),
            payload.get('reference_no'),
            payload.get('remark'),
            user_id,
            ts,
        ),
    )
    return int(cur.lastrowid)


def customer_deposit_balance(conn: Connection, customer_id: int) -> float:
    paid = conn.execute('SELECT COALESCE(SUM(amount),0) AS x FROM payment_transactions WHERE customer_id=?', (customer_id,)).fetchone()
    used = conn.execute(
        '''
        SELECT COALESCE(SUM(pa.allocated_amount),0) AS x
        FROM payment_allocations pa
        JOIN settlement_lines sl ON sl.id = pa.settlement_line_id
        WHERE sl.customer_id=?
        ''',
        (customer_id,),
    ).fetchone()
    return round(float(paid['x']) - float(used['x']), 2)


def _next_statement_no(conn: Connection, base_no: str) -> str:
    no = (base_no or '').strip()
    if not no:
        raise ValueError('statement_no is empty')
    exists = conn.execute('SELECT 1 FROM settlement_statements WHERE statement_no=? LIMIT 1', (no,)).fetchone()
    if not exists:
        return no
    i = 2
    while True:
        candidate = f'{no}-{i}'
        row = conn.execute('SELECT 1 FROM settlement_statements WHERE statement_no=? LIMIT 1', (candidate,)).fetchone()
        if not row:
            return candidate
        i += 1


def generate_statement(conn: Connection, container_id: int, user_id: int,
                       statement_no: str | None = None, statement_date: str | None = None) -> int:
    c = conn.execute('SELECT id, status, default_price_per_m3 FROM containers WHERE id=?', (container_id,)).fetchone()
    if not c:
        raise ValueError('container not found')
    if c['status'] != 'CONFIRMED':
        raise ValueError('only CONFIRMED container can be settled')
    if c['default_price_per_m3'] is None:
        raise ValueError('container unit price is required')

    date_val = statement_date or today_str()
    base_no = (statement_no or f'STM-{container_id}-{date_val.replace("-", "")}').strip()
    no = _next_statement_no(conn, base_no)
    ts = now_ts()

    cur = conn.execute(
        '''
        INSERT INTO settlement_statements(statement_no, container_id, statement_date, status, currency, created_by, created_at, updated_at)
        VALUES (?, ?, ?, 'DRAFT', 'CNY', ?, ?, ?)
        ''',
        (no, container_id, date_val, user_id, ts, ts),
    )
    statement_id = int(cur.lastrowid)

    rows = conn.execute(
        '''
        SELECT i.customer_id, SUM(ci.cbm_at_load) AS cbm_total
        FROM container_items ci
        JOIN inbound_items i ON i.id = ci.inbound_item_id
        WHERE ci.container_id=?
        GROUP BY i.customer_id
        ''',
        (container_id,),
    ).fetchall()

    if not rows:
        raise ValueError('container has no items')

    container_price = float(c['default_price_per_m3'])
    for row in rows:
        cid = int(row['customer_id'])
        cbm_total = float(row['cbm_total'])
        freight = round(cbm_total * container_price, 2)

        available = customer_deposit_balance(conn, cid)
        use_deposit = round(min(available, freight), 2)
        due = round(freight - use_deposit, 2)
        balance = round(available - use_deposit, 2)

        line_cur = conn.execute(
            '''
            INSERT INTO settlement_lines(statement_id, customer_id, cbm_total, price_per_m3, freight_amount,
                                         deposit_used, amount_due, amount_balance, remark)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (statement_id, cid, cbm_total, container_price, freight, use_deposit, due, balance, None),
        )
        line_id = int(line_cur.lastrowid)

        if use_deposit > 0:
            remaining = use_deposit
            payments = conn.execute(
                '''
                SELECT p.id, p.amount,
                       p.amount - COALESCE((SELECT SUM(pa.allocated_amount) FROM payment_allocations pa WHERE pa.payment_id=p.id), 0) AS remain
                FROM payment_transactions p
                WHERE p.customer_id=?
                ORDER BY p.payment_date, p.id
                ''',
                (cid,),
            ).fetchall()
            for p in payments:
                if remaining <= 0:
                    break
                can_alloc = round(max(float(p['remain']), 0), 2)
                if can_alloc <= 0:
                    continue
                alloc = round(min(can_alloc, remaining), 2)
                conn.execute(
                    'INSERT INTO payment_allocations(payment_id, settlement_line_id, allocated_amount, created_at) VALUES (?, ?, ?, ?)',
                    (int(p['id']), line_id, alloc, ts),
                )
                remaining = round(remaining - alloc, 2)

    return statement_id


def post_statement(conn: Connection, statement_id: int) -> None:
    row = conn.execute('SELECT status FROM settlement_statements WHERE id=?', (statement_id,)).fetchone()
    if not row:
        raise ValueError('statement not found')
    if row['status'] != 'DRAFT':
        raise ValueError('only DRAFT statement can be posted')
    conn.execute("UPDATE settlement_statements SET status='POSTED', updated_at=? WHERE id=?", (now_ts(), statement_id))


def unpost_statement(conn: Connection, statement_id: int) -> None:
    row = conn.execute('SELECT status FROM settlement_statements WHERE id=?', (statement_id,)).fetchone()
    if not row:
        raise ValueError('statement not found')
    if row['status'] != 'POSTED':
        raise ValueError('only POSTED statement can be unposted')
    conn.execute("UPDATE settlement_statements SET status='DRAFT', updated_at=? WHERE id=?", (now_ts(), statement_id))


def list_statements(conn: Connection, limit: int = 100) -> list[dict]:
    rows = conn.execute(
        '''
        SELECT s.id, s.statement_no, s.statement_date, s.status, s.currency, c.container_no, s.created_at
        FROM settlement_statements s
        JOIN containers c ON c.id = s.container_id
        ORDER BY s.id DESC
        LIMIT ?
        ''',
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def ledger(conn: Connection, customer_id: int | None = None) -> list[dict]:
    where = 'WHERE c.is_active=1'
    args = []
    if customer_id:
        where = 'WHERE c.is_active=1 AND c.id=?'
        args.append(customer_id)

    rows = conn.execute(
        f'''
        SELECT c.id AS customer_id, c.name,
               COALESCE((SELECT GROUP_CONCAT(ca.alias_name, '|') FROM customer_aliases ca WHERE ca.customer_id=c.id AND ca.is_active=1), '') AS aliases,
               COALESCE((SELECT SUM(amount) FROM payment_transactions p WHERE p.customer_id=c.id),0) AS total_deposit,
               COALESCE((SELECT SUM(freight_amount) FROM settlement_lines sl WHERE sl.customer_id=c.id),0) AS total_freight,
               COALESCE((SELECT SUM(amount_due) FROM settlement_lines sl2 WHERE sl2.customer_id=c.id),0) AS total_due,
               COALESCE((SELECT SUM(amount_balance) FROM settlement_lines sl3 WHERE sl3.customer_id=c.id),0) AS latest_balance
        FROM customers c
        {where}
        ORDER BY c.name
        ''',
        args,
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['aliases'] = d['aliases'].split('|') if d.get('aliases') else []
        d['net_balance'] = round(float(d['total_deposit']) - float(d['total_freight']), 2)
        result.append(d)
    return result
