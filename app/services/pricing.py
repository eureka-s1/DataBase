from __future__ import annotations

from sqlite3 import Connection
from .common import today_str


def upsert_price_rule(conn: Connection, customer_id: int, effective_from: str, price_per_m3: float,
                      currency: str = 'USD', effective_to: str | None = None, remark: str | None = None) -> None:
    conn.execute(
        '''
        INSERT INTO customer_price_rules(customer_id, effective_from, effective_to, price_per_m3, currency, remark)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(customer_id, effective_from) DO UPDATE SET
          effective_to=excluded.effective_to,
          price_per_m3=excluded.price_per_m3,
          currency=excluded.currency,
          remark=excluded.remark
        ''',
        (customer_id, effective_from, effective_to, price_per_m3, currency, remark),
    )


def resolve_price_per_m3(conn: Connection, customer_id: int, on_date: str | None = None,
                         container_default: float | None = None) -> float:
    d = on_date or today_str()
    row = conn.execute(
        '''
        SELECT price_per_m3
        FROM customer_price_rules
        WHERE customer_id=?
          AND effective_from <= ?
          AND (effective_to IS NULL OR effective_to >= ?)
        ORDER BY effective_from DESC
        LIMIT 1
        ''',
        (customer_id, d, d),
    ).fetchone()
    if row:
        return float(row['price_per_m3'])

    if container_default is not None:
        return float(container_default)

    row = conn.execute('SELECT default_price_per_m3 FROM customers WHERE id=?', (customer_id,)).fetchone()
    return float(row['default_price_per_m3']) if row else 0.0
