from __future__ import annotations

import re
from datetime import datetime
from sqlite3 import Connection


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_name(name: str) -> str:
    compact = re.sub(r"\s+", "", name).upper().strip()
    return compact


def create_customer(conn: Connection, customer_code: str, name: str, phone: str | None = None,
                    country: str | None = None, email: str | None = None,
                    default_price_per_m3: float = 89.71) -> int:
    ts = now_ts()
    cur = conn.execute(
        """
        INSERT INTO customers(customer_code, name, phone, country, email, default_price_per_m3, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (customer_code, name, phone, country, email, default_price_per_m3, ts, ts),
    )
    customer_id = cur.lastrowid
    upsert_alias(conn, customer_id=customer_id, alias_name=name, source="MANUAL", is_primary=1)
    return customer_id


def upsert_alias(conn: Connection, customer_id: int, alias_name: str,
                 source: str = "IMPORT_MAP", is_primary: int = 0,
                 is_active: int = 1, remark: str | None = None) -> None:
    ts = now_ts()
    alias_name_norm = normalize_name(alias_name)
    conn.execute(
        """
        INSERT INTO customer_aliases(
            customer_id, alias_name, alias_name_norm, source, is_primary, is_active, remark, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(alias_name_norm) DO UPDATE SET
            customer_id=excluded.customer_id,
            alias_name=excluded.alias_name,
            source=excluded.source,
            is_primary=excluded.is_primary,
            is_active=excluded.is_active,
            remark=excluded.remark,
            updated_at=excluded.updated_at
        """,
        (customer_id, alias_name, alias_name_norm, source, is_primary, is_active, remark, ts, ts),
    )


def resolve_customer_id(conn: Connection, raw_name: str) -> int | None:
    normalized = normalize_name(raw_name)
    row = conn.execute(
        """
        SELECT customer_id
        FROM customer_aliases
        WHERE alias_name_norm = ? AND is_active = 1
        LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if row:
        return int(row["customer_id"])

    row = conn.execute(
        "SELECT id FROM customers WHERE UPPER(REPLACE(name, ' ', '')) = ? LIMIT 1",
        (normalized,),
    ).fetchone()
    return int(row["id"]) if row else None


def list_customers(conn: Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT c.id, c.customer_code, c.name, c.phone, c.country, c.email, c.default_price_per_m3,
               GROUP_CONCAT(ca.alias_name, '|') AS aliases
        FROM customers c
        LEFT JOIN customer_aliases ca ON ca.customer_id = c.id AND ca.is_active = 1
        WHERE c.is_active = 1
        GROUP BY c.id
        ORDER BY c.name
        """
    ).fetchall()
    result = []
    for row in rows:
        result.append({
            "id": row["id"],
            "customer_code": row["customer_code"],
            "name": row["name"],
            "phone": row["phone"],
            "country": row["country"],
            "email": row["email"],
            "default_price_per_m3": row["default_price_per_m3"],
            "aliases": row["aliases"].split("|") if row["aliases"] else [],
        })
    return result
