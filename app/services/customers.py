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


def _next_auto_customer_code(conn: Connection, prefix: str = "AUTOIMP") -> str:
    i = 1
    while True:
        code = f"{prefix}{i:05d}"
        row = conn.execute("SELECT 1 FROM customers WHERE customer_code=? LIMIT 1", (code,)).fetchone()
        if not row:
            return code
        i += 1


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


def get_or_create_customer_id(conn: Connection, raw_name: str) -> tuple[int, bool]:
    name = (raw_name or "").strip()
    if not name:
        raise ValueError("empty customer name")

    cid = resolve_customer_id(conn, name)
    if cid is not None:
        return cid, False

    code = _next_auto_customer_code(conn)
    cid = create_customer(conn, customer_code=code, name=name, default_price_per_m3=89.71)
    upsert_alias(conn, customer_id=cid, alias_name=name, source="AUTO_DETECT", is_primary=1, remark="auto created from import")
    return int(cid), True


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


def find_customer_id_by_name(conn: Connection, name: str) -> int | None:
    raw = (name or "").strip()
    if not raw:
        return None
    row = conn.execute("SELECT id FROM customers WHERE name=? LIMIT 1", (raw,)).fetchone()
    if row:
        return int(row["id"])
    return resolve_customer_id(conn, raw)


def merge_customers(conn: Connection, source_customer_id: int, target_customer_id: int) -> dict:
    if source_customer_id == target_customer_id:
        raise ValueError("source and target cannot be the same")

    src = conn.execute("SELECT id, name, is_active FROM customers WHERE id=?", (source_customer_id,)).fetchone()
    tgt = conn.execute("SELECT id, name, is_active FROM customers WHERE id=?", (target_customer_id,)).fetchone()
    if not src:
        raise ValueError("source customer not found")
    if not tgt:
        raise ValueError("target customer not found")

    # 1) move aliases from source -> target
    alias_rows = conn.execute(
        "SELECT alias_name, source, is_primary, is_active, remark FROM customer_aliases WHERE customer_id=?",
        (source_customer_id,),
    ).fetchall()
    for r in alias_rows:
        upsert_alias(
            conn,
            customer_id=target_customer_id,
            alias_name=r["alias_name"],
            source=r["source"] or "MANUAL",
            is_primary=0,
            is_active=int(r["is_active"] or 1),
            remark=r["remark"] or f"merged from customer {source_customer_id}",
        )
    # keep old source name as alias too
    upsert_alias(
        conn,
        customer_id=target_customer_id,
        alias_name=src["name"],
        source="MANUAL",
        is_primary=0,
        is_active=1,
        remark=f"merged source customer {source_customer_id}",
    )
    conn.execute("DELETE FROM customer_aliases WHERE customer_id=?", (source_customer_id,))

    # 2) move major business rows
    inbound_count = conn.execute(
        "UPDATE inbound_items SET customer_id=? WHERE customer_id=?",
        (target_customer_id, source_customer_id),
    ).rowcount
    payment_count = conn.execute(
        "UPDATE payment_transactions SET customer_id=? WHERE customer_id=?",
        (target_customer_id, source_customer_id),
    ).rowcount

    # 3) merge price rules (skip duplicates by effective_from)
    conn.execute(
        '''
        INSERT INTO customer_price_rules(customer_id, effective_from, effective_to, price_per_m3, currency, remark)
        SELECT ?, effective_from, effective_to, price_per_m3, currency, remark
        FROM customer_price_rules
        WHERE customer_id=?
        ON CONFLICT(customer_id, effective_from) DO NOTHING
        ''',
        (target_customer_id, source_customer_id),
    )
    conn.execute("DELETE FROM customer_price_rules WHERE customer_id=?", (source_customer_id,))

    # 4) merge settlement_lines with unique(statement_id, customer_id)
    src_lines = conn.execute(
        "SELECT id, statement_id, cbm_total, freight_amount, deposit_used, amount_due, amount_balance FROM settlement_lines WHERE customer_id=?",
        (source_customer_id,),
    ).fetchall()
    settlement_moved = 0
    settlement_merged = 0
    for sl in src_lines:
        tgt_line = conn.execute(
            "SELECT id FROM settlement_lines WHERE statement_id=? AND customer_id=? LIMIT 1",
            (sl["statement_id"], target_customer_id),
        ).fetchone()
        if tgt_line:
            conn.execute(
                '''
                UPDATE settlement_lines
                SET cbm_total=cbm_total+?,
                    freight_amount=freight_amount+?,
                    deposit_used=deposit_used+?,
                    amount_due=amount_due+?,
                    amount_balance=amount_balance+?
                WHERE id=?
                ''',
                (
                    sl["cbm_total"] or 0,
                    sl["freight_amount"] or 0,
                    sl["deposit_used"] or 0,
                    sl["amount_due"] or 0,
                    sl["amount_balance"] or 0,
                    int(tgt_line["id"]),
                ),
            )
            conn.execute(
                "UPDATE payment_allocations SET settlement_line_id=? WHERE settlement_line_id=?",
                (int(tgt_line["id"]), int(sl["id"])),
            )
            conn.execute("DELETE FROM settlement_lines WHERE id=?", (int(sl["id"]),))
            settlement_merged += 1
        else:
            conn.execute("UPDATE settlement_lines SET customer_id=? WHERE id=?", (target_customer_id, int(sl["id"])))
            settlement_moved += 1

    # 5) soft deactivate source customer
    ts = now_ts()
    conn.execute(
        "UPDATE customers SET is_active=0, updated_at=? WHERE id=?",
        (ts, source_customer_id),
    )

    return {
        "source_customer_id": source_customer_id,
        "target_customer_id": target_customer_id,
        "source_name": src["name"],
        "target_name": tgt["name"],
        "inbound_rows_moved": int(inbound_count),
        "payment_rows_moved": int(payment_count),
        "settlement_rows_moved": int(settlement_moved),
        "settlement_rows_merged": int(settlement_merged),
        "message": "customers merged",
    }


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


def update_customer_phone(conn: Connection, customer_id: int, phone: str | None) -> None:
    row = conn.execute("SELECT id FROM customers WHERE id=? AND is_active=1", (customer_id,)).fetchone()
    if not row:
        raise ValueError("customer not found")
    phone_text = str(phone or "").strip()
    conn.execute(
        "UPDATE customers SET phone=?, updated_at=? WHERE id=?",
        (phone_text or None, now_ts(), customer_id),
    )
