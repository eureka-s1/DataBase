from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from sqlite3 import Connection

import openpyxl

from .common import now_ts, to_float, to_int


def ensure_sync_columns(conn: Connection) -> None:
    cols_ib = {str(r["name"]) for r in conn.execute("PRAGMA table_info(import_batches)").fetchall()}
    if "receipt_synced_at" not in cols_ib:
        conn.execute("ALTER TABLE import_batches ADD COLUMN receipt_synced_at TEXT")
    cols_ct = {str(r["name"]) for r in conn.execute("PRAGMA table_info(containers)").fetchall()}
    if "outbound_synced_at" not in cols_ct:
        conn.execute("ALTER TABLE containers ADD COLUMN outbound_synced_at TEXT")


def _find_customer_dir(work_dir: Path, customer_name: str) -> Path | None:
    exact = work_dir / customer_name
    if exact.exists() and exact.is_dir():
        return exact
    up = customer_name.strip().upper()
    for p in work_dir.iterdir():
        if p.is_dir() and p.name.strip().upper() == up:
            return p
    return None


def _pick_receipt_xlsx(customer_dir: Path, customer_name: str) -> Path:
    files = [
        p for p in customer_dir.rglob("*")
        if p.is_file() and p.suffix.lower() == ".xlsx" and not p.name.startswith("~$")
    ]
    preferred = [p for p in files if "收货清单" in p.name]
    target = preferred or files
    if target:
        target.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return target[0]
    out = customer_dir / f"{customer_name} 收货清单.xlsx"
    wb = openpyxl.Workbook()
    wb.active.title = "Sheet1"
    wb.save(out)
    return out


def _ensure_month_sheet(wb, customer_name: str, customer_phone: str | None, year: int, month: int):
    candidates = [f"{year} {month:02d}", f"{year} {month}"]
    for sname in candidates:
        if sname in wb.sheetnames:
            return wb[sname]
    sname = candidates[0]
    ws = wb.create_sheet(title=sname)
    ws.cell(1, 1, customer_phone or "")
    ws.cell(1, 2, f"{customer_name} ORDER {year}-{month}")
    headers = ["SHOP NO", "TEL", "ITEM NO", "品名", "材质", "CTNS", "QTY", "PRICE", "T.PRICE", "定金", "CBM", "长", "宽", "高"]
    for i, h in enumerate(headers, start=1):
        ws.cell(2, i, h)
    return ws


def list_receipt_sync_batches(conn: Connection, limit: int = 100) -> list[dict]:
    ensure_sync_columns(conn)
    rows = conn.execute(
        """
        SELECT b.id AS batch_id, b.batch_no, b.source_file, b.created_at, b.success_rows, b.failed_rows,
               b.receipt_synced_at,
               COALESCE((SELECT COUNT(*) FROM inbound_items i WHERE i.import_batch_id=b.id), 0) AS item_rows
        FROM import_batches b
        WHERE b.import_type='inbound'
        ORDER BY b.id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def sync_receipts_by_batch(conn: Connection, batch_id: int, work_dir: Path) -> dict:
    ensure_sync_columns(conn)
    rows = conn.execute(
        """
        SELECT i.*, c.name AS customer_name, COALESCE(c.phone, '') AS customer_phone
        FROM inbound_items i
        JOIN customers c ON c.id = i.customer_id
        WHERE i.import_batch_id=?
        ORDER BY c.name, i.inbound_date, i.id
        """,
        (batch_id,),
    ).fetchall()
    if not rows:
        return {"batch_id": batch_id, "customers": 0, "rows": 0, "files_updated": 0, "errors": ["no inbound rows in batch"]}

    grouped: dict[str, list] = {}
    for r in rows:
        grouped.setdefault(str(r["customer_name"]), []).append(r)

    files_updated = 0
    err: list[str] = []
    row_count = 0
    for customer_name, items in grouped.items():
        cdir = _find_customer_dir(work_dir, customer_name)
        if not cdir:
            err.append(f"{customer_name}: customer folder not found")
            continue
        try:
            xlsx = _pick_receipt_xlsx(cdir, customer_name)
            wb = openpyxl.load_workbook(xlsx)
            phone = str(items[0]["customer_phone"] or "").strip()
            changed = False
            for it in items:
                try:
                    d = datetime.strptime(str(it["inbound_date"]), "%Y-%m-%d")
                except Exception:
                    d = datetime.now()
                ws = _ensure_month_sheet(wb, customer_name, phone, d.year, d.month)
                rno = ws.max_row + 1
                ws.cell(rno, 1, it["shop_no"] or "")
                ws.cell(rno, 2, it["position_or_tel"] or "")
                ws.cell(rno, 3, it["item_no"] or "")
                ws.cell(rno, 4, it["item_name_cn"] or "")
                ws.cell(rno, 5, it["material"] or "")
                ws.cell(rno, 6, to_int(it["carton_count"], 0))
                ws.cell(rno, 7, to_int(it["qty"], 0))
                ws.cell(rno, 8, to_float(it["unit_price"], 0.0))
                ws.cell(rno, 9, to_float(it["total_price"], 0.0))
                ws.cell(rno, 10, to_float(it["deposit_hint"], 0.0))
                ws.cell(rno, 11, to_float(it["cbm_override"], to_float(it["cbm_calculated"], 0.0)))
                ws.cell(rno, 12, to_float(it["length_cm"], 0.0))
                ws.cell(rno, 13, to_float(it["width_cm"], 0.0))
                ws.cell(rno, 14, to_float(it["height_cm"], 0.0))
                row_count += 1
                changed = True
            if changed:
                wb.save(xlsx)
                files_updated += 1
        except Exception as e:
            err.append(f"{customer_name}: {e}")

    conn.execute("UPDATE import_batches SET receipt_synced_at=? WHERE id=?", (now_ts(), batch_id))
    return {
        "batch_id": batch_id,
        "customers": len(grouped),
        "rows": row_count,
        "files_updated": files_updated,
        "errors": err,
    }


def list_outbound_sync_containers(conn: Connection, limit: int = 200) -> list[dict]:
    ensure_sync_columns(conn)
    rows = conn.execute(
        """
        SELECT c.id, c.container_no, c.status, c.confirmed_at, c.outbound_synced_at,
               COALESCE((SELECT COUNT(*) FROM container_items ci WHERE ci.container_id=c.id), 0) AS item_count
        FROM containers c
        WHERE c.status='CONFIRMED'
        ORDER BY c.id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def sync_outbound_container(conn: Connection, container_id: int, work_dir: Path) -> dict:
    ensure_sync_columns(conn)
    c = conn.execute("SELECT * FROM containers WHERE id=? AND status='CONFIRMED'", (container_id,)).fetchone()
    if not c:
        raise ValueError("container not found or not CONFIRMED")
    rows = conn.execute(
        """
        SELECT cu.name AS customer_name, COALESCE(cu.phone, '') AS customer_phone,
               SUM(COALESCE(i.carton_count,0)) AS ctns,
               SUM(COALESCE(ci.cbm_at_load, COALESCE(i.cbm_override, i.cbm_calculated), 0)) AS cbm_total
        FROM container_items ci
        JOIN inbound_items i ON i.id=ci.inbound_item_id
        JOIN customers cu ON cu.id=i.customer_id
        WHERE ci.container_id=?
        GROUP BY cu.id, cu.name, cu.phone
        ORDER BY cu.name
        """,
        (container_id,),
    ).fetchall()
    if not rows:
        return {"container_id": container_id, "customers": 0, "files_updated": 0, "errors": ["container has no items"]}

    files_updated = 0
    err: list[str] = []
    date_text = datetime.now().strftime("%Y/%m/%d")
    unit_price = to_float(c["default_price_per_m3"], 0.0)

    for r in rows:
        name = str(r["customer_name"])
        cdir = _find_customer_dir(work_dir, name)
        if not cdir:
            err.append(f"{name}: customer folder not found")
            continue
        try:
            xlsx = _pick_receipt_xlsx(cdir, name)
            wb = openpyxl.load_workbook(xlsx)
            ws = wb[wb.sheetnames[-1]]
            row_no = ws.max_row + 3
            ctns = to_int(r["ctns"], 0)
            cbm = round(to_float(r["cbm_total"], 0.0), 3)
            freight_usd = round(cbm * unit_price, 2)
            ws.cell(row_no, 4, str(c["container_no"] or ""))
            ws.cell(row_no, 5, f"{ctns} CTNS")
            ws.cell(row_no, 6, str(r["customer_phone"] or ""))
            ws.cell(row_no, 7, name)
            ws.cell(row_no, 8, date_text)
            ws.cell(row_no, 9, f"{cbm} CBM FREIGHT")
            ws.cell(row_no, 11, freight_usd)
            wb.save(xlsx)
            files_updated += 1
        except Exception as e:
            err.append(f"{name}: {e}")

    conn.execute("UPDATE containers SET outbound_synced_at=?, updated_at=? WHERE id=?", (now_ts(), now_ts(), container_id))
    return {"container_id": container_id, "customers": len(rows), "files_updated": files_updated, "errors": err}


@dataclass
class MonthlyUpdateResult:
    ym: str
    files_scanned: int
    files_updated: int
    errors: list[str]


def monthly_create_sheet(work_dir: Path, year: int, month: int) -> MonthlyUpdateResult:
    ym = f"{year:04d} {month:02d}"
    files = [
        p for p in work_dir.rglob("*")
        if p.is_file() and p.suffix.lower() == ".xlsx" and not p.name.startswith("~$") and "收货清单" in p.name
    ]
    updated = 0
    errors: list[str] = []
    for p in files:
        try:
            wb = openpyxl.load_workbook(p)
            if ym not in wb.sheetnames:
                wb.create_sheet(title=ym)
                wb.save(p)
                updated += 1
        except Exception as e:
            errors.append(f"{p}: {e}")
    return MonthlyUpdateResult(ym=ym, files_scanned=len(files), files_updated=updated, errors=errors)
