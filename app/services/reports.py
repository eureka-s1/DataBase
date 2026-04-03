from __future__ import annotations

from pathlib import Path
from sqlite3 import Connection

from openpyxl import Workbook
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from .common import today_str
from .finance import ledger
from .inbound import list_inbound


def _export_dir() -> Path:
    path = Path('exports')
    path.mkdir(parents=True, exist_ok=True)
    return path


def export_daily_inbound_excel(conn: Connection, inbound_date: str | None = None) -> str:
    d = inbound_date or today_str()
    data = list_inbound(conn, inbound_date=d)

    wb = Workbook()
    ws = wb.active
    ws.title = 'daily_inbound'
    ws.append(['inbound_no', 'date', 'customer', 'item', 'ctn', 'qty', 'cbm_final', 'status'])
    for row in data:
        ws.append([
            row['inbound_no'], row['inbound_date'], row['customer_name'], row['item_name_cn'],
            row['carton_count'], row['qty'], row['cbm_final'], row['status'],
        ])

    out = _export_dir() / f'daily_inbound_{d}.xlsx'
    wb.save(out)
    return str(out)


def export_inventory_excel(conn: Connection) -> str:
    data = list_inbound(conn, only_in_stock=True)
    wb = Workbook()
    ws = wb.active
    ws.title = 'inventory'
    ws.append(['inbound_no', 'date', 'customer', 'item', 'cbm_final'])
    for row in data:
        ws.append([row['inbound_no'], row['inbound_date'], row['customer_name'], row['item_name_cn'], row['cbm_final']])
    out = _export_dir() / f'inventory_{today_str()}.xlsx'
    wb.save(out)
    return str(out)


def export_ledger_excel(conn: Connection) -> str:
    data = ledger(conn)
    wb = Workbook()
    ws = wb.active
    ws.title = 'ledger'
    ws.append(['customer_id', 'name', 'total_deposit', 'total_freight', 'total_due', 'net_balance'])
    for row in data:
        ws.append([
            row['customer_id'], row['name'], row['total_deposit'],
            row['total_freight'], row['total_due'], row['net_balance'],
        ])
    out = _export_dir() / f'ledger_{today_str()}.xlsx'
    wb.save(out)
    return str(out)


def statement_lines(conn: Connection, statement_id: int) -> tuple[dict, list[dict]]:
    head = conn.execute(
        '''
        SELECT s.id, s.statement_no, s.statement_date, s.status, c.container_no
        FROM settlement_statements s
        JOIN containers c ON c.id = s.container_id
        WHERE s.id=?
        ''',
        (statement_id,),
    ).fetchone()
    if not head:
        raise ValueError('statement not found')

    rows = conn.execute(
        '''
        SELECT sl.*, cu.name AS customer_name
        FROM settlement_lines sl
        JOIN customers cu ON cu.id = sl.customer_id
        WHERE sl.statement_id=?
        ORDER BY cu.name
        ''',
        (statement_id,),
    ).fetchall()
    return dict(head), [dict(r) for r in rows]


def export_statement_excel(conn: Connection, statement_id: int) -> str:
    head, rows = statement_lines(conn, statement_id)
    wb = Workbook()
    ws = wb.active
    ws.title = 'statement'
    ws.append(['statement_no', head['statement_no']])
    ws.append(['container_no', head['container_no']])
    ws.append(['statement_date', head['statement_date']])
    ws.append([])
    ws.append(['customer', 'cbm_total', 'price_per_m3', 'freight', 'deposit_used', 'amount_due', 'balance'])
    for r in rows:
        ws.append([
            r['customer_name'], r['cbm_total'], r['price_per_m3'], r['freight_amount'],
            r['deposit_used'], r['amount_due'], r['amount_balance'],
        ])
    out = _export_dir() / f"statement_{head['statement_no']}.xlsx"
    wb.save(out)
    return str(out)


def export_statement_pdf(conn: Connection, statement_id: int) -> str:
    head, rows = statement_lines(conn, statement_id)
    out = _export_dir() / f"statement_{head['statement_no']}.pdf"
    c = canvas.Canvas(str(out), pagesize=A4)
    width, height = A4
    y = height - 40
    c.setFont('Helvetica', 11)
    c.drawString(40, y, f"Statement: {head['statement_no']}  Container: {head['container_no']}  Date: {head['statement_date']}")
    y -= 30
    c.drawString(40, y, 'Customer')
    c.drawString(180, y, 'CBM')
    c.drawString(240, y, 'Price')
    c.drawString(300, y, 'Freight')
    c.drawString(370, y, 'Deposit')
    c.drawString(450, y, 'Due')
    y -= 16
    for r in rows:
        if y < 60:
            c.showPage()
            y = height - 40
        c.drawString(40, y, str(r['customer_name'])[:20])
        c.drawRightString(220, y, f"{float(r['cbm_total']):.3f}")
        c.drawRightString(290, y, f"{float(r['price_per_m3']):.2f}")
        c.drawRightString(360, y, f"{float(r['freight_amount']):.2f}")
        c.drawRightString(440, y, f"{float(r['deposit_used']):.2f}")
        c.drawRightString(520, y, f"{float(r['amount_due']):.2f}")
        y -= 14
    c.save()
    return str(out)
