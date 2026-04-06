from __future__ import annotations

from pathlib import Path
from sqlite3 import Connection

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

from .common import today_str
from .containers import container_manifest
from .finance import ledger
from .inbound import list_inbound


def _export_dir() -> Path:
    path = Path('exports')
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sanitize_filename(s: str) -> str:
    bad = '\\/:*?"<>|'
    out = ''.join('_' if ch in bad else ch for ch in (s or ''))
    return out.strip() or 'export'


def _style_sheet(ws, col_widths: list[int] | None = None) -> None:
    if ws.max_row <= 0:
        return
    header_fill = PatternFill('solid', fgColor='E8F1FB')
    thin = Side(style='thin', color='D9E1EA')
    for cell in ws[1]:
        cell.font = Font(bold=True, color='1F2D3D')
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = Border(top=thin, bottom=thin, left=thin, right=thin)
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.alignment = Alignment(vertical='center')
            cell.border = Border(top=thin, bottom=thin, left=thin, right=thin)
    ws.freeze_panes = 'A2'
    if col_widths:
        for i, w in enumerate(col_widths, start=1):
            ws.column_dimensions[get_column_letter(i)].width = w
    else:
        for col in range(1, ws.max_column + 1):
            max_len = 8
            for row in range(1, ws.max_row + 1):
                val = ws.cell(row=row, column=col).value
                if val is None:
                    continue
                max_len = max(max_len, len(str(val)))
            ws.column_dimensions[get_column_letter(col)].width = min(max_len + 2, 50)


def _new_pdf(out: Path):
    try:
        pdfmetrics.getFont('STSong-Light')
    except Exception:
        pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
    c = canvas.Canvas(str(out), pagesize=A4)
    c.setFont('STSong-Light', 11)
    return c


def export_daily_inbound_excel(conn: Connection, inbound_date: str | None = None) -> str:
    d = inbound_date or today_str()
    data = list_inbound(conn, inbound_date=d)

    wb = Workbook()
    ws = wb.active
    ws.title = 'daily_inbound'
    ws.append([
        'inbound_no', 'date', 'customer', 'shop_no', 'item_no', 'item_name', 'material',
        'ctn', 'qty', 'unit_price', 'total_price', 'deposit_hint',
        'length_cm', 'width_cm', 'height_cm', 'cbm_final', 'status'
    ])
    for row in data:
        ws.append([
            row.get('inbound_no'), row.get('inbound_date'), row.get('customer_name'), row.get('shop_no'),
            row.get('item_no'), row.get('item_name_cn'), row.get('material'), row.get('carton_count'),
            row.get('qty'), row.get('unit_price'), row.get('total_price'), row.get('deposit_hint'),
            row.get('length_cm'), row.get('width_cm'), row.get('height_cm'), row.get('cbm_final'), row.get('status'),
        ])
    _style_sheet(ws, [20, 12, 16, 12, 14, 20, 12, 8, 8, 10, 12, 10, 10, 10, 10, 10, 10])

    out = _export_dir() / f'daily_inbound_{d}.xlsx'
    wb.save(out)
    return str(out)


def export_inventory_excel(conn: Connection) -> str:
    data = list_inbound(conn, only_in_stock=True)
    wb = Workbook()
    ws = wb.active
    ws.title = 'inventory'
    ws.append(['inbound_no', 'date', 'customer', 'shop_no', 'item_no', 'item_name', 'material', 'ctn', 'qty', 'length_cm', 'width_cm', 'height_cm', 'cbm_final', 'status'])
    for row in data:
        ws.append([
            row.get('inbound_no'), row.get('inbound_date'), row.get('customer_name'), row.get('shop_no'),
            row.get('item_no'), row.get('item_name_cn'), row.get('material'), row.get('carton_count'),
            row.get('qty'), row.get('length_cm'), row.get('width_cm'), row.get('height_cm'), row.get('cbm_final'), row.get('status'),
        ])
    _style_sheet(ws, [20, 12, 16, 12, 14, 20, 12, 8, 8, 10, 10, 10, 10, 10])
    out = _export_dir() / f'inventory_{today_str()}.xlsx'
    wb.save(out)
    return str(out)


def export_ledger_excel(conn: Connection) -> str:
    data = ledger(conn)
    wb = Workbook()
    ws = wb.active
    ws.title = 'ledger'
    ws.append(['customer_id', 'name', 'aliases', 'total_deposit', 'total_freight', 'total_due', 'net_balance'])
    for row in data:
        ws.append([
            row.get('customer_id'), row.get('name'), ' / '.join(row.get('aliases') or []), row.get('total_deposit'),
            row['total_freight'], row['total_due'], row['net_balance'],
        ])
    _style_sheet(ws, [10, 18, 24, 12, 12, 12, 12])
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
    ws.append(['statement_no', 'container_no', 'statement_date', 'status'])
    ws.append([head.get('statement_no'), head.get('container_no'), head.get('statement_date'), head.get('status')])
    ws.append([])
    ws.append(['customer', 'cbm_total', 'price_per_m3', 'freight', 'deposit_used', 'amount_due', 'balance'])
    for r in rows:
        ws.append([
            r['customer_name'], r['cbm_total'], r['price_per_m3'], r['freight_amount'],
            r['deposit_used'], r['amount_due'], r['amount_balance'],
        ])
    _style_sheet(ws, [20, 14, 14, 14, 14, 14, 14])
    out = _export_dir() / f"statement_{_sanitize_filename(head['statement_no'])}.xlsx"
    wb.save(out)
    return str(out)


def export_statement_pdf(conn: Connection, statement_id: int) -> str:
    head, rows = statement_lines(conn, statement_id)
    out = _export_dir() / f"statement_{_sanitize_filename(head['statement_no'])}.pdf"
    c = _new_pdf(out)
    width, height = A4
    y = height - 40
    c.drawString(40, y, f"结算单: {head['statement_no']}  柜号: {head['container_no']}  日期: {head['statement_date']}")
    y -= 30
    c.drawString(40, y, '客户')
    c.drawString(180, y, '体积CBM')
    c.drawString(260, y, '单价')
    c.drawString(320, y, '运费')
    c.drawString(390, y, '扣款')
    c.drawString(460, y, '应收')
    y -= 16
    for r in rows:
        if y < 60:
            c.showPage()
            y = height - 40
            c.setFont('STSong-Light', 11)
        c.drawString(40, y, str(r['customer_name'])[:20])
        c.drawRightString(250, y, f"{float(r['cbm_total']):.3f}")
        c.drawRightString(310, y, f"{float(r['price_per_m3']):.2f}")
        c.drawRightString(380, y, f"{float(r['freight_amount']):.2f}")
        c.drawRightString(450, y, f"{float(r['deposit_used']):.2f}")
        c.drawRightString(530, y, f"{float(r['amount_due']):.2f}")
        y -= 14
    c.save()
    return str(out)


def export_container_excel(conn: Connection, container_id: int) -> str:
    head, items, customer_summary = container_manifest(conn, container_id)
    wb = Workbook()
    ws = wb.active
    ws.title = 'container_manifest'
    ws.append(['container_no', 'status', 'capacity_cbm', 'used_cbm', 'price_per_m3'])
    ws.append([head.get('container_no'), head.get('status'), head.get('capacity_cbm'), head.get('used_cbm'), head.get('default_price_per_m3')])
    ws.append([])
    ws.append(['customer', 'cbm_total', 'freight_amount'])
    for r in customer_summary:
        ws.append([r['customer_name'], r['cbm_total'], r['freight_amount']])
    ws.append([])
    ws.append(['customer', 'customer_id', 'inbound_date', 'item_no', 'item_name', 'shop_no', 'item_status', 'length_cm', 'width_cm', 'height_cm', 'cbm_at_load', 'item_freight'])
    for r in items:
        ws.append([
            r['customer_name'], r['customer_id'], r['inbound_date'], r['item_no'], r['item_name_cn'],
            r['shop_no'], r['item_status'], r.get('length_cm'), r.get('width_cm'), r.get('height_cm'), r['cbm_at_load'], r['freight_amount'],
        ])
    _style_sheet(ws, [18, 12, 12, 14, 22, 12, 10, 10, 10, 10, 12, 12])

    out = _export_dir() / f"container_{_sanitize_filename(head['container_no'])}_{today_str()}.xlsx"
    wb.save(out)
    return str(out)


def export_container_pdf(conn: Connection, container_id: int) -> str:
    head, items, _ = container_manifest(conn, container_id)
    out = _export_dir() / f"container_{_sanitize_filename(head['container_no'])}_{today_str()}.pdf"
    c = _new_pdf(out)
    width, height = A4
    y = height - 40
    c.drawString(40, y, f"柜号: {head['container_no']}  状态: {head['status']}  已用/容量: {head['used_cbm']}/{head['capacity_cbm']}")
    y -= 16
    c.drawString(40, y, f"单价(每立方): {head['default_price_per_m3']}")
    y -= 24
    c.drawString(40, y, '客户')
    c.drawString(180, y, '货品')
    c.drawString(330, y, 'CBM')
    c.drawString(410, y, '金额')
    y -= 16
    for r in items:
        if y < 60:
            c.showPage()
            y = height - 40
            c.setFont('STSong-Light', 11)
        c.drawString(40, y, str(r['customer_name'])[:20])
        c.drawString(180, y, str(r['item_name_cn'] or r['item_no'] or '')[:24])
        c.drawRightString(390, y, f"{float(r['cbm_at_load']):.3f}")
        c.drawRightString(520, y, f"{float(r['freight_amount']):.2f}")
        y -= 14
    c.save()
    return str(out)
