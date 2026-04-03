from __future__ import annotations

from pathlib import Path
from sqlite3 import Connection

import openpyxl
import xlrd

from .common import normalize_name, now_ts, to_float, to_int
from .customers import resolve_customer_id
from .inbound import create_inbound_item

HEADER_MAP = {
    'CUSTOMER NAME': 'customer_name',
    'SHOP NO': 'shop_no',
    '位置': 'position_or_tel',
    'TEL': 'position_or_tel',
    'ITEM NO': 'item_no',
    '品名': 'item_name_cn',
    '材质': 'material',
    'CTNS': 'carton_count',
    'CTN': 'carton_count',
    'QTY': 'qty',
    'PRICE': 'unit_price',
    'T.PRICE': 'total_price',
    '定金': 'deposit_hint',
    'CBM': 'cbm_calculated',
    '长': 'length_cm',
    '宽': 'width_cm',
    '高': 'height_cm',
}


def _read_rows(path: Path, max_cols: int = 30) -> list[list]:
    rows: list[list] = []
    suffix = path.suffix.lower()
    if suffix == '.xlsx':
        wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
        ws = wb[wb.sheetnames[0]]
        for r in ws.iter_rows(values_only=True, max_col=max_cols):
            rows.append(list(r))
        wb.close()
    elif suffix == '.xls':
        wb = xlrd.open_workbook(path)
        sh = wb.sheet_by_index(0)
        for i in range(sh.nrows):
            rows.append(sh.row_values(i, 0, max_cols))
    else:
        raise ValueError('unsupported file type')
    return rows


def _norm_header(value) -> str:
    return str(value or '').strip().upper().replace(' ', '')


def _detect_header(rows: list[list]) -> tuple[int, dict[int, str]]:
    keys = {k.replace(' ', '').upper(): v for k, v in HEADER_MAP.items()}
    for i, row in enumerate(rows[:80]):
        mapping: dict[int, str] = {}
        for idx, val in enumerate(row):
            kk = _norm_header(val)
            if kk in keys:
                mapping[idx] = keys[kk]
        if len(mapping) >= 5 and 'customer_name' in mapping.values() and 'item_name_cn' in mapping.values():
            return i, mapping
    raise ValueError('unable to detect inbound header row')


def parse_inbound_excel(path: Path) -> dict:
    rows = _read_rows(path)
    header_idx, mapping = _detect_header(rows)

    parsed = []
    errors = []
    last_customer_name = ''

    for i, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        item = {}
        for col_idx, field in mapping.items():
            item[field] = row[col_idx] if col_idx < len(row) else None

        customer_name = str(item.get('customer_name') or '').strip()
        if customer_name:
            last_customer_name = customer_name
        else:
            customer_name = last_customer_name
        if not customer_name:
            continue

        item_name = str(item.get('item_name_cn') or '').strip()
        if not item_name:
            continue

        item['customer_name'] = customer_name
        item['carton_count'] = to_int(item.get('carton_count'))
        item['qty'] = to_int(item.get('qty'))
        item['unit_price'] = to_float(item.get('unit_price'))
        item['total_price'] = to_float(item.get('total_price'))
        item['deposit_hint'] = to_float(item.get('deposit_hint'))
        item['cbm_calculated'] = to_float(item.get('cbm_calculated'))
        item['length_cm'] = to_float(item.get('length_cm'))
        item['width_cm'] = to_float(item.get('width_cm'))
        item['height_cm'] = to_float(item.get('height_cm'))
        item['row_no'] = i

        parsed.append(item)

    return {
        'header_row': header_idx + 1,
        'field_mapping': mapping,
        'rows': parsed,
        'errors': errors,
    }


def import_inbound_excel(conn: Connection, path: Path, inbound_date: str, created_by: int,
                         dry_run: bool = True) -> dict:
    parsed = parse_inbound_excel(path)
    ts = now_ts()

    conn.execute(
        '''
        INSERT INTO import_batches(batch_no, source_file, sheet_name, import_type, total_rows, success_rows, failed_rows, created_by, created_at)
        VALUES (?, ?, ?, 'inbound', ?, 0, 0, ?, ?)
        ''',
        (f'IB-{ts.replace("-", "").replace(":", "").replace(" ", "")}', str(path), 'Sheet1', len(parsed['rows']), created_by, ts),
    )
    batch_id = int(conn.execute('SELECT last_insert_rowid()').fetchone()[0])

    success = 0
    failed = 0
    err_rows = []

    for idx, row in enumerate(parsed['rows'], start=1):
        cid = resolve_customer_id(conn, row['customer_name'])
        if cid is None:
            failed += 1
            err_rows.append({'row_no': row['row_no'], 'reason': f"customer not mapped: {row['customer_name']}"})
            continue

        payload = {
            'inbound_no': f'IN-{inbound_date.replace("-", "")}-{idx:05d}',
            'import_batch_id': batch_id,
            'customer_id': cid,
            'warehouse_id': 1,
            'inbound_date': inbound_date,
            'shop_no': row.get('shop_no'),
            'position_or_tel': row.get('position_or_tel'),
            'item_no': row.get('item_no'),
            'item_name_cn': row.get('item_name_cn'),
            'material': row.get('material'),
            'carton_count': row.get('carton_count'),
            'qty': row.get('qty'),
            'unit_price': row.get('unit_price'),
            'total_price': row.get('total_price'),
            'deposit_hint': row.get('deposit_hint'),
            'length_cm': row.get('length_cm'),
            'width_cm': row.get('width_cm'),
            'height_cm': row.get('height_cm'),
            'cbm_calculated': row.get('cbm_calculated'),
            'status': 'IN_STOCK',
        }
        if not dry_run:
            create_inbound_item(conn, payload)
        success += 1

    conn.execute(
        'UPDATE import_batches SET success_rows=?, failed_rows=? WHERE id=?',
        (success, failed, batch_id),
    )

    return {
        'batch_id': batch_id,
        'total_rows': len(parsed['rows']),
        'success_rows': success,
        'failed_rows': failed,
        'errors': err_rows,
        'dry_run': dry_run,
    }
