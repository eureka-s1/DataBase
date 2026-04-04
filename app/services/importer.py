from __future__ import annotations

from pathlib import Path
import re
from sqlite3 import Connection
from uuid import uuid4

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
    'DESCRIPTION': 'item_name_cn',
    'DESCRPETION': 'item_name_cn',
    '品名': 'item_name_cn',
    '材质': 'material',
    'CTNS': 'carton_count',
    'CTN': 'carton_count',
    'QTY': 'qty',
    'PRICE': 'unit_price',
    'T.PRICE': 'total_price',
    '定金': 'deposit_hint',
    'CBM': 'cbm_calculated',
    'ITEM NAME': 'item_name_cn',
    'ITEMNAME': 'item_name_cn',
    'CNS': 'carton_count',
    'AMOUNT': 'total_price',
    'DEPOZIT': 'deposit_hint',
    '长': 'length_cm',
    '宽': 'width_cm',
    '高': 'height_cm',
}


def _default_customer_from_path(path: Path) -> str:
    parts = path.parts
    for i, p in enumerate(parts):
        if p == '2025data':
            if i + 1 < len(parts):
                return str(parts[i + 1]).strip()
            break
    return path.parent.name.strip()


def _is_skip_customer_token(value: str) -> bool:
    v = (value or '').strip().upper()
    if not v:
        return True
    bad = ('TOTAL', 'SEND TO ME', 'BALANCE', 'REMAIN', 'PAID', 'COMMISSION', 'FREIGHT')
    if any(x in v for x in bad):
        return True
    return False


def _is_skip_item_token(value: str) -> bool:
    v = (value or '').strip().upper()
    if not v:
        return True
    bad_exact = {'品名', 'ITEM NAME', 'ITEM NO', 'SHOP NO', 'TEL', 'TOTAL', 'BALANCE', 'REMAIN'}
    if v in bad_exact:
        return True
    bad_contains = ('CBM FREIGHT', 'SEND TO ME', 'PAID', 'COMMISSION')
    if any(x in v for x in bad_contains):
        return True
    if re.match(r'^\d+\s*CTNS?$', v):
        return True
    return False


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
        # Many real files do not provide CUSTOMER NAME column in header.
        # We accept header rows with either item_name or item_no plus
        # at least a few useful fields, then fall back to directory name.
        has_item = ('item_name_cn' in mapping.values()) or ('item_no' in mapping.values())
        if len(mapping) >= 4 and has_item:
            return i, mapping
    raise ValueError('unable to detect inbound header row')


def _parse_headerless_excel(path: Path, rows: list[list]) -> dict | None:
    parsed = []
    fallback_customer = _default_customer_from_path(path)
    last_customer_name = fallback_customer

    for i, row in enumerate(rows[:500], start=1):
        vals = [None if v is None else str(v).strip() for v in row]
        while vals and not vals[0]:
            vals.pop(0)
        tokens = [v for v in vals if v]
        if len(tokens) < 5:
            continue

        item = None

        # Layout A:
        # [customer, shop/tel, ?, item_name, material, ctn, qty, cbm, L, W, H]
        if len(tokens) >= 8 and tokens[3]:
            cbm_a = to_float(tokens[7]) if len(tokens) > 7 else 0.0
            cbm_a_alt = to_float(tokens[6]) if len(tokens) > 6 else 0.0
            use_alt = cbm_a <= 0 and cbm_a_alt > 0
            cbm_used = cbm_a_alt if use_alt else cbm_a
            if cbm_used > 0:
                customer = tokens[0] or last_customer_name or fallback_customer
                if not _is_skip_customer_token(customer):
                    last_customer_name = customer
                else:
                    customer = last_customer_name or fallback_customer
                item = {
                    'customer_name': customer,
                    'shop_no': tokens[1] if len(tokens) > 1 else None,
                    'item_name_cn': tokens[3],
                    'material': tokens[4] if len(tokens) > 4 else None,
                    'carton_count': to_int(tokens[5]) if len(tokens) > 5 else 0,
                    'qty': to_int(tokens[6]) if (len(tokens) > 6 and not use_alt) else 0,
                    'cbm_calculated': cbm_used,
                    'length_cm': to_float(tokens[7]) if use_alt and len(tokens) > 7 else (to_float(tokens[8]) if len(tokens) > 8 else 0.0),
                    'width_cm': to_float(tokens[8]) if use_alt and len(tokens) > 8 else (to_float(tokens[9]) if len(tokens) > 9 else 0.0),
                    'height_cm': to_float(tokens[9]) if use_alt and len(tokens) > 9 else (to_float(tokens[10]) if len(tokens) > 10 else 0.0),
                    'row_no': i,
                }

        # Layout B (continuation row):
        # [item_name, material, ctn, qty, cbm, L, W, H]
        if item is None and len(tokens) >= 5 and tokens[0]:
            cbm_b = to_float(tokens[4]) if len(tokens) > 4 else 0.0
            if cbm_b > 0:
                item = {
                    'customer_name': last_customer_name or fallback_customer,
                    'item_name_cn': tokens[0],
                    'material': tokens[1] if len(tokens) > 1 else None,
                    'carton_count': to_int(tokens[2]) if len(tokens) > 2 else 0,
                    'qty': to_int(tokens[3]) if len(tokens) > 3 else 0,
                    'cbm_calculated': cbm_b,
                    'length_cm': to_float(tokens[5]) if len(tokens) > 5 else 0.0,
                    'width_cm': to_float(tokens[6]) if len(tokens) > 6 else 0.0,
                    'height_cm': to_float(tokens[7]) if len(tokens) > 7 else 0.0,
                    'row_no': i,
                }

        if item and item.get('item_name_cn') and not _is_skip_item_token(str(item.get('item_name_cn'))):
            parsed.append(item)

    if not parsed:
        return None

    return {
        'header_row': 0,
        'field_mapping': {},
        'rows': parsed,
        'errors': [],
    }


def parse_inbound_excel(path: Path) -> dict:
    rows = _read_rows(path)
    try:
        header_idx, mapping = _detect_header(rows)
    except ValueError:
        fallback = _parse_headerless_excel(path, rows)
        if fallback:
            return fallback
        raise
    fallback_customer = _default_customer_from_path(path)
    has_customer_col = 'customer_name' in mapping.values()

    parsed = []
    errors = []
    last_customer_name = ''

    for i, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        item = {}
        for col_idx, field in mapping.items():
            item[field] = row[col_idx] if col_idx < len(row) else None

        if has_customer_col:
            customer_name = str(item.get('customer_name') or '').strip()
        else:
            customer_name = ''

        if has_customer_col and customer_name and not _is_skip_customer_token(customer_name):
            last_customer_name = customer_name
        else:
            customer_name = last_customer_name
        if not customer_name:
            customer_name = fallback_customer

        item_name = str(item.get('item_name_cn') or '').strip()
        if not item_name:
            item_name = str(item.get('item_no') or '').strip()
        if not item_name or _is_skip_item_token(item_name):
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
    fallback_customer = _default_customer_from_path(path)
    batch_no = f'IB-{ts.replace("-", "").replace(":", "").replace(" ", "")}-{uuid4().hex[:6].upper()}'

    conn.execute(
        '''
        INSERT INTO import_batches(batch_no, source_file, sheet_name, import_type, total_rows, success_rows, failed_rows, created_by, created_at)
        VALUES (?, ?, ?, 'inbound', ?, 0, 0, ?, ?)
        ''',
        (batch_no, str(path), 'Sheet1', len(parsed['rows']), created_by, ts),
    )
    batch_id = int(conn.execute('SELECT last_insert_rowid()').fetchone()[0])

    success = 0
    failed = 0
    err_rows = []

    for idx, row in enumerate(parsed['rows'], start=1):
        cid = resolve_customer_id(conn, row['customer_name'])
        if cid is None and fallback_customer:
            cid = resolve_customer_id(conn, fallback_customer)
        if cid is None:
            failed += 1
            err_rows.append({'row_no': row['row_no'], 'reason': f"customer not mapped: {row['customer_name']}"})
            continue

        payload = {
            'inbound_no': f'IN-{inbound_date.replace("-", "")}-{batch_id:06d}-{idx:05d}',
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
