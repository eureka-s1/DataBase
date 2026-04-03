from __future__ import annotations

import tempfile
from pathlib import Path

from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[1]
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import create_app
from app.config import DB_PATH
from app.db import init_db


def make_sample_excel(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(['CUSTOMER NAME', 'SHOP NO ', '位置', 'ITEM NO ', '品名', '材质', 'CTNS', 'QTY', 'PRICE', 'T.PRICE ', '定金', 'CBM'])
    ws.append(['MARTIN', 'A1', 'P1', 'IT-1', '杯子', '塑料', 2, 20, 10, 200, 0, 0.5])
    ws.append(['', '', '', 'IT-2', '碗', '陶瓷', 1, 10, 20, 200, 0, 0.3])
    wb.save(path)


def run() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()
    init_db()
    app = create_app()
    c = app.test_client()

    # login
    r = c.post('/login', json={'username': 'admin', 'password': 'admin123'})
    assert r.status_code == 200, r.data

    # create customer + alias
    r = c.post('/customers', json={'customer_code': 'C001', 'name': 'MARTIN'})
    assert r.status_code == 201, r.data
    customer_id = r.get_json()['id']

    r = c.post('/customer-aliases', json={'customer_id': customer_id, 'alias_name': 'MARTIN STORE'})
    assert r.status_code == 200

    # inbound manual
    r = c.post('/inbound-items', json={
        'inbound_no': 'IN-TEST-0001',
        'customer_id': customer_id,
        'warehouse_id': 1,
        'inbound_date': '2026-04-04',
        'item_name_cn': '杯子',
        'carton_count': 2,
        'qty': 20,
        'length_cm': 50,
        'width_cm': 40,
        'height_cm': 30,
    })
    assert r.status_code == 201, r.data
    inbound_item_id = r.get_json()['id']

    # create container & load
    r = c.post('/containers', json={'container_no': 'CAB-20260404-01', 'capacity_cbm': 68})
    assert r.status_code == 201
    container_id = r.get_json()['id']

    r = c.post(f'/containers/{container_id}/items', json={'inbound_item_id': inbound_item_id})
    assert r.status_code == 200, r.data

    # confirm & revoke
    r = c.post(f'/containers/{container_id}/confirm')
    assert r.status_code == 200
    r = c.post(f'/containers/{container_id}/revoke')
    assert r.status_code == 200

    # payment + settlement
    r = c.post('/payments', json={
        'payment_no': 'PAY-0001',
        'customer_id': customer_id,
        'payment_date': '2026-04-04',
        'amount': 1000,
        'method': 'WECHAT',
    })
    assert r.status_code == 201

    # create second container for statement
    r = c.post('/containers', json={'container_no': 'CAB-20260404-02', 'capacity_cbm': 68})
    assert r.status_code == 201
    container2_id = r.get_json()['id']
    r = c.post(f'/containers/{container2_id}/items', json={'inbound_item_id': inbound_item_id})
    assert r.status_code == 200
    r = c.post(f'/containers/{container2_id}/confirm')
    assert r.status_code == 200

    r = c.post('/settlements/generate', json={'container_id': container2_id, 'statement_date': '2026-04-04'})
    assert r.status_code == 201, r.data
    statement_id = r.get_json()['statement_id']

    r = c.post(f'/settlements/{statement_id}/post')
    assert r.status_code == 200

    # exports
    assert c.post('/exports/daily-inbound', json={'inbound_date': '2026-04-04'}).status_code == 200
    assert c.post('/exports/inventory', json={}).status_code == 200
    assert c.post('/exports/ledger', json={}).status_code == 200
    assert c.post(f'/exports/statement/{statement_id}', json={'format': 'xlsx'}).status_code == 200
    assert c.post(f'/exports/statement/{statement_id}', json={'format': 'pdf'}).status_code == 200

    # backup
    assert c.post('/backup', json={}).status_code == 200

    # import preview + dry run execute (no real data import)
    with tempfile.TemporaryDirectory() as tmp:
        sample = Path(tmp) / 'sample.xlsx'
        make_sample_excel(sample)
        r = c.post('/import/inbound/preview', json={'file_path': str(sample)})
        assert r.status_code == 200, r.data
        r = c.post('/import/inbound/execute', json={'file_path': str(sample), 'inbound_date': '2026-04-04', 'dry_run': True})
        assert r.status_code == 200, r.data

    print('smoke test passed')


if __name__ == '__main__':
    run()
