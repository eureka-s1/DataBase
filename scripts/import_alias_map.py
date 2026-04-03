from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import db_session, init_db
from app.services.customers import resolve_customer_id, upsert_alias


def load_alias_csv(path: Path) -> list[dict]:
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))


def main() -> None:
    parser = argparse.ArgumentParser(description='Import customer alias mapping table.')
    parser.add_argument('csv_path', type=Path, help='CSV file with columns: alias_name, customer_id or customer_name')
    args = parser.parse_args()

    init_db()
    rows = load_alias_csv(args.csv_path)
    ok = 0
    failed = 0

    with db_session() as conn:
        for row in rows:
            alias_name = (row.get('alias_name') or '').strip()
            if not alias_name:
                failed += 1
                continue

            customer_id = row.get('customer_id')
            if customer_id:
                target_id = int(customer_id)
            else:
                customer_name = (row.get('customer_name') or '').strip()
                if not customer_name:
                    failed += 1
                    continue
                target_id = resolve_customer_id(conn, customer_name)
                if target_id is None:
                    failed += 1
                    continue

            upsert_alias(
                conn,
                customer_id=target_id,
                alias_name=alias_name,
                source='IMPORT_MAP',
                is_primary=0,
                is_active=1,
                remark='bulk import alias mapping',
            )
            ok += 1

    print(f'alias import done: ok={ok}, failed={failed}')


if __name__ == '__main__':
    main()
