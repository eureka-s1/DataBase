from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import db_session, init_db
from app.services.customers import create_customer, upsert_alias


def clean_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip())


def load_folder_names(base: Path) -> list[str]:
    names: list[str] = []
    for p in sorted(base.iterdir()):
        if p.is_dir():
            nm = clean_name(p.name)
            if nm:
                names.append(nm)
    return names


def main() -> None:
    data_dir = ROOT / "2025data"
    init_db()
    folders = load_folder_names(data_dir)
    created = 0
    existed = 0

    with db_session() as conn:
        for i, name in enumerate(folders, start=1):
            row = conn.execute("SELECT id FROM customers WHERE name=? LIMIT 1", (name,)).fetchone()
            if row:
                customer_id = int(row["id"])
                existed += 1
            else:
                code = f"AUTO{i:04d}"
                # Ensure unique customer code if rerun with partially imported data.
                while conn.execute("SELECT 1 FROM customers WHERE customer_code=? LIMIT 1", (code,)).fetchone():
                    i += 1
                    code = f"AUTO{i:04d}"
                customer_id = create_customer(conn, customer_code=code, name=name)
                created += 1

            upsert_alias(conn, customer_id=customer_id, alias_name=name, source="AUTO_DETECT", is_primary=0)

    print(f"bootstrap done: total_folders={len(folders)}, created={created}, existed={existed}")


if __name__ == "__main__":
    main()
