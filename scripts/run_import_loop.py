from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import db_session, init_db
from app.services.importer import import_inbound_excel, parse_inbound_excel
from app.services.customers import create_customer, upsert_alias


EXCLUDE_KEYWORDS = (
    "费用明细",
    "请款单",
    "对账",
    "装柜",
    "剩货",
    "invoice",
    "proforma",
    "profoma",
    "支付宝",
    "拼柜名单",
)

EXCLUDE_TOP_DIRS = {
    "海运费",
    "店面对账",
    "小杨",
    "新建文件夹",
}


@dataclass
class LoopResult:
    path: str
    parsed: bool
    total_rows: int = 0
    success_rows: int = 0
    failed_rows: int = 0
    error: str = ""
    committed: bool = False


def is_candidate(path: Path, data_root: Path) -> bool:
    if path.suffix.lower() not in (".xls", ".xlsx"):
        return False
    if path.name.startswith("~$") or path.name.endswith(".tmp"):
        return False
    rel = path.relative_to(data_root)
    rel_text = str(rel).lower()
    if rel.parts and rel.parts[0] in EXCLUDE_TOP_DIRS:
        return False
    if any(k.lower() in rel_text for k in EXCLUDE_KEYWORDS):
        return False
    return True


def clean_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip())


def bootstrap_customers(data_root: Path) -> tuple[int, int]:
    folders = [clean_name(p.name) for p in sorted(data_root.iterdir()) if p.is_dir() and clean_name(p.name)]
    created = 0
    existed = 0
    with db_session() as conn:
        for i, name in enumerate(folders, start=1):
            row = conn.execute("SELECT id FROM customers WHERE name=? LIMIT 1", (name,)).fetchone()
            if row:
                cid = int(row["id"])
                existed += 1
            else:
                code = f"AUTO{i:04d}"
                while conn.execute("SELECT 1 FROM customers WHERE customer_code=? LIMIT 1", (code,)).fetchone():
                    i += 1
                    code = f"AUTO{i:04d}"
                cid = create_customer(conn, customer_code=code, name=name)
                created += 1
            upsert_alias(conn, customer_id=cid, alias_name=name, source="AUTO_DETECT", is_primary=0)
    return created, existed


def write_report(report_path: Path, results: list[LoopResult], inbound_date: str, created: int, existed: int) -> None:
    total = len(results)
    parsed_ok = sum(1 for r in results if r.parsed)
    parse_fail = total - parsed_ok
    dry_rows_total = sum(r.total_rows for r in results if r.parsed)
    dry_rows_ok = sum(r.success_rows for r in results if r.parsed)
    dry_rows_failed = sum(r.failed_rows for r in results if r.parsed)
    committed = sum(1 for r in results if r.committed)

    lines = [
        "# 导入测试 Loop 报告",
        "",
        f"- inbound_date: `{inbound_date}`",
        f"- 样本文件总数: `{total}`",
        f"- 解析成功: `{parsed_ok}`",
        f"- 解析失败: `{parse_fail}`",
        f"- dry-run 总行数: `{dry_rows_total}`",
        f"- dry-run 成功行: `{dry_rows_ok}`",
        f"- dry-run 失败行: `{dry_rows_failed}`",
        f"- 正式导入文件数: `{committed}`",
        f"- 客户预置: created=`{created}`, existed=`{existed}`",
        "",
        "## 解析失败（前30）",
    ]

    for r in [x for x in results if not x.parsed][:30]:
        lines.append(f"- `{r.path}` -> {r.error}")

    lines += ["", "## dry-run 失败文件（前30）"]
    fails = [x for x in results if x.parsed and x.failed_rows > 0]
    fails.sort(key=lambda x: x.failed_rows, reverse=True)
    for r in fails[:30]:
        lines.append(
            f"- `{r.path}` rows={r.total_rows}, ok={r.success_rows}, failed={r.failed_rows}"
        )

    lines += ["", "## 正式导入文件", *(f"- `{r.path}`" for r in results if r.committed)]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description="Run import-test loop on inbound-like files.")
    p.add_argument("--data-dir", type=Path, default=ROOT / "2025data")
    p.add_argument("--inbound-date", default="2026-04-04")
    p.add_argument("--limit", type=int, default=120)
    p.add_argument("--commit-files", type=int, default=10)
    p.add_argument("--reset-db", action="store_true")
    p.add_argument("--report", type=Path, default=ROOT / "agent" / "import_loop_report.md")
    args = p.parse_args()

    if args.reset_db:
        db_file = Path(__import__("app.config", fromlist=["DB_PATH"]).DB_PATH)
        if db_file.exists():
            db_file.unlink()
    init_db()
    created, existed = bootstrap_customers(args.data_dir)

    files = [x for x in sorted(args.data_dir.rglob("*")) if x.is_file() and is_candidate(x, args.data_dir)]
    if args.limit > 0:
        files = files[: args.limit]

    results: list[LoopResult] = []

    for f in files:
        rel = str(f.relative_to(ROOT))
        try:
            parse_inbound_excel(f)
        except Exception as e:
            results.append(LoopResult(path=rel, parsed=False, error=str(e)))
            continue

        with db_session() as conn:
            r = import_inbound_excel(conn, path=f, inbound_date=args.inbound_date, created_by=1, dry_run=True)
        results.append(
            LoopResult(
                path=rel,
                parsed=True,
                total_rows=r["total_rows"],
                success_rows=r["success_rows"],
                failed_rows=r["failed_rows"],
            )
        )

    can_commit = [r for r in results if r.parsed and r.failed_rows == 0 and r.total_rows > 0][: args.commit_files]
    for r in can_commit:
        f = ROOT / r.path
        with db_session() as conn:
            import_inbound_excel(conn, path=f, inbound_date=args.inbound_date, created_by=1, dry_run=False)
        r.committed = True

    write_report(args.report, results, args.inbound_date, created, existed)
    print(f"loop finished: files={len(files)}, report={args.report}")
    print(f"parsed_ok={sum(1 for x in results if x.parsed)}, parse_fail={sum(1 for x in results if not x.parsed)}")
    print(f"committed={sum(1 for x in results if x.committed)}")


if __name__ == "__main__":
    main()
