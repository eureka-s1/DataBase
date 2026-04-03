from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import BACKUP_DIR, DB_PATH
from app.db import db_session
from app.services.backup import backup_sqlite


if __name__ == '__main__':
    out = backup_sqlite(DB_PATH, BACKUP_DIR)
    with db_session() as conn:
        conn.execute(
            'INSERT INTO backup_jobs(backup_time, backup_file, size_bytes, status, message) VALUES (datetime("now"), ?, ?, ?, ?)',
            (str(out), out.stat().st_size, 'SUCCESS', 'script backup'),
        )
    print(out)
