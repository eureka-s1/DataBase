from __future__ import annotations

import re
from datetime import datetime, date


def now_ts() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def today_str() -> str:
    return date.today().isoformat()


def normalize_name(name: str) -> str:
    return re.sub(r'\s+', '', (name or '')).upper().strip()


def calc_cbm(length_cm: float | None, width_cm: float | None, height_cm: float | None) -> float:
    if not length_cm or not width_cm or not height_cm:
        return 0.0
    return round((float(length_cm) * float(width_cm) * float(height_cm)) / 1_000_000, 6)


def to_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        s = str(value).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def to_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        s = str(value).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default
