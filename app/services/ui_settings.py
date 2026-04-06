from __future__ import annotations

import json
from pathlib import Path

from ..config import DATA_ROOT


SETTINGS_PATH = DATA_ROOT / "ui_settings.json"


def _default_work_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "2026data"


def _load() -> dict:
    if not SETTINGS_PATH.exists():
        return {}
    try:
        return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: dict) -> None:
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_ui_settings() -> dict:
    data = _load()
    wd = str(data.get("work_dir") or "").strip()
    if not wd:
        wd = str(_default_work_dir())
    return {"work_dir": wd}


def set_work_dir(path_text: str) -> dict:
    p = Path(str(path_text or "").strip()).expanduser().resolve()
    if not p.exists() or not p.is_dir():
        raise ValueError("work directory not found")
    data = _load()
    data["work_dir"] = str(p)
    _save(data)
    return {"work_dir": str(p)}


def list_receipt_files(work_dir: str, limit: int = 200) -> list[dict]:
    root = Path(work_dir).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        return []
    files = [
        p for p in root.rglob("*")
        if p.is_file()
        and p.suffix.lower() in (".xls", ".xlsx")
        and not p.name.startswith("~$")
        and ("收货清单" in p.name)
    ]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for p in files[: max(1, min(int(limit), 1000))]:
        out.append(
            {
                "path": str(p),
                "rel_path": str(p.relative_to(root)),
                "filename": p.name,
                "mtime": int(p.stat().st_mtime),
            }
        )
    return out


def pick_work_dir(initial_dir: str | None = None) -> dict:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as e:
        raise ValueError(f"file dialog unavailable: {e}")

    init_dir = str(initial_dir or get_ui_settings().get("work_dir") or _default_work_dir())
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    picked = filedialog.askdirectory(title="选择工作目录", initialdir=init_dir)
    root.destroy()
    if not picked:
        raise ValueError("cancelled")
    return set_work_dir(picked)
