from __future__ import annotations

import argparse
import hashlib
from datetime import datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


EXCLUDE_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "dist",
    "backups",
    "exports",
    ".canyu_data",
}

EXCLUDE_SUFFIXES = {".pyc", ".pyo"}


def should_skip(path: Path, include_2026data: bool) -> bool:
    parts = set(path.parts)
    if parts & EXCLUDE_DIRS:
        return True
    if not include_2026data and "2026data" in parts:
        return True
    if path.suffix.lower() in EXCLUDE_SUFFIXES:
        return True
    return False


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_release_note() -> str:
    return (
        "DataBase Windows 一键启动说明\n"
        "================================\n\n"
        "给用户的使用步骤：\n"
        "1. 先把压缩包完整解压到本地目录（如 D:\\DataBase）。\n"
        "2. 双击 start_windows.bat 启动系统。\n"
        "3. 首次启动会自动安装依赖，可能需要 1-5 分钟，请耐心等待。\n"
        "4. 看到启动提示后，打开浏览器访问：http://127.0.0.1:5000/login\n"
        "5. 默认账号：admin\n"
        "   默认密码：admin123\n"
        "6. 首次登录后请立即修改密码。\n\n"
        "使用注意：\n"
        "- 系统运行期间，不要关闭黑色命令窗口；关闭后系统会停止。\n"
        "- 下次使用时，再次双击 start_windows.bat 即可。\n"
        "- 默认数据目录：%APPDATA%\\CanyuShipping\n"
        "- 核心数据文件：%APPDATA%\\CanyuShipping\\shipping.db\n"
        "- 请定期备份数据，升级前务必先备份。\n\n"
        "常见问题：\n"
        "- 如果提示“py 不是内部或外部命令”，请重新安装 Python，并勾选 Add Python to PATH。\n"
        "- 如果 5000 端口被占用，请先关闭占用该端口的程序再重试。\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Package project into a shareable ZIP release.")
    parser.add_argument("--name", help="Release file base name (without .zip)")
    parser.add_argument("--include-2026data", action="store_true", help="Include 2026data directory")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    dist_dir = root / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = args.name or f"DataBase_release_{ts}"
    zip_path = dist_dir / f"{base_name}.zip"
    note_path = dist_dir / "WINDOWS_QUICK_START.txt"

    files: list[Path] = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if should_skip(rel, include_2026data=args.include_2026data):
            continue
        files.append(p)

    note_path.write_text(build_release_note(), encoding="utf-8-sig")

    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as zf:
        for f in files:
            zf.write(f, arcname=f.relative_to(root).as_posix())
        zf.write(note_path, arcname="WINDOWS_QUICK_START.txt")

    note_path.unlink(missing_ok=True)

    digest = sha256_file(zip_path)
    sha_path = dist_dir / f"{base_name}.sha256"
    sha_path.write_text(f"{digest}  {zip_path.name}\n", encoding="utf-8")

    print(f"release zip: {zip_path}")
    print(f"sha256: {sha_path}")
    print(f"files packed: {len(files)}")
    if not args.include_2026data:
        print("2026data excluded by default (use --include-2026data to include).")


if __name__ == "__main__":
    main()
