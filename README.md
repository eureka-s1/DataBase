# DataBase
外贸货运数据库系统（仓储 + 拼柜 + 结算）

## 当前实现（MVP 基础）
- SQLite 数据库初始化（`schema/schema.sql`）
- Flask 基础服务（`run.py`）
- 客户与别名管理（支持同一客户多个姓名映射）
- WSL 开发 / Windows 运行路径兼容配置

## 快速启动（WSL/Linux）
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/init_db.py
python run.py
```

## 快速启动（Windows）
```bat
start_windows.bat
```

## 关键接口
- `GET /health`
- `POST /init-db`
- `GET /customers`
- `POST /customers`
- `POST /customer-aliases`
- `GET /customer-resolve?name=...`

## 别名对照表导入
CSV 需要包含以下列之一：
- `alias_name + customer_id`
- `alias_name + customer_name`

导入命令：
```bash
python scripts/import_alias_map.py /path/to/alias_map.csv
```
