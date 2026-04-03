# DataBase
外贸货运数据库系统（仓储 + 拼柜 + 结算）

## 功能完成状态
已实现并可运行：
- 账号与权限基础：管理员默认账号、登录、改密、退出
- 客户与价格：客户档案、别名映射、价格规则
- 入库管理：手工入库、修改、删除（仅在库）、在库查询、体积自动计算
- Excel 导入：收货清单预览与执行接口，支持 `dry_run`（当前停在导入前，可先预览与演练）
- 拼柜管理：建柜、装柜、容量校验、确认出柜、撤回柜次
- 费用结算：定金流水、按客户体积分摊运费、自动抵扣、生成并过账账单
- 报表导出：日入库、在库库存、客户总账导出 Excel
- 备份：SQLite 手动备份并记录日志
- 基础 Web 页面：登录页、首页、业务面板

## 快速启动（WSL/Linux）
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/init_db.py
python run.py
```

浏览器访问：
- `http://127.0.0.1:5000/login`
- 默认管理员：`admin / admin123`

## 快速启动（Windows）
```bat
start_windows.bat
```

## 关键 API
### 认证
- `POST /login`
- `POST /logout`
- `POST /change-password`

### 客户与价格
- `GET /customers`
- `POST /customers`
- `POST /customer-aliases`
- `GET /customer-resolve?name=...`
- `POST /price-rules`

### 入库
- `GET /inbound-items`
- `POST /inbound-items`
- `PUT /inbound-items/<item_id>`
- `DELETE /inbound-items/<item_id>`

### 拼柜
- `GET /containers`
- `POST /containers`
- `GET /containers/<id>/usage`
- `POST /containers/<id>/items`
- `DELETE /containers/<id>/items/<item_id>`
- `POST /containers/<id>/confirm`
- `POST /containers/<id>/revoke`

### 结算
- `POST /payments`
- `POST /settlements/generate`
- `POST /settlements/<statement_id>/post`
- `GET /ledger`

### 导出与备份
- `POST /exports/daily-inbound`
- `POST /exports/inventory`
- `POST /exports/ledger`
- `POST /exports/statement/<statement_id>`（`format`: `xlsx|pdf`）
- `POST /backup`

### 导入（当前建议先 dry-run）
- `POST /import/inbound/preview`
- `POST /import/inbound/execute`

## 别名对照表导入
CSV 需要包含以下列之一：
- `alias_name + customer_id`
- `alias_name + customer_name`

导入命令：
```bash
python scripts/import_alias_map.py /path/to/alias_map.csv
```

## 冒烟测试（不导入真实业务数据）
```bash
python scripts/smoke_test.py
```

## 目录结构
- `schema/schema.sql`：数据库结构
- `app/`：Flask 应用与业务服务
- `scripts/init_db.py`：初始化数据库
- `scripts/import_alias_map.py`：导入别名对照表
- `scripts/smoke_test.py`：端到端冒烟验证
- `agent/database_design.md`：数据库设计文档
- `agent/update_log.md`：开发更新日志
