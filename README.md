# DataBase
外贸货运数据库系统（仓储 + 拼柜 + 结算）

## 非技术用户一键启动（推荐）
如果你不熟悉命令行，按下面做即可：
1. 只需要双击 `更新.bat`（会自动下载/更新到最新版并启动）。
2. 不需要区分下载、更新、启动，统一用这一个脚本。
3. 等待黑色窗口自动安装并启动（首次会慢一些）。
4. 浏览器打开 `http://127.0.0.1:5000/login`。
5. 使用默认账号登录：`admin / admin123`。
6. 第一次登录后先改密码，再开始录入和导入数据。

注意：
- 使用期间不要关闭黑色窗口；关闭后系统会停止。
- 下次使用时，继续双击 `更新.bat` 即可。

## Windows 一键入口脚本
- 脚本：`更新.bat`
- 功能：
  - 只发这一个脚本给用户，也能首次下载 GitHub 项目
  - 后续再次双击即可从 GitHub ZIP 覆盖更新（不依赖 Git）
  - 默认更新后自动完成环境准备并启动系统（不依赖 `启动.bat`）
  - 更新时默认保留本地数据目录与常见本地文件（如 `.canyu_data`、`shipping.db`、`.env`）
- 可选参数：
```bat
更新.bat --sync-only
更新.bat --start-only
```
- `--sync-only`：仅更新，不启动。
- `--start-only`：仅启动，不更新。

排查提示：
- 如果双击后仍有异常，请查看同目录日志：`更新.log`。
- 常见原因：网络受限、公司电脑拦截 PowerShell 下载、GitHub 连接不稳定。

## 一键打包
### Windows（推荐）
1. 双击 `package_release.bat`
2. 打包完成后，到 `dist` 目录取 `DataBase_release_时间戳.zip`
3. 解压 zip ，按包内 `WINDOWS_QUICK_START.txt` 启动

### 命令行方式（WSL/Linux/Windows 通用）
```bash
python scripts/package_release.py
```

可选参数：
- `--include-2026data`：把 `2026data` 一并打包（默认不包含，避免包太大）
- `--name 自定义名称`：自定义压缩包名称

## 功能完成状态
已实现并可运行：
- 账号与权限基础：管理员默认账号、登录、改密、退出
- 客户与价格：客户档案、别名映射、价格规则
- 入库管理：手工入库、修改、删除（仅在库）、在库查询、体积自动计算
- Excel 导入：收货清单上传、预览、执行导入、批次撤销、单条撤销
- 拼柜管理：建柜、装柜、容量校验、确认出柜、撤回柜次
- 费用结算：定金流水、按柜次单价自动计费、自动抵扣、生成/过账/撤销过账
- 报表导出：入库单、库存单、柜单、账单、总账（Excel；账单/柜单支持 PDF）
- 查询增强：客户货物查询（是否入柜/所在柜/发货明细）、拼柜明细展开查看
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
更新.bat
```

浏览器访问：
- `http://127.0.0.1:5000/login`
- 默认管理员：`admin / admin123`

## Windows 使用指南
### 1. 环境准备
- Windows 10/11（64 位）。
- 安装 Python 3.10+，并确保命令行可用 `py` 命令。
- 建议将项目放在本地磁盘目录（如 `D:\DataBase`），不要放在只读目录。

### 2. 首次启动
1. 双击 `更新.bat`，或在 CMD/PowerShell 中执行：
```bat
更新.bat
```
2. 脚本会自动完成：
- 自动下载/更新代码
- 创建虚拟环境 `.venv`
- 安装依赖 `requirements.txt`
- 初始化数据库
- 启动 Flask 服务
3. 打开浏览器访问 `http://127.0.0.1:5000/login`。

### 3. Windows 数据目录（重要）
- 默认数据目录：`%APPDATA%\CanyuShipping`
- 默认数据库文件：`%APPDATA%\CanyuShipping\shipping.db`
- 默认备份目录：`%APPDATA%\CanyuShipping\backups`

可通过环境变量自定义路径（见 `.env.example`）：
- `CANYU_DATA_DIR`
- `CANYU_DB_PATH`
- `CANYU_BACKUP_DIR`

### 4. 关闭与下次启动
- 在启动窗口按 `Ctrl + C` 停止服务。
- 下次仍执行 `更新.bat` 即可。

### 5. 常见问题
- `py 不是内部或外部命令`：
  - 重新安装 Python，勾选“Add Python to PATH”。
- 依赖安装失败：
  - 检查网络，或更换 pip 源后重试。
- 5000 端口被占用：
  - 关闭占用程序，或修改 [run.py](/home/eureka/database/DataBase/run.py) 中端口后重启。

## Windows 用户须知
- 首次登录后请立刻修改默认管理员密码 `admin123`。
- `shipping.db` 是核心业务数据文件，请勿手工编辑或直接删除。
- 每次重要操作后建议执行一次备份（`POST /backup` 或 `python scripts/backup_db.py`）。
- 升级代码前先备份数据库，再执行更新，避免数据风险。
- 导入 Excel 建议先走 `dry_run` 预演，再执行正式导入。
- 若使用杀毒软件，请将项目目录和数据目录加入信任，避免锁库导致 SQLite 写入失败。

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
