# Update Log

## 2026-04-04

### 1. 新增数据库设计文档
- 时间：2026-04-04
- 更新内容：创建 `agent/database_design.md`，完成需求整合、原始数据采样、完整数据库设计、技术栈、开发流程、可扩展性分析。
- 更新原因：落实 `agent/AGENTS.md` 指定的设计产出要求。

### 2. 设计文档补充（别名 + 环境）
- 时间：2026-04-04
- 更新内容：
  - 新增“客户别名映射”需求与表结构 `customer_aliases`。
  - 新增“WSL 开发 -> Windows 运行”环境约束与发布要求。
  - 在建库 SQL 与示例 SQL 中加入别名映射相关语句。
- 更新原因：响应最新业务要求，降低历史多命名导致的客户识别错误，并确保跨环境可部署。

### 3. 第一阶段代码实现（MVP 基础）
- 时间：2026-04-04
- 更新内容：
  - 新增数据库脚本：`schema/schema.sql`。
  - 新增应用代码：`app/`（配置、数据库连接、客户与别名服务、Flask 接口）。
  - 新增脚本：`scripts/init_db.py`、`scripts/import_alias_map.py`。
  - 新增跨环境文件：`.env.example`、`start_windows.bat`、`requirements.txt`。
  - 更新 `README.md` 说明启动方式和接口。
- 更新原因：进入开发实施阶段，先建立可运行基线并打通客户别名能力。
