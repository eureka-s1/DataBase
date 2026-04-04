# 导入测试记录模板（每轮一份）

## 0. 基本信息
- 记录编号：`IMP-YYYYMMDD-XX`
- 记录日期：
- 执行人：
- 环境：`Windows / WSL / Linux`
- 系统版本（Git Commit）：
- 数据库路径：
- 源文件路径：
- 源文件类型：`xlsx/xls`

## 1. 本轮目标
- 本轮类型：`开发联调 / 导入测试 / 正式导入`
- 目标说明（1-3 句话）：
- 通过标准（量化）：

## 2. Gate A - Preview 结果
- 接口：`POST /import/inbound/preview`
- 请求参数：
```json
{
  "file_path": ""
}
```
- 返回摘要：
  - `header_row`：
  - `field_mapping` 是否合理：`是/否`
  - 抽样行核对结论：`通过/不通过`
- 结论：`通过/不通过`
- 问题清单：

## 3. Gate B - Dry-run 结果
- 接口：`POST /import/inbound/execute`（`dry_run=true`）
- 请求参数：
```json
{
  "file_path": "",
  "inbound_date": "YYYY-MM-DD",
  "dry_run": true
}
```
- 返回摘要：
  - `batch_id`：
  - `total_rows`：
  - `success_rows`：
  - `failed_rows`：
  - 失败率（failed/total）：
- 失败原因分类（按数量）：
  - 客户未映射：
  - 必填字段缺失：
  - 格式异常：
  - 其他：
- 结论：`通过/不通过`

## 4. 修复动作（若 Gate B 不通过）
- 是否补充别名：`是/否`
- 补充内容：
- 是否修改导入规则：`是/否`
- 代码变更文件：
- 修复后是否重跑 Dry-run：`是/否`
- 重跑结果：

## 5. Gate C - 正式导入结果（如执行）
- 前置备份：
  - 是否完成备份：`是/否`
  - 备份文件：
  - 备份时间：
- 正式导入请求参数：
```json
{
  "file_path": "",
  "inbound_date": "YYYY-MM-DD",
  "dry_run": false
}
```
- 返回摘要：
  - `batch_id`：
  - `total_rows`：
  - `success_rows`：
  - `failed_rows`：
- 结论：`成功/失败/回滚`

## 6. 导入后核对（必填）
- 批次核对（`import_batches`）：
- 在库抽查（客户、品名、CBM、状态）：
- 查询页可见性核对：
- 导出链路核对：

## 7. 问题与风险
- 本轮问题列表（按严重度）：
- 影响范围：
- 临时绕过方案：
- 根因判断：

## 8. 后续动作
- 下一轮要做的 1-3 项任务：
1.
2.
3.

- 是否允许进入下一阶段：`是/否`
- 审核人（如有）：
