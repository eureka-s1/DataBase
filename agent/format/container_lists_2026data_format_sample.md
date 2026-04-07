# 装柜清单（2026data）格式采样与“确认出仓同步”模板

采样时间：2026-04-07  
采样目录：`./2026data/装柜清单`  
基线文件：`./2026data/装柜清单/2026 4月.xlsx`（优先参考其列宽与样式）

## 1. 文件命名与选择
- 月文件常见命名：`yyyy m月.xlsx`、`yyyy m.xlsx`（例如 `2026 4月.xlsx`、`2026 2.xlsx`）。
- 同步时按“当月文件”写入；若不存在，应先提示是否创建新文件。

## 2. 追加写入结构（A-E）
- 在目标 sheet 末尾先空两行，再写本次货柜块。
- 块结构固定为：
1. 第 1 行（柜头）：
   - A：日期（`YYYY/MM/DD`，只要年月日）
   - B：柜号（container_no）
   - C：空
   - D：空
   - E：货柜固定运费（不是按当前行货量变化）
2. 第 2 行（表头）：
   - `PHONE NUMBER | NAME | CBM | CTN | FREIGHT`
3. 第 3 行起（客户行）：
   - 第一个客户为柜主（若有）
   - 后续为其他客户

## 3. 字段映射（输入字段 -> 目标列）

### 3.1 柜头行
- `container.ship_date` -> A（格式化 `YYYY/MM/DD`）
- `container.container_no` -> B
- `""` -> C
- `""` -> D
- `container.total_freight_usd_fixed` -> E（Excel 美元货币格式，不拼接 `USD` 文本）

### 3.2 表头行
- A: `PHONE NUMBER`
- B: `NAME`
- C: `CBM`
- D: `CTN`
- E: `FREIGHT`

### 3.3 客户明细行
- `customer.phone` -> A
- `customer.name` -> B
- `customer.cbm_total` -> C
- `customer.ctn_total` -> D
- `customer.freight_usd` -> E（Excel 美元货币格式）

## 4. 样式要求
- 字体：宋体。
- 对齐：全区域居中（水平+垂直）。
- 边框：本次新增块的 A-E 区域每个单元格均加细边框。
- 列宽：沿用基线文件 A-E 列宽（不要改动成新模板宽度）。
- 货币格式：FREIGHT 使用 Excel 内置美元格式（例如 `$#,##0.00`），不在单元格文本后追加 `USD`。

## 5. 实施注意事项
- 不要重复新建同月份文件；优先复用已有月文件。
- 写入完成后建议把活动 sheet 置为最后一个有写入的 sheet，便于人工核对。
- 若客户电话为空，可写空字符串，不阻塞同步。
