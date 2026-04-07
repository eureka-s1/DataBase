工作流程：
1. 导入收货清单（手动上传或历史在库导入）。
2. 入库后可执行“收货同步”，把导入数据回写到客户收货清单（格式参考 `/agent/format/customer_receipts_2026data_format_sample.md`）。
3. 用户/管理员手动组柜（Draft -> Confirmed）。
4. 货柜确认出仓后执行“出仓同步”：
   - 同步到客户文件：追加“柜次出仓/运费结算标记行”。
   - 同步到装柜清单：追加当月装柜记录。
   - 具体格式参考：
     - `/agent/format/customer_receipts_2026data_format_sample.md`
     - `/agent/format/container_lists_2026data_format_sample.md`
5. 月份更新：
   - 自动模式（默认开启）按月创建新 sheet；
   - 手动模式可随时触发。
   - 月份 sheet 采用模糊匹配，`yyyy m` 与 `yyyy mm` 视为同一月份。

补充：
- 目前资金结算仍可手动参与，系统负责货柜状态流转与同步写回。
