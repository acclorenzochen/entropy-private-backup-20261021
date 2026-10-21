# NBIS 多轮对冲手动启动教程

首次运行只需选择批次轮数、手动确认实盘授权，再执行启动命令。助手自动填写固定参数、生成独立批次 ID，并在启动前运行无签名公共预检。

固定参数是 Entropy `io:NBIS` 对 Lighter 主网 NBIS，双腿 7 倍，每腿名义金额上限 200U。没有固定 5 轮上限；批次轮数仍需选定一个正整数。整轮和批次金额止损都未设置，0.5U 仅限制预计剩余净资金费支出，**不是整轮最大亏损**。离线测试通过不代表策略盈利或私有结算账本已验证。

## 开始前确认账户状态

在两家交易所核对：旧 SNDK 已停止、没有遗留仓位或未完成订单，NBIS 也没有需要人工处理的旧仓位。首次启动与异常重启不同；有仓位或未知订单时，不要套用首次启动步骤换批次重开。

沿用项目已有 `.env`，不用重新粘贴私钥。不要把 `.env` 内容或私钥发到聊天。真实账户费用、资金费结算记录和迟入账表现仍需核实；公共预检不验证这些内容。未核实时先停在检查阶段，不把 `ok: true` 当作实盘就绪。

## 第一步 生成待确认授权

打开 macOS 终端，粘贴：

```bash
cd /Users/mingyuchen/Documents/ChatGPT/entropy
.venv/bin/python scripts/nbis_funding.py prepare
```

它会询问“本批次最多运行多少轮”。输入你选择的正整数并回车，没有默认 5 轮。生成的 `nbis-funding-authorization.json` 包含独立批次 ID，`approved_live` 仍为 `false`，这一步不会签名、设置杠杆或下单。

如果提示“授权文件已存在”，不要删除或覆盖；直接检查原文件。重启必须沿用原 batch_id 和累计轮数，不能用重新生成授权来绕过停止状态。

## 第二步 手动确认授权

运行：

```bash
open -a TextEdit nbis-funding-authorization.json
```

核对 `max_position_cycles` 是刚才选择的轮数，`max_notional_usd` 是 `"200"`，`loss_budget_usd` 是 `null`。决定实盘后，只把这一行：

```json
"approved_live": false
```

改成：

```json
"approved_live": true
```

按 Command S 保存并关闭编辑器。`true` 不加引号，其余批次 ID、配对标识和参数保持不变。

## 第三步 启动 NBIS

在同一个终端运行：

```bash
.venv/bin/python scripts/nbis_funding.py start
```

这个命令由你手动执行后，会先做公共预检；失败则不会进入实盘。预检通过后，进入真实账户路径，可以设置双方 7 倍杠杆并发送真实订单。账户互斥、旧持仓和未知订单保护仍生效，不会接管无证据的旧仓位。

这是前台运行，保持该终端和电脑运行，不要关窗口或让电脑休眠。命令没有额外的 `--max-new-cycles 5`；实际开仓次数受授权文件及已有累计次数约束。不要在另一个终端重复启动同一实盘实例。

如果只想检查公共市场、不启动实盘，改用：

```bash
.venv/bin/python scripts/nbis_funding.py
```

默认只做无签名公共预检，不读取 `.env` 文件、不改杠杆、不下单。`ok: true` 表示公共检查通过；`live_ready: false` 是预期值，因为该模式不验证私有账户和账本。

## 查看 NBIS 是否进入运行阶段

另开一个终端，运行：

```bash
cd /Users/mingyuchen/Documents/ChatGPT/entropy
.venv/bin/python scripts/nbis_funding.py status
```

它只读 `logs/v3/nbis/status.json`，不会用 SNDK 的旧状态冒充 NBIS。输出是最后发布的状态；进程退出后文件仍可能保留，需结合启动终端和 `source_ts` 是否持续更新判断是否仍在运行。

| 状态 | 含义 |
| --- | --- |
| `STARTING` 或 `RECONCILING` | 正在加载或恢复，不能据此认为已开仓 |
| `READY` | 正在等待具备有效行情、资金费及平仓证据的开仓条件 |
| `HOLDING_FUNDING` | 正在持有对冲，等待真实结算证据 |
| `EXITING` | 正在执行双腿退出，可能仍有敞口或未完成订单 |
| `ACCOUNTING_PENDING` | 前一轮的成交、费用或平仓后资金费查询证据不完整，暂不开下一轮 |
| `BATCH_COMPLETE` | 已达到授权轮数，不再新开仓，仍继续迟入账重核 |
| `BATCH_CLOSED` 或 `RECONCILE_REQUIRED` | 已停止新开仓或需要恢复；看 `reason`，不要靠重启换批次绕过 |

`funding_state.settlement_counts` 是两边分别确认的结算次数。完成一轮后，两边归零并完成费用核对才允许下一轮；资金费账务保持 provisional 不等于“未平仓”，也不等于已最终核算。

## 看板仍显示 SNDK 时

原来的 `http://127.0.0.1:8767/` 不会自动切换到 NBIS。最简单是先以上面的 NBIS 状态命令为准。

如需独立 NBIS 看板，在另一个终端手动运行以下只读服务；它不启动交易，也不替换旧的 8767 服务：

```bash
cd /Users/mingyuchen/Documents/ChatGPT/entropy
V3_DATA_DIR=logs/v3/nbis DASHBOARD_V3_CACHE_DIR=logs/dashboard-v3-nbis \
  .venv/bin/python -m uvicorn entropy_arb.dashboard_api.v3_app:create_app \
  --factory --workers 1 --host 127.0.0.1 --port 8768 --no-access-log
```

然后打开 [NBIS 只读看板](http://127.0.0.1:8768/)。如果提示端口占用，不要结束不明进程；先使用状态命令，核对已有服务。看板缺失 recorder 数据时可能没有完整行情图，不影响状态文件的独立性。

## 出错或需要停止时

- **授权未批准**：确认改的是 `nbis-funding-authorization.json`，不是 `.example.json`，且已保存布尔值 `true`。
- **公共预检失败或行情过期**：不进入实盘。检查网络并稍后重试，不放宽新鲜度阈值。
- **共享账户锁被占用**：说明其他实例仍在运行。不要删除锁文件或强行并行启动。
- **提前退出或缺失结算证据**：按 `reason` 处理；不修改 batch_id、不删除 Journal、不自动反向开仓。
- **需要停止**：不要把“关闭程序”当作“已经平仓”。Ctrl C 会停止进程，不保证自动平仓。只有确认双腿为零、没有未完成订单，才按 Ctrl C 退出；持仓或状态不明时，先在交易所确认并处理风险，再停止监控。

停止后如需恢复，保留授权文件和 `logs/v3/nbis/journal.sqlite3`，不要重新执行 `prepare` 生成新批次。既有停止状态不会因重启自动解除。

详细参数及证据边界见 [NBIS 资金费运行说明](NBIS_FUNDING_RUNBOOK.md)。
