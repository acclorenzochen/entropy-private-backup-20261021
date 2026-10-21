# SNDK 普通 basis 待应用配置

`config.v3.edge-next.json` 是独立待应用配置。它沿用当前 SNDK / Lighter 的 basis 参数，仅修改两项：

| 字段 | 当前旧配置的有效值 | 待应用值 |
| --- | --- | --- |
| `exit_min_gross_profit_usd` | `0.03`（旧文件省略，使用默认值） | `0.05` |
| `single_cycle_review_loss_usd` | `0.10` | `null`（明确禁用单圈复核门槛） |

普通退出判断为：本周期实际成交的有符号价格现金，加剩余双腿按新鲜完整盘口深度可执行的价格现金，毛收益 **≥0.05 USD** 时允许退出。0.0499 不触发，0.05 触发。手续费与资金费不扣入这个毛收益门槛，最终净收益仍可能低于它；IOC 实际成交价格也可能与触发时盘口不同。普通退出继续并发提交双腿 reduce-only IOC，UNKNOWN 订单和残仓仍走原核实、修复路径。

`null` 仅取消普通 basis 的 −0.10 USD 单圈浮亏/亏损复核停开仓门槛。原批次 3 USD 亏损预算、100 次持仓周期、200 USD 名义上限及其他独立门禁保留。批次预算的活动浮亏检查仍可能使用 `SINGLE_CYCLE_REVIEW_LOSS` 原因名，需结合记录的 `close_threshold` 判断实际触发门槛。NBIS funding 模式的退出参数不变。

本次离线修改未改当前 `config.v3.edge.json`、旧授权、生产锁或 Journal，也未解锁、重启或下单。V3 仅启动时读取配置；旧 profile 省略字段时仍保持 0.03 / 0.10 默认值。运行中的进程不会加载这个文件或本次代码改动。

## 用户手动应用顺序

以下命令供用户自行执行，代理未执行。务必沿用原账户、部署、Journal、共享进程锁及原授权 `logs/v3/batch.authorization.20260923-002.json`；不要另建批次、删锁文件或清空数据库。若当前启动另有环境变量或路径覆盖，先核对实际启动命令，并在下面各步保持相同。

1. 验证待应用 profile；输出隔离到 `/tmp`，不打开交易 SDK、生产 Journal 或进程锁。这一步只验证配置，不证明空仓或旧批次可解锁。

   ```bash
   cd /Users/mingyuchen/Documents/ChatGPT/entropy
   .venv/bin/python main_v3.py --symbol SNDK --hedge lighter \
     --config config.yaml --env-file .env \
     --v3-config config.v3.edge-next.json --deployment default \
     --live --batch-authorization logs/v3/batch.authorization.20260923-002.json \
     --validate-only --status-json /tmp/sndk-profile-validation/status.json
   ```

   检查 `/tmp/sndk-profile-validation/status.effective_manifest.json` 的 `v3_config_source.path` 指向待应用文件的绝对路径，`settings.exit_min_gross_profit_usd` 为 `"0.05"`，`settings.single_cycle_review_loss_usd` 为 `null`，`settings.strategy_mode` 为 `"basis"`。

2. 用户确认双腿空仓、无未决订单后，正常结束原实盘进程，等待其退出和共享进程锁释放。若仍有仓位、UNKNOWN 订单或未完成核账，应先查明原状态；不要强制删除锁来并行启动。

3. 仅执行手动解锁，并等待该命令结束：

   ```bash
   .venv/bin/python main_v3.py --symbol SNDK --hedge lighter \
     --config config.yaml --env-file .env \
     --v3-config config.v3.edge-next.json --deployment default \
     --live --batch-authorization logs/v3/batch.authorization.20260923-002.json \
     --journal logs/v3/journal.sqlite3 --status-json logs/v3/status.json \
     --observer-jsonl logs/v3/observer.jsonl \
     --reference-checkpoint logs/v3/reference_checkpoint.json \
     --reopen-entries
   ```

   此命令连接真实账户，要求 ≤10 秒的可信双腿 0/0 证据、无活动周期/未决订单/未完成或非最终核账，且原批次次数和亏损预算未耗尽。满足条件才清除旧停开仓 latch；确认 `logs/v3/status.json` 的 `current_state` 为 `UNLOCKED` 且命令以退出码 0 结束。标准输出是批次 JSON。它不进入交易循环，也不设置杠杆。若状态为 `UNLOCK_REFUSED` 或发生进程锁冲突，停止此应用流程，检查明确拒绝原因。解锁不重置历史盈亏、次数或预算。

4. 确认解锁命令已退出后，以相同 profile 和授权正常启动：

   ```bash
   .venv/bin/python main_v3.py --symbol SNDK --hedge lighter \
     --config config.yaml --env-file .env \
     --v3-config config.v3.edge-next.json --deployment default \
     --live --batch-authorization logs/v3/batch.authorization.20260923-002.json \
     --journal logs/v3/journal.sqlite3 --status-json logs/v3/status.json \
     --observer-jsonl logs/v3/observer.jsonl \
     --reference-checkpoint logs/v3/reference_checkpoint.json
   ```

   正常实盘启动仍按既有逻辑设置原配置杠杆。本次不更改杠杆值。检查新 `logs/v3/status.effective_manifest.json` 的 `process_id` / `start_ts`、`v3_config_source.path` / `sha256` 和上述三个设置字段；同一 PID 的当前状态也须正常，不能为 `HALTED` 或启动拒绝。manifest 在账户初始化前写入，只证明该进程解析的配置。持仓状态中的 `normal_exit_min_gross_profit_usd` 应为 `"0.05"`。不能用磁盘 profile 存在或旧看板状态代替确认。

如继续使用 `scripts/run_v3_live.sh`，每次显式传入 `--v3-config config.v3.edge-next.json`，或明确设置 `ENTROPY_V3_CONFIG` 指向它；脚本默认仍指向旧 `config.v3.edge.json`。命令行同名参数以最后一个为准。连接配置中的旧策略字段不会覆盖这两个 V3 字段。

展示限制：现有系统参数表会将 `null` 显示为 `—`，不能据此区分禁用与证据缺失；`exit_min_gross_profit_usd` 也尚未归入“退出与风险”组。本次只报告这些展示问题，未改看板，应用确认请读取实际 manifest。
