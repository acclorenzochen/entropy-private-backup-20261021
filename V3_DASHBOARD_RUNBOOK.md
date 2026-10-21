# V3 只读看板运行与回滚

普通 V3（`strategy_mode=basis`）正常退出只看本轮两腿全仓可执行价格毛盈亏
是否达到 `exit_min_gross_profit_usd`，默认 `0.03 USD`。按本轮真实成交数量、
成交开仓价与部分平仓价累计价格现金流，再加剩余全仓当前盘口深度的平仓现金流；
不使用 mark 浮盈，不混入旧轮。手续费、资金费与额外执行缓冲不参与该门槛，
仍保留在记账、费用后估值与复盘中。旧配置的 `exit_reversion_bps`、
`exit_loss_allowance_usd` 仍可读取，但不再允许普通退出；回归量仅供诊断。
盘口过期或成交价格证据缺失时不触发普通退出。已发生的敞口/残仓恢复仍优先。
达到门槛后先持久化 `BASIS_EXIT` 退出意图，再并发提交两腿 reduce-only IOC
主动吃单，保留各自执行房间与硬限价保护。IOC 可能部分或零成交；未知结果先按
原订单 ID 查证，终态成交与权威仓位相符后只补实际残量，直到确认两腿归零。
锁定后不再重新等待毛收益门槛；重启后继续恢复退出。盘口失效的腿等新盘口，
已有新鲜盘口的另一腿可以继续平仓。单周期亏损与批次预算仍可锁住后续开仓，
这些 basis 锁本身不强制平仓，也不会阻塞已锁定的退出。
`funding_hedge` 保留两腿各至少两次真实资金费结算后正常退出的专属规则，
以及原有清算距离、数据缺失、资金费逆转支出与轮次亏损风控。

本次只修改源码、示例配置与只读看板；没有改实盘配置、日志或 SQLite。
V3 在启动时读取配置，不热加载本次变更。用户须在自己的正常运维窗口手动
让新进程加载新代码，核对 effective manifest 中的策略模式和
`exit_min_gross_profit_usd=0.03`；不要仅为看板更新而重启持仓进程。

`tools/v3_opportunities.py` 回放目前仍是旧的顺序退出模型，输出标记
`execution_model=legacy_sequential_exit`、`live_execution_parity=false`。
其利润、耗时、完整周期率和参数筛选不能作为新版并发退出的执行验证。

新看板使用独立 `entropy_arb.dashboard_api.v3_app:create_app`；旧
`entropy_arb.dashboard_api.app:app` 不参与 V3 指标，也不会被自动替换。

本地启动：

```bash
./scripts/run_v3_dashboard_api.sh
```

默认仅监听 `127.0.0.1:8766`，`--workers 1`，页面和 `/api/v3/*` 同源。
首次启动会在独立 `logs/dashboard-v3/readmodel.sqlite3` 建立可删除的
recorder 读缓存；生产 `logs/v3/journal.sqlite3` 仅以 SQLite `mode=ro`
和 `query_only` 打开。缓存回填期间历史页明确显示覆盖未完成。
固定 q 图按所选范围流式读取该独立缓存，服务端最多返回 1200 个
保留局部极值的显示点；图中空白和覆盖起止时间表示实际未采样，
不等于“没有机会”。较长历史窗的首次重报价可能需要数秒。

可由服务环境指定以下**服务端**路径（浏览器不能提交路径）：
`V3_JOURNAL_PATH`、`V3_LIVE_STATUS_PATH`、`V3_LIVE_MANIFEST_PATH`、
`V3_LIVE_DIAGNOSTICS_PATH`、`V3_RECORDER_STATUS_PATH`、
`V3_RECORDER_MANIFEST_PATH`、`V3_RECORDER_DATA_DIR`、`V3_REPORTS_DIR`、
`DASHBOARD_V3_CACHE_DIR`。默认按当前项目的 `logs/v3/*` 映射；部署前
核对实际 live/recorder 启动参数、身份、时间戳和文件权限。

公网代理默认关闭 `/api/v3/*`。`dashboard/worker` 只允许白名单 GET/HEAD；
即便配置了 upstream token，也**不能**把它当作访客认证。只有部署层
真正启用并验证访客访问保护，才可设置
`DASHBOARD_PRIVATE_ACCESS_CONFIRMED=true` 打开代理。不得用 CORS 代替认证。

当前 live release 未发布细分决策诊断文件时，页面只展示 status 已记录的
概括原因，逐方向拒绝层级仍为未知；不能用 recorder 代填。部署该诊断
需要单独的 sender 补丁、策略差分测试和发布审批，不因看板上线自动重启。
离线研究报告未生成时明确显示 `INSUFFICIENT_DATA`，不推荐参数。
当前平仓估值只有 live status 提供新鲜、同 cycle 的可执行数值时才展示；
缺失不显示为 0。较早 schema、历史回填、缺成交锚点等都保留缺口状态。

验收命令：

```bash
.venv/bin/python -m pytest -q tests_v3/test_dashboard_v3.py tests_v3/test_economics.py
node --test dashboard/worker/index.test.mjs
cd dashboard && npm run build
```

端到端测试使用已有 Playwright 运行时，按 `dashboard/e2e/v3-real-smoke.mjs`
的 `PLAYWRIGHT_MODULE`/`DASHBOARD_URL` 指向本地只读 API；不发送验证订单。

回滚仅停止 V3 看板服务或切回旧页面/代理，不修改、迁移或回滚生产 Journal，
不改策略参数，不重启 live/recorder。删除独立缓存可重建，但历史回填需时间。
