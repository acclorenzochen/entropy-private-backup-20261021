# V3 看板双腿平仓

> 需求已更新为三个独立按钮：启动、停止、平仓。下述实现是旧的一次性“平仓并保持 MANUAL_STOP”方案，仅保留为已验证的退出工具与展示组件；不能满足新运行意图语义，暂不激活。新接口与核心接线边界见 [V3_ENGINE_CONTROL_CONTRACT.md](V3_ENGINE_CONTROL_CONTRACT.md)。

当前展示入口：<http://127.0.0.1:5186/#/overview>。8776 保持 GET-only；8786 是独立本地控制服务。一次性 CLI 的 HTTP 实盘入口现已禁用，旧 V3_EXIT_CONTROL_ENABLED=1 也不能开启；需先接入引擎 owner 协议。本次未启动实盘控制、查询真实账户预检、发送真实订单、启停交易或解除 MANUAL_STOP。

当前 SNDK 周期 `v3-1791722378735-88119c98` 的退出记录已权威归零并核账，净 −0.004772 U。旧 `status.json` 的 HOLDING 保留为历史来源；看板按同周期、批次、pair、配置、账户绑定、订单与账本证据展示“退出时已平仓”，带观测时间。它不证明当前账户实时仓位。没有未结束周期时按钮禁用。

## 操作流程

1. 在当前持仓卡片点击“双腿平仓”。打开面板只创建本地会话并读取账户、周期、两腿数量；不会写 MANUAL_STOP 或执行退出。
2. 面板显示标的、两腿有符号持仓/平仓方向、权威观测时间、账户绑定摘要、15 秒确认有效期、预计毛盈亏与费用口径。只读预检并发读取实际标的的双边深度，按剩余数量逐档估算退出价格现金，加上本周期已确认成交的纯价格现金；毛额不扣手续费或资金费。报价超时、过期、深度不足、未决订单或成交价格缺失时显示“无法估算”；费用/资金费显示“待核算”，不补零。最终确认只复核账户与订单绑定，不重复读报价拖延确认。
3. 最终点击“确认平仓两腿”才启动退出 worker。worker 在原共享账户锁内重读并复核全部确认凭证，变化/过期则拒绝，随后复用 `v3_exit_only.py` 原并发 reduce-only、正常账本与 UNKNOWN 恢复。
4. 进度原地显示每腿累计成交、剩余量及来源，区分执行中、部分成交、UNKNOWN、完成待核算、已核算。响应丢失时沿用同一幂等键取回操作；收起再打开恢复同一操作。UNKNOWN 先核对原订单，不重复发送未知订单。只有退出工具确认权威归零、无待决订单/活动周期，才显示已平仓；净额仅在最终核算证据齐全时显示。
5. 不会自动启动策略、解除 MANUAL_STOP、取消未知订单或终止持锁进程。普通交易进程持锁时预检明确拒绝；用户需在原终端停止并等待锁释放。当前只支持 ordinary basis，资金费周期需要其专用管理者。

## 已废弃的旧激活方式（不要执行）

以下旧命令仅记录原一次性退出方案，不满足新三按钮语义，入口已禁用；不要执行。新启动流程等待 owner 接线。无需新增持久凭据，沿用现有本地 `.env`。公开账户绑定必须由操作者核对，不能依赖旧账本身份推断。

```sh
cd /Users/mingyuchen/Documents/ChatGPT/entropy
V3_EXIT_CONTROL_ENABLED=1 \
V3_EXIT_HL_ACCOUNT='<已核对的真实公开 0x 查询地址>' \
V3_EXIT_LIGHTER_ACCOUNT_INDEX='<已核对的 Lighter 整数账户 index>' \
V3_EXIT_UI_ORIGIN='http://127.0.0.1:5186' \
./scripts/run_v3_exit_control.sh
```

默认固定参数与用户已安装退出工具的交付 README 一致：SNDK/Lighter、config.yaml、.env、config.v3.edge-next.json、default、logs/v3/batch.authorization.20260923-002.json、logs/v3/journal.sqlite3、manual-exit-status.json、observer.jsonl、reference_checkpoint.json。若部署实际路径不同，在服务器启动时显式设置 V3_EXIT_CONFIG、V3_EXIT_ENV_FILE、V3_EXIT_V3_CONFIG、V3_EXIT_DEPLOYMENT、V3_EXIT_AUTHORIZATION、V3_EXIT_JOURNAL、V3_EXIT_STATUS、V3_EXIT_OBSERVER、V3_EXIT_CHECKPOINT；浏览器不能指定路径或工具参数。修改来源必须同时配置只读看板来源；不要混用账户/账本。

前端默认将 `/api/v3-control` 转发到 127.0.0.1:8786。当前 5186 已重启加载该路由；未启动控制服务时界面安全拒绝。显示开发服务用 `V3_DASHBOARD_API_URL=http://127.0.0.1:8776` 继续读取现有只读 API。

8786 必须保持 loopback、单 worker、无可信转发头；只接受配置的精确 Origin 与 loopback Host，POST JSON 需 HttpOnly/SameSite=Strict 会话及 CSRF token。短期 receipt 只存在服务端；确认文件为临时目录内 0600 文件。命令使用固定 argv，不使用 shell。不得将该端点加入公网代理、Sites 或开放 CORS。对外开放或新增凭据需要另行评估/批准。

服务重启时 HTTP 会话不恢复。执行子进程不会因 HTTP 超时被杀掉；共享账户锁与原订单 ID 仍是最终单管理者/去重保障。重启后先看持久退出记录与订单状态，重新预检同一周期，不自动重发确认。预检遇到未决开仓、SUBMITTING 或缺失/畸形账户证据时安全拒绝。

## 代码与验证

- 前端：ExitPosition.tsx、ProfitOverview.tsx、OverviewPage.tsx、types.ts、styles.css、vite.config.ts。
- 只读投影：v3_readmodel.py、v3_projection.py、v3_deployments.py。
- 控制层：v3_exit_app.py、tools/v3_exit_control.py、run_v3_exit_control.sh；旧 v3_exit_only.py 仅增加可选锁内 pre_arm_check，默认行为不变。
- 无 runtime/core/venue 改动；不涉及另一线程的 2.5 bp 与 A/B 核心变更。

离线验证：

```sh
.venv/bin/python -m pytest tests_v3/test_exit_control_api.py tests_v3/test_exit_control_worker.py \
  tests_v3/test_dashboard_manual_exit.py tests_v3/test_dashboard_v3.py \
  tests_v3/test_dashboard_profit.py tests_v3/test_dashboard_deployments.py \
  tests_v3/test_dashboard_funding.py tests_v3/test_dashboard_workflow.py \
  tests_v3/test_parallel_exit.py tests_v3/test_normal_exit.py -q
cd dashboard
./node_modules/.bin/tsc -b
./node_modules/.bin/vite build --outDir /tmp/entropy-exit-delivery/build --emptyOutDir
```

浏览器 mock：`dashboard/e2e/v3-exit-smoke.mjs` 读取 `/tmp/entropy-exit-delivery/live-overview.json` 与 `live-deployments.json` 的只读基线；所有控制请求由 Playwright 拦截，模拟订单不连接真实服务。设置 PLAYWRIGHT_MODULE 为已有运行时路径后执行。截图、构建、测试结果与本次补丁保存 `/tmp/entropy-exit-delivery/`。
