# V3 启动 / 停止 / 平仓：核心接线边界

2026-10-11 更新：三个按钮语义替代“平仓并永久 MANUAL_STOP”。本文件是供核心线程接线的最小接口提案；运行中控制尚未实现，不能以一次性退出 CLI 作为替代。当前控制服务未启动，当前 MANUAL_STOP 和账户锁未操作。

## 三个独立动作

| 按钮 | 引擎拥有者执行 | 不改变的约束 |
| --- | --- | --- |
| 启动 | 将操作意图设为 RUNNING，开始寻找符合既有规则的机会 | 不自动解除 MANUAL_STOP、亏损预算、圈数、订单恢复或其他风险锁；被锁时明确拒绝并说明原因 |
| 停止 | 立即禁止后续新增敞口，意图设为 STOPPING；继续管理已有仓位、原订单与正常退出，权威双腿归零且无未决订单后设为 STOPPED | 不 kill、不把现有仓位丢给第二管理者、不默认强制平仓 |
| 平仓 | 立即将当前周期设为退出中，复用并发 reduce-only 与原 UNKNOWN 恢复；保留此前 RUNNING / STOPPING / STOPPED 意图 | RUNNING 平完继续寻找机会，STOPPING 平完停止；不自动解除既有风险锁、不把瞬时平仓写为永久 MANUAL_STOP |

彻底退出是用户先点停止、再点平仓。该顺序必须由 owner 的 intent_revision / 串行命令处理保证：旧平仓确认不得把较新的停止意图改回运行。

## 最小接口

浏览器继续只访问独立 loopback HTTP 控制适配层；8776 保持 GET-only。所有写动作是 POST JSON，精确 Origin、HttpOnly SameSite=Strict 会话、CSRF 和幂等检查沿用已完成适配层。浏览器不能传路径、命令、账户地址、启动 argv 或任意环境变量。

HTTP 可统一为：

- `GET /api/v3-control/session`：会话 + 当前能力与控制状态；只有 owner 协议已握手、状态新鲜且账户/批次/配置相符，才声明动作可用。
- `POST /api/v3-control/preview`：`{source:"basis", action:"start"|"stop"|"close", cycle_id?:string}`。start/stop 绑定账户、部署、批次、配置与 owner 实例；close 额外绑定当前周期、有符号两腿数量、原订单水位、15秒有效期及账户观测证据。只读，不先改变运行意图。
- `POST /api/v3-control/confirm`：同一 target + `preview_id`、`idempotency_key`。receipt 保留在服务端；owner 在最终提交/状态变更的临界区重新核对绑定与有效期，持久去重后才 ACK。
- `GET /api/v3-control/operations/{id}`：owner 的动作回执、当前运行意图、每腿进度及账务证据；不能用 HTTP 请求成功或本地旧 status 推断真实成交。

适配层与单一引擎 owner 建议使用进程用户限定的 Unix domain socket（父目录0700、socket0600），定长受限的 JSON 请求/响应，不增加持久密钥。每个请求至少绑定：

```text
protocol_version, request_id, owner_instance_id, account_binding_hash,
pair_identity, deployment, batch_id, config_hash, intent_revision,
action, expected_cycle_id, receipt/receipt_id, idempotency_key
```

owner 状态至少返回：

```text
source_ts, owner_instance_id, intent_revision,
intent: RUNNING | STOPPING | STOPPED,
phase: STARTING | RUNNING | MANAGING | EXITING | STOPPING | STOPPED | BLOCKED,
allowed_actions, blocked_reason, risk_locks,
active_cycle_id, authoritative position timestamp,
operation_id, operation state and two-leg evidence
```

`close` 的运行意图取 owner 接受命令时的当前值。若预检后意图变化，确认应拒绝并要求重新预检；恢复时绝不复写旧 RUNNING 意图。UNKNOWN 使用既有订单 ID；重复确认和 HTTP/进程恢复不产生第二次提交。

## 需要核心线程配合的位置

1. `entropy_arb/v3/runtime.py`：owner 命令消费与状态机；在 `_maybe_open` 及新增敞口的最后提交检查中执行 operator gate；主循环优先消费 close，复用 `_basis_exit` / `_resolve_pending` / `_finish_flat`；STOPPING 期间不退出持仓管理；STOPPED 保留可启动的控制 owner。网络 handler 只排队，不并发调用交易方法。
2. `entropy_arb/v3/live_host.py`：提交层新增敞口的最后一道 operator gate（可以复用现有 entries_halted 机制但不能覆盖其风险/持久化失败原因）。reduce-only 不受 operator stop 阻断。
3. `entropy_arb/v3/journal.py`：持久 operator intent / revision、close 请求、幂等回执；与现有批次风险关闭分开。不得在 start 中调用 reopen_entries 或清空 close_reason。
4. `main_v3.py`：固定部署配置下启动/关闭本地 owner 控制 socket，绑定 ProcessGuard 生命周期并发布协议能力。真实 owner 不存在时，HTTP adapter 返回 unavailable；需要明确授权的常驻 supervisor/启动入口后，才承诺从完全没有进程的状态点击启动，不能从浏览器临时 shell 拼接启动命令。

可把 socket 协议与请求验证放新 `entropy_arb/v3/operator_control.py`，但上述安全接入点必须由核心线程实施/确认。本线程只接 HTTP、前端与只读投影，不编辑这些核心文件，避免与2.5bp/A/B改动冲突。

## 已完成且可复用的部分

- 当前持仓的紧凑入口、同页确认/进度浮层、部分成交/UNKNOWN/待核算/已核算展示，关闭后无常驻结果卡。
- 过期 HOLDING 与同账户/周期/批次更新的权威退出记录核对；SNDK 已平记录和最终 -0.004772 U；明确不是当前实时仓位租约。
- 独立 loopback HTTP 会话、Origin/CSRF、账户周期 receipt、重复确认幂等、进度累计成交去重、安全结果白名单。
- 只读实盘数量与可执行深度预检、安全未知费用口径。原一次性 CLI 与锁内 hook 已做离线回归，但仅是独立退出工具，不能用于新三按钮语义。

## 接线验收（只用 fake owner / 订单）

1. RUNNING→close→FLAT→继续找机会；不写 MANUAL_STOP；不自动新增真实订单。
2. RUNNING→stop：新增敞口最后提交被阻断；已有仓位继续管理→权威FLAT→STOPPED；零 kill。
3. stop→close→FLAT→STOPPED；close→较新的stop→FLAT仍STOPPED；旧确认不能恢复运行。
4. 现有 MANUAL_STOP/预算/复核锁下 start 拒绝，锁、授权、风险值不变。
5. 预检/确认 owner 实例、账户、周期、配置、数量或 intent revision 改变即拒绝；无 owner、持锁但无协议时明确不可用。
6. partial/UNKNOWN/归零待核算/核算完成、重复累计回报、丢响应、服务重启恢复、重复点击均不创建重复订单/第二管理者。
7. 浏览器三个按钮同一行，确认及进度原地、不跳页、不闪屏、不常驻扩卡；真实启动/停止/平仓请求均拦截。

核心线程接口确认/交付后，再替换 HTTP adapter 的一次性 worker，并完成三个按钮接线与 mock 验收。未接线前，旧激活脚本不得作为这三个动作的交付证明。
