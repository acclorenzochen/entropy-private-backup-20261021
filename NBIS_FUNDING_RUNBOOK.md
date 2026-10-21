# NBIS 资金费对冲模板与公共预检

首次手动运行可先看 [NBIS 简明启动教程](NBIS_QUICKSTART.md)。`scripts/nbis_funding.py` 封装固定参数，默认只做公共预检；prepare 只生成未批准授权，start 必须由用户手动执行并保留现有安全检查。

## 多轮运行（2026-10-10）

已移除 NBIS 的单轮硬限制，**没有固定 5 轮上限**。批次轮数仍由真实授权文件的 `max_position_cycles`（正整数）控制；`--max-new-cycles` 是可选的本次进程新增周期上限，不传时没有额外会话上限。授权示例默认 `1` 仅是未获准使用的安全占位值，不是代码上限，也不是无限授权。真实运行前由用户选择批次上限并手动确认，不能通过修改同一批次文件、重启或删除 Journal 重置/扩大已经记录的授权。

一轮是完整的“开仓 → 对冲配平 → 双方各确认至少两次真实资金费结算 → 双腿 reduce-only 平仓”。只有正常完成、账户权威确认归零、所有订单终态、历史成交数量与手续费核对完成，且两边平仓后时间窗口的资金费查询均成功，才允许再开下一轮。下一轮重新按有效实时资金费差选方向，结算次数和负采样连续性独立计数。真实零成交不消耗持仓轮数，重试仍受持久化请求速率预算约束。

达到授权上限或可选会话上限后只停止新开仓；进程继续运行迟入账重核，不把等待时间或空账本查询当作资金费损益 final。资金费 `accounting_final` 仍是 false，`pending_accounting` 仍展示 provisional 数量；仅具备上述证据的正常资金费周期可不计入 `blocking_pending_accounting`。basis 模式的待核算开仓限制不变。

资金费逆转成本、强平距离、风险数据缺失、未知订单恢复或其他提前退出会持久化 `FUNDING_EARLY_EXIT`，先继续完成双腿退出，再停止该批次后续开仓；不会为凑轮数重开或自动翻仓。重启及迟到结算不会自动解除这个停止状态。旧版本账本的新证据字段默认 false，必须重新核对后才允许下一轮。整轮/批次金额亏损阈值仍不设置，0.5U 剩余资金费支出保护及 2% 强平距离保护保持不变。

多轮改动离线回归：`790 passed`（全部 `tests_v3`、费用适配器及配置测试，禁用真实 DNS/socket 连接；3 条依赖弃用警告）。包含真实主循环的模拟连续 6 轮、方向重新采样、授权/会话上限、活动/已完成周期重启、迟入账归属和去重、缺失交易费用/平仓后资金费查询、旧 schema 默认拒绝、native partial 与 UNKNOWN 恢复。这里的 6 轮是离线 fake venue 测试，不是实盘交易记录。本次没有运行实盘私有账本预检或设置杠杆。

2026-10-09 原始交付范围是实现、离线测试和只读检查：真实公共预检返回 `ok: true`，全部 13 项检查通过；另一次独立无 signer 的 `fetch_position()` 检查确认当时两边 NBIS 持仓均为 `0.0`。当天未初始化 signer、设置杠杆、发送订单、修改生产 logs 状态或停止 SNDK；后续停止 SNDK 的操作见上方 2026-10-10 记录。私有资金费账本与迟记账行为仍未验证，授权示例保持 `approved_live: false`；只读检查通过不等于实盘就绪。本模板不承诺收益，也不声称获得积分。

## 2026-10-09 只读检查结果

以下快照数值按主任务回报记录，仅代表该次读取：

| 检查或字段 | 结果 |
| --- | --- |
| 真实公共预检 | `ok: true`，13/13 checks 通过 |
| Entropy 资金费展示值 | `0.490357` bps/hour |
| Lighter 资金费展示值 | `0.32` bps/hour |
| 推荐方向 | 做空 Entropy / 做多 Lighter |
| 共同匹配数量 | `0.88` NBIS |
| Entropy / Lighter 名义金额 | `199.1616` / `199.2144` USDC |
| 估算净资金费收入 | `0.00339116766912` USDC/hour，未扣手续费及其他交易成本 |
| 市场最大杠杆：Entropy / Lighter | `15x` / `10x`，均允许配置的 `7x` |
| 独立无 signer 持仓查询：Entropy / Lighter | NBIS `0.0` / `0.0` |

初次公共预检因慢 funding/metadata 请求延迟了统一评估，导致先返回的两边盘口被判过期。入口现先完成资金费和市场详情，再在最后一组请求中并发获取两边盘口并立即评估；这次真实重跑已通过。仍使用请求开始时间计龄，`age <= 1 秒` 上限及双方时间差检查保持原值，盘口请求自身超过 1 秒仍失败。此前首次账户检查的 load_market 超时已由后续独立只读持仓查询补充结果。

两边持仓为零仅证明该次 NBIS 持仓读取结果，不能证明资金费账本零余额、真实零金额结算、手续费已核实或迟记账已完成。私有 ledger 未在本轮验证。公共入口本身仍不查询账户，因此公共 JSON 的 `account_verified`、`settlements_verified`、`accounting_delay_verified` 和 `live_ready` 保持 false；独立持仓查询不会将这些公共字段变成实盘授权。

## 文件与隔离

- `config.nbis.yaml`：连接及费用模板。CLI 必须指定 `--symbol NBIS --hedge lighter`；Entropy 必须是 Hyperliquid 主网 `io:NBIS`，Lighter 必须是主网 NBIS，公共元数据市场 ID 为 `189`。YAML 的 `thresholds` 仅满足现有配置加载器的必填 schema，V3 不执行 legacy 阈值。
- `config.v3.nbis-funding.json`：显式选择 `strategy_mode: funding_hedge`；其他现有 V3 配置仍默认使用 basis。
- `nbis-funding-authorization.example.json`：采用真实 `BatchAuthorization` 六字段格式，`approved_live: false`，默认批次上限 `1` 为安全占位；多轮由用户设置正整数上限。每腿 cap 为 $200，`loss_budget_usd: null`。
- NBIS 默认 deployment 为 `nbis-funding`，授权配对标识为 `nbis-funding:hl:NBIS:lighter:NBIS`。自定义 deployment 时授权标识也必须一致。
- NBIS 输出默认在 `logs/v3/nbis/`。公共预检仅写 `public-preflight-status.json`，不创建 Journal、observer、checkpoint、有效配置 manifest 或 ProcessGuard。显式 status 路径不能覆盖已知运行状态、账本、配置、锁或共享 logs 输出。
- 未来实盘 Journal 使用 `logs/v3/nbis/journal.sqlite3`，但进程互斥仍使用共享 `logs/entropy-arb.lock`，不得改成 NBIS 独立锁。

若其他实盘策略持有共享锁，公共预检仍可运行；NBIS 实盘必须拒绝抢占。2026-10-10 用户要求停止旧 SNDK：独立只读查询确认 Entropy io 和 Lighter 账户均无非零仓位，账本无未完成周期/订单后，旧实盘 PID 59991 已正常退出，锁已释放；行情记录与看板保留。本次多轮实现不启动 NBIS、不删除锁文件、不设置杠杆或发送订单。历史平仓检查不替代未来启动时的实时账户核对。

## 参数与证据边界

双方配置杠杆均为 7x。公共预检只检查市场允许范围，不设置杠杆，也不核实账户当前杠杆、抵押品、余额、持仓或强平价格。Entropy 最大杠杆取适配器从 `maxLeverage` 保存的 `max_leverage`；Lighter 取公共 `orderBookDetails.min_initial_margin_fraction`（万分比），按官方 SDK 的 `int(10000 / 7)` 检查范围。字段缺失或不可信时拒绝。

双方 base 数量采用共同 step `0.01`。预检分别计算“做空 Entropy / 做多 Lighter”和“做多 Entropy / 做空 Lighter”的共同数量，用两侧可执行深度逐档限制每腿名义金额。$200 是每腿的名义金额上限，不是保证金金额，也不是 200 个 NBIS；支持更小 cap。两腿数量相同，两边实际深度名义金额各自不超过最小配置 cap。

开仓匹配使用较粗的共同 step `0.01`，内部记账与恢复使用两边原生精度中更细的 step `0.0001`，用于精确表示适配器报告的真实 partial fill，不能把较细成交量舍入成已完整匹配。运行时 status 的 `common_quantity_step` 和 `accounting_quantity_step` 分别展示这两个值。这是代码中的精度处理约定，不表示本轮曾下单或产生真实 partial fill。

运行时 status 发布 `funding_state`，其中包含 `settlement_counts`、`funding_by_venue` 和 `actual_funding_usd`；Journal 在独立 SQLite 的 `funding_cycles`、`funding_settlements` 中持久化周期状态与结算事件，实际资金费合计从已记录事件派生，可用于重启恢复。这里描述的是实现与持久化能力，不能据此声称真实私有账本验证通过。没有结算事件时显示的合计零值不构成“真实零结算”的证据，完成条件仍要求真实结算记录。

公共资金费通过适配器 `fetch_funding()` 归一化为 bps/hour：正数表示多头付给空头。Lighter `/api/v1/funding-rates` 比较 feed 返回 8 小时等价费率，由适配器除以 8 后转换为小时费率；该比较值的归一化周期与真实逐小时支付应分别理解。来源见 [Lighter 官方 agent-kit 的 market funding schema](https://github.com/elliottech/lighter-agent-kit/blob/main/references/schemas-read.md#market-funding)。负数或数值零可以是有效费率，前提是来源、时间和数值验证通过。费率预测不等于已经收到的资金费。

JSON 的 `funding_direction` 使用现有纯策略 `entry_direction` 按两边 mid 估计方向；没有有效费率或单位数量净收益为零时为 null。每个方向的 `estimated_net_funding_usdc_per_hour` 使用该方向共同数量与各腿深度报价的实际名义金额，由 `pair_income_usd_per_hour` 计算；顶层同名字段对应推荐方向。所有这些值都是 **estimated**，不是已结算资金费，且未扣开平仓手续费、滑点和价差损益。mid 推荐方向与执行深度价格下的收益可以不同，预检不据此开仓。

`matched_quotes` 仅是本次公共入口盘口快照，推荐方向在对应名义金额估算下出现负收入也会原样展示，不能据此声称 live-ready。公共方向继续使用两边 mid 近似，quote 收入使用该次执行深度名义金额；它们不是按结算定价计算的真实到账金额。

资金费支付的数量定价基准是 Entropy / Hyperliquid **oracle price** 与 Lighter **index price**。持有期未来资金费估值应使用这两个价格；mark 用于强平距离等风险检查。Hyperliquid 官方说明以 oracle 将持仓数量换算为支付名义金额；Lighter 官方支付公式使用有符号持仓数量、index 和 funding rate。参见 [Hyperliquid 官方 Funding 文档](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding)和 [Lighter 官方 Funding 文档](https://docs.lighter.xyz/trading/funding.md)。

| 用途 | 价格或金额来源 | 证据含义 |
| --- | --- | --- |
| 公共预检方向 / quote 收入 | 两边 mid / 当次可执行深度名义金额 | estimated，保持非 live-ready |
| 持有期未来资金费估值 | HL oracle / Lighter index | estimated，尚未发生的支付预测 |
| 强平距离检查 | 当前 mark 与账户强平价格 | 风险距离，不是资金费支付定价 |
| 实际资金费与手续费 | 独立账本中的真实金额事件 | 已记录实际收支；本轮私有 ledger 未验证 |

NBIS 运行时资金费估值的接口约定是通过 `fetch_funding(include_price=True)` 取得快照的 `index_price`：HL 端该统一字段承载 oracle，Lighter 端承载 index。该接口接入由主任务完成并验证；公共预检保持默认调用及 mid estimate，不要求该定价字段。实际手续费与实际资金费仍由独立账本金额核算，不把任何价格估值预测写成已实现收入；账本空响应也不能证明真实零结算。

| 字段 | 模板值 | 用途 |
| --- | --- | --- |
| `funding_settlements_required` | 2 | 正常完成所需的真实结算证据要求 |
| `funding_poll_seconds` | 30 | 资金费轮询间隔 |
| `funding_stale_seconds` | 90 | 资金费有效期 |
| `funding_reversal_samples` | 3 | 反向费率连续样本要求 |
| `funding_reversal_seconds` | 60 | 反向持续时间要求 |
| `funding_remaining_cost_limit_usd` | 0.5 | 保留剩余资金费成本保护规则 |
| `funding_round_loss_limit_usd` | null | 当前不设置固定整轮金额亏损阈值 |
| `funding_liquidation_distance` | 0.02 | 保留 2% 强平距离保护规则 |
| `funding_risk_poll_seconds` | 2 | 风险状态轮询间隔 |
| `funding_risk_stale_seconds` | 10 | 风险证据有效期 |
| `funding_accounting_grace_seconds` | 120 | 最小账务重核周期，不是强制 final 截止时间 |

风险读取的外层请求截止时间为 10 秒，证据有效期仍为 10 秒，强平距离保护仍为 2%。两腿独立并发刷新；可信危险证据立即请求统一退出，已请求的正常退出也不等待风险、资金费历史或费率读取。单次失败只可复用同一周期、同一有符号仓位及订单状态下仍有效的内存快照，保留原始时间与到期时间；仓位变化、重启、缓存过期或矛盾的持仓证据使缓存失效。读取失败且无可信证据时阻止首单增仓，已有仓位进入双腿退出；可靠空仓快照不要求强平价格。不会延长旧快照、增加故障宽限或无限重试。

`observer.jsonl` 的 `v3_funding_risk` 事件记录每腿请求起止、耗时、证据年龄、缓存使用、失败分类及连续失败时间；`v3_funding_risk_expiry` 留存故障期间缓存失效导致退出的证据，`v3_funding_hold` 保存持有期观测。事件不保存原始异常、请求 URL 或账户标识。上述变更在后续用户手动启动进程时加载，不会自动重启现有服务。

当前授权 `loss_budget_usd: null`，不设置固定批次金额止损。仅 funding_hedge 加载授权时允许 null；basis 仍要求正数预算。如果以后双方金额阈值均设为数值，批次预算不得超过整轮阈值；任一为 null 时不作该数值比较。

缺失资金费、过期或未来时间、错误来源、错误市场、共同 step 不符、杠杆范围不足、无可执行共同数量或超 cap 的检查都 fail closed。先并发完成资金费及市场详情请求，随后并发抓取两边盘口并立即评估，避免等待其他请求人为老化盘口。盘口仍以本地请求开始时间计算保守年龄，保留 age <= 1 秒和双方时间差检查；盘口请求自身延迟超过 1 秒时仍明确失败，不放宽参数，也不将本地时间称为服务器新鲜度证明。

账户真实结算事件、去重、归属、迟记账与恢复必须另外核实。`zero settlement` 只有真实账务事件能证明零金额；空响应、缺失事件或没有证据不能推断“已结算为零”，也不能补齐两次结算。缺证据应保持未完成并 fail closed。没有账本 watermark 或完整性保证时，迟记账核算保持 **provisional**；等待 120 秒或某次查询返回空数组不能使其变成 final。

费用核账将每腿成交独立保存到 `fee_fills`，按 `(venue, exchange_fill_key)` 去重，仅允许未知费用补为真实金额。`orders.known_fee_usd / fee_covered_units / fee_source` 保留累计回报覆盖范围；成交量增长而费用未更新时，完整 `fee_usd` 置空，已有部分证据保留。部分累计回报与逐成交费用可能重叠，按覆盖数量选一个来源，不相加；完整逐成交历史优先于累计回报。配置费率为零不证明真实费用为零。

新步骤保存原始 `live_steps.quantity_step`。`funding_cycles.trade_gross_cash_usd` 仅用已证实的累计成交价格、数量与原始网格重建，缺网格或成交价格时保持未知，不凭当前配置或旧 `trade_cash_net_usd` 回填。缺稳定成交 ID 的行不能进入逐成交账本或证明完整覆盖。完整逐成交价格优先于重复累计回执，与费用是否全知分开判断。`known_cash_result_usd` 是毛现金减每订单选定的已知费用小计、加已入账资金费的部分结果，可供数值损失预算绝对重算；缺任一费用时 `trade_cash_net_usd` 和周期净值保持未知。全量数量与费用核对完成后才有 `trade_accounting_complete`；资金费无完整性 watermark 时周期依然 provisional。

本次费用修复只改代码并使用临时数据库离线验证，没有补写生产历史或查询真实私有费用。旧交易缺失的费用、成交 ID、原始单位网格及资金费完整性证据仍需真实来源恢复；读模型当前只展示完整订单费用，部分订单的已知费用保存在新账本字段中。

## 用户手动命令

实现后的离线检查已通过 760 项（`tests_v3`、受影响费用适配器及配置检查，禁用 DNS 和真实 socket 连接）。新增回归覆盖持续 UNKNOWN 首腿不新开 hedge、风险查询在途不放行增仓、延迟经济回调与零成交不误标 final、手续费历史缺失时资金费仍进入同一损益账，以及 .0001 部分成交的跨重启恢复。费用交易现金基线与实际资金费合计分别持久化，重核采用绝对重算，不重复追加。

提前退出会保存退出原因及 `completed: false`，status 展示 `funding_completion_label: 两周期未完成`；完成两边各两次确认后为“两周期完成”。正常完成可进入下一轮；提前退出锁住批次，不再开仓，进程保留账务重核能力。最新独立公共定价读取确认 oracle/index 为 `226.18` / `226.19`，对应费率 `0.492744` / `0.32` bps/hour，仅是另一时点的快照，不覆盖前述预检报价。

以下命令仅供用户审阅并手动执行，代理不代为执行。

从项目目录运行离线测试：

```bash
cd /Users/mingyuchen/Documents/ChatGPT/entropy
.venv/bin/python -m pytest -q tests_v3/test_nbis_launch.py tests_v3/test_main_v3.py
```

只做离线配置校验（会写 NBIS 独立校验状态及有效配置 manifest，不接网络）：

```bash
.venv/bin/python main_v3.py \
  --symbol NBIS --hedge lighter \
  --config config.nbis.yaml --v3-config config.v3.nbis-funding.json \
  --deployment nbis-funding --validate-only
```

无签名公共预检，无需授权文件或私钥：

```bash
.venv/bin/python main_v3.py \
  --symbol NBIS --hedge lighter \
  --config config.nbis.yaml --v3-config config.v3.nbis-funding.json \
  --deployment nbis-funding --env-file /dev/null \
  --public-preflight-only
```

该入口只调用公共市场加载、公共资金费和盘口快照，以及用于杠杆范围检查的公共市场详情。stdout 输出一个结构化 JSON；检查通过返回 0，公共检查失败返回 2，并写独立 status。JSON 中 `live_ready`、`account_verified`、`settlements_verified`、`accounting_delay_verified` 始终为 false。公共模式与 `--live`、私有恢复/预检/重新开仓、批次授权、`--max-new-cycles`、`--record-only`、`--validate-only` 互斥。

### 实盘命令（仅供用户手动执行，本次未执行）

示例授权始终为 false，不能用于 live。用户手动确认并完成账户与迟记账验证后，才可准备真实 `nbis-funding-authorization.json`（独立 batch_id，配对一致，`max_position_cycles` 为用户选定的正整数，cap 不超过配置与 $200）。只读公共预检成功不是实盘批准；`approved_live` 必须由用户手动确认。已有 batch_id 的上限不得通过改文件悄悄扩大。

以下私有预检会进入真实账户路径，可能初始化 signer 并读写独立 Journal，和公共预检有不同副作用：

```bash
.venv/bin/python main_v3.py \
  --symbol NBIS --hedge lighter \
  --config config.nbis.yaml --v3-config config.v3.nbis-funding.json \
  --deployment nbis-funding \
  --batch-authorization nbis-funding-authorization.json \
  --live --preflight-only
```

未来真实运行命令可能设置杠杆并发送订单，只能由用户在另行授权后手动执行：

```bash
.venv/bin/python main_v3.py \
  --symbol NBIS --hedge lighter \
  --config config.nbis.yaml --v3-config config.v3.nbis-funding.json \
  --deployment nbis-funding \
  --batch-authorization nbis-funding-authorization.json \
  --live
```

如需对本次进程另设较小的轮数上限，可追加 `--max-new-cycles N`；有效上限始终还受批次剩余授权约束。省略它不代表无限授权，也不会忽略已有累计轮数。

当前 YAML 的 0 bps 是费用模板值。实际账户费率和可能的附加费用仍需独立核实；公共预检没有验证账户费用。多轮授权不代表能够安全实盘或账务已 final，也不应通过换 batch_id 或删除 Journal 来绕过未完成周期。真实资金费结算账本、迟入账表现和账户费用仍未完成实盘验证。
