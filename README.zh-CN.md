# entropy-arb

**[English documentation / 英文文档 → README.md](README.md)**

开源双交易所永续合约套利机器人。其中一条腿永远是 **Entropy**（Hyperliquid 上的
`io` builder dex）；另一条腿（对冲腿）三选一：

| `--hedge` | 交易所 | 计价货币 | 吃单费 | 协议 |
|---|---|---|---|---|
| `lighter` | Lighter 主网 | USDC | 0 bps | zkLighter ws（增量订单簿，异步结算） |
| `lighter-rh` | Lighter Robinhood 链 | **USDG** | 0 bps | zkLighter ws |
| `tradexyz` | Hyperliquid trade.xyz dex | USDC | ~1 bps | HL l2Book，IOC 同步结算 |

> **推荐链接** —— 通过以下链接注册即可支持本项目：
> - Entropy — Tier 4 推荐，100% 返佣：<https://entropy.io/?r=yourquantguy>
> - Lighter Robinhood 链：<https://robinhoodchain.lighter.xyz/?referral=QUANT>
> - trade.xyz（Hyperliquid）：<https://app.hyperliquid.xyz/join/QUANTGUY>

机器人交易的是相对动态中枢的跨市场溢价回归，不要求开仓成交边本身为正。
先确认 Entropy 成交，再按实际数量发送对冲腿（均为吃单），持有 delta 中性仓位。
两腿不是原子成交；盘口信号不能保证最终成交价格和利润。所有交易决策使用的
价格都来自**将要实际成交的那个交易所的真实订单簿**——Hyperliquid 的盘口来自
官方 websocket（`wss://api.hyperliquid.xyz/ws`），Lighter 的盘口来自 Lighter
官方 websocket。

## 整轮净收益保护

`profit_guard` 默认关闭，旧配置未填写该段时保持原有开平仓路径，优先保留交易频率。
只有显式设为 `enabled: true` 才启用下述额外收益限制。首次使用新代码需要重启
交易引擎与看板 API。部署前先检查双腿仓位及未完成执行，不要另开第二个实盘进程。
现有杠杆、名义价值上限、对冲恢复和紧急减仓规则不变。

- **开仓/加仓**：统计入场带仍需满足；再估算返回目标中枢后的平仓收益，扣除
  开平两次的双腿手续费、当前反向盘口深度成本、预计资金费和执行余量。
  模型中枢不是未来价格保证。负开仓边可以通过，但预计整轮净收益必须过线。
- **正常离场**：不再要求先碰中枢离场线。按本次拟平数量读取 SQLite 中剩余
  FIFO 批次的真实成本和未分摊开仓手续费，加上当前平仓深度、预计平仓费、
  资金费成本与退出执行余量，净收益达到底线即可参与离场信号。
  `exit_bps` 此时只影响新开仓的目标回归情景；图上的中枢离场线不再是硬性门槛。
- **发单前**：重新获取同一数量的盘口报价并复核收益、数据新鲜度与仓位上限。
  第一腿成交后必须继续对冲，不能因收益转负停发第二腿。
- **部分成交与重启**：成本直接来自持久化剩余批次，不从本次运行起点重置。
  累计成交均价会转换成新增成交价格；API 手续费回查会更新剩余批次费用。
  对账基线、仅有限价的成交以及无法证实成本的恢复仓位不会被当成真实成本；
  会阻止普通盈利离场/加仓并记录原因。紧急敞口回补与手动平仓不受该门槛约束。

```yaml
profit_guard:
  enabled: true
  min_profit_usd: 0.01
  # entry_buffer_bps: 8.5
  # exit_buffer_bps: 8.5
```

`min_profit_usd` 是**每个拟成交配对切片**扣成本后的美元底线，不是整账户收益。
两个 buffer 各覆盖一次双腿执行；省略时各自取现有
`leg_slippage_bps + second_leg_slippage_bps`。例如 2.5 + 6 时，各为 8.5 bp：
新开仓预留开、平合计 17 bp；平仓仅预留 8.5 bp，开仓损耗已包含在实际成本内，
不重复扣减。它们是规划余量，不是对市场跳动/第二腿重报价损失的保证上限。
该默认值偏保守，可能明显降低频率。不要为了凑交易次数直接把成本余量归零。

这四项支持现有配置热更新：完整校验后，下次扫描和发第一腿前复核使用新值。
已经发出的订单与恢复对冲不追改。资金费为历史已记录扣款与按持有时间计算的
当前费率估计两者取高，不用预计资金费收入补利润；它不是完整的交易所结算审计。
普通止盈保护也不等于止损：亏损或成本未知可能继续持有，并未新增强制平仓/熔断。

诊断保存在状态 API 的 `strategy.profit_guard.checks`（含检查时间、成本、费用、
余量、净收益及拒绝原因），获准执行的预算还会进入 SQLite 的执行上下文，便于回放。

### 减仓预算限价与零成交重试

开启 `profit_guard` 时，正常减仓的 Entropy 首腿限价不再固定为
`leg_slippage_bps`：按拟平切片的 FIFO 开仓现金流，扣除已分摊开仓费、
预计平仓费、资金费、`exit_buffer_bps` 和 `min_profit_usd`，再预留 Lighter
按 `second_leg_slippage_bps` 计算的保护价格，反推首腿最差可接受价格。
买价向下、卖价向上取整，不越过收益预算。可用利润越多，首腿可以接受的
价格移动越大；预留后连当前深度都覆盖不了，就不发这笔减仓。

这里额外预留对冲腿保护价格，**不挪用原有退出余量**。相比只看盘口净收益会更
保守，可能降低离场频率。`limit_net_profit_usd` 是按两腿预算价格计算的净收益，
不是保证实现的利润：首腿成交期间若对冲盘口跳动，仍必须按实际成交量继续对冲，
不能为了保利润遗留裸仓。开仓/加仓仍使用原有首腿滑点；仓位和杠杆不变。

- 第一腿发出前按同一数量重新报价；用实际盘口更新时间检查过期，心跳不代表报价刷新。
- 第一腿确认零成交后，等待 `no_fill_retry_sec`、两边盘口更新并重新满足
  `premium_persist_sec`，重新算收益和限价。不是拿旧价格连续补单，也不保证固定间隔成交。
- 回报丢失或异常无法确认结果时，只查询持久化的原订单 ID，不另发首腿。
- 任何正成交量都进入成交账本和恢复检查。低于交易所最小量、且敞口仍在既有容差内的
  尾差可结束本次恢复，但记录为单腿并保留实际仓位；不会冒充零成交或为凑最小量超额对冲。
- IOC 未即时成交的原始原因写入 SQLite 执行事件；未发出的第二腿显示“未发送”。
- 历史 CSV 镜像按执行完成时间与成交形状去重；旧“零成交”记录若实际成交量非零，
  展示及统计会重新分类。原始历史数据不删除。

代码升级需重启交易引擎和看板 API 后生效，不能仅靠配置热更新加载新逻辑。

机器人运行期间（即使没有密钥、没有开策略）会自动把两边盘口记录成兼容的
**分钟级 CSV** 与**秒级 top-of-book** 数据，配套工具用于分析阈值、收敛、
持仓风险、资金费和执行成本。

## 成交链路延迟

行情事件直接唤醒策略，HTTPS 连接复用和定期预热保持开启。进一步的本地优化：

- 盘口排序和最优价按更新缓存；任何快照、增量或清空都会使缓存失效。
  返回的深度列表仍可由调用方独立使用，不会修改下一次决策的缓存。
- Entropy 订单结果不明时，以原订单编号补查；首次补查立即执行，后续间隔
  为 100、200、400、500 毫秒，之后保持 500 毫秒；请求异常时不加速重试。
  直接返回成交的正常 HTTP 响应不需要补查，也不会因此额外提速。
- Lighter 的已认证订单终态与 HTTP 回执竞速；终态先到就先返回成交结果。
  HTTP 请求仍会收尾，新策略订单等待 SDK 收尾后再提交，避免 nonce 冲突。
  只有 HTTP 成功回执不能判定成交；超时/传输异常继续按原订单编号对账。
- 第一腿确认后立即按真实成交量发送对冲；保留成交记账和发单前持久化，
  不并发盲发双腿、不放宽滑点、不缩短信号持续期、不调整账户等级或交易参数。

日志 `[LATENCY 执行编号]` 和 SQLite `executions.context_json.latency_ms`
记录预检至第一腿提交、第一腿提交至结果、第一腿结果至第二腿提交、第二腿
提交至结果，以及本次尝试至结果的总耗时。单位为毫秒，使用单调时钟；
它们包含本地处理与网络等待，不是交易所撮合时间。未完成对冲的后续恢复
不计入本次尝试总时长，没有发送的第二腿也不会出现耗时。

离线回归：`python -m pytest tests/test_execution_latency.py -q`。
可复现盘口基准：`python -m tests.benchmark_execution_latency`，不读取密钥、
不连接交易所。合成负载提速不等于端到端成交提速；实际改善应在重启引擎后
根据真实分段日志比较，不能由本地测试承诺。修改不热加载到已有进程。

## 信号逻辑

整个信号就是 `config.yaml` 里三个数字，由你根据采集的数据自己设定：

```
premium_bps =（Entropy 价格 / 对冲腿价格 − 1）× 10 000

                          ┌──────────────  卖出 Entropy + 买入对冲腿
midline + upper  ───────────────────────────────────────────────────
                                       ▲
midline          ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─   溢价的长期中枢
                                       ▼
midline − lower  ───────────────────────────────────────────────────
                          └──────────────  买入 Entropy + 卖出对冲腿
```

- `midline_bps` —— 溢价的常态水平。跨所溢价几乎从不以零为中心（预言机不同、
  计价货币不同、新上市溢价等），零中心的带只会朝一个方向开仓、打满仓位上限、
  永远无法平仓。请实际测量溢价所在的位置，然后填入。
- `upper_bps` / `lower_bps` —— 中枢上下两侧的入场带宽。

两个方向的门槛都作用于**可实际成交的价格**（Entropy 买一 对 对冲腿卖一，
反之亦然），并且是**扣除双边吃单手续费之后的净门槛**——引擎会在阈值之上
另行叠加手续费。因此一次完整往返扣费后**净赚 ≥ upper + lower bps**，这是
结构上保证的。

有一点必须理解：当 `midline_bps: 5` 时，买入 Entropy 的门槛是
`lower − midline`，可能为**负数**。这是有意为之——如果 Entropy 长期贵 5 bps，
那么在溢价为 0 时买入它，相对其自身均衡水平就是便宜了 5 bps，这笔交易正是
此前在 `midline + upper` 处卖出的获利平仓。这同时意味着**中枢填错就是亏钱
策略**：若真实溢价中枢是 0 而你填了 5，机器人会整天以公允价买入 Entropy。
先测量、再交易——数据采集器和分析工具就是为此而生。

## 自适应入场带

`thresholds.upper_bps` 和 `thresholds.lower_bps` 仍然是入场带的最低值。
开启 `adaptive_bands.enabled` 后，每秒使用动态中枢模型同一套稳健波动估计：

```text
effective_band = min(max(base_band, configured_min, entry_sigma_mult × robust_sigma), max_band)
```

实际入场带永远不会低于原始 base threshold。当
`robust_sigma_bps > max_sigma_bps` 时，波动保护会禁止 `OPEN` 和 `ADD`；
`REDUCE`、紧急 `HEDGE` 与 `RECONCILE` 仍然允许执行。

## 配置热更新

默认开启 `hot_reload.enabled`。机器人每隔 `hot_reload.poll_sec` 秒检查
`config.yaml`；文件会先完整解析和校验，校验通过后，信号模型参数从下一次策略
扫描开始生效。正在执行的订单不会重新定价，已经保存的周期入场记录也不会被改写。

可以运行中修改的主要参数包括：`thresholds`、`dynamic_midline`、
`premium_model`、`adaptive_bands`、`midline_lag_guard`，以及执行节奏中的
`premium_persist_sec`、`cooldown_sec` 和 `min_edge_surplus_bps`。例如把
`adaptive_bands.min_upper_bps` 从 `5.0` 改为 `3.0` 后，下一次新的开仓/加仓扫描
会使用新的最低带宽。

账户、交易所、密钥、杠杆、仓位上限、下单数量、库存规则、资金费任务、订单价格
保护、数据路径和看板端口等配置仍然需要重启。这些字段若在运行中被修改，会在日志
中标记为 `restart required`，当前进程继续使用旧值；`hot_reload.enabled` 也需要
重启才能改变，但 `hot_reload.poll_sec` 可以热更新。新配置无效时同样保留旧配置，
不会因为一次编辑错误中断交易循环。

## Premium 数据质量

每条溢价样本只有在两边盘口都有较新的 `last_update_ts`，且两边时间戳差不
超过 `premium_model.max_cross_venue_skew_ms` 时才会进入模型。盘口超过
`max_book_age_ms`、任一盘口缺失或跨市场时间差超限的样本会记录为无效并注明
原因，同时从滚动中枢和 sigma 中排除；无效样本不会用上一秒数据前填。连续无效
超过宽限时间后进入 `premium_data_degraded`，禁止新增敞口，但减仓、紧急对冲和
对账仍可执行。

## 中枢延迟保护

模型用 20 分钟 `short_center` 对比限速后的 `active_midline`。持续处于警告区间
时使用普通移动速度；更大的延迟持续达到配置时间后切换到加速速度，并受
`absolute_max_move_bps_per_10m` 硬上限保护。延迟达到阻止阈值并持续足够时间后，
只禁止 `OPEN` 和 `ADD`。活动中枢仍会继续向 3 小时 `long_center` 移动，以便恢复；
这个保护不会阻止 `REDUCE`、`HEDGE` 和 `RECONCILE`。

## 独立 sigma 窗口

动态中枢使用配置的长窗口（默认 3 小时），稳健 sigma 使用独立的有效溢价窗口
（默认 30 分钟）。sigma 计算为 `1.4826 × MAD`，MAD 以该 sigma 窗口自己的中位数
为中心，因此缓慢的中枢迁移不会被误判为当前盘口噪声。本项目不包含自动优化器。

## 周期锚定退出

一次 `OPEN` 双腿完整成交后，引擎保存该周期的入场中枢、稳健波动、入场带、
门槛、方向和配对数量。之后减仓使用“本周期入场中枢 + 固定 `exit_bps`”计算，
运行中的动态中枢漂移不会重写原来的均值回归假设。当前中枢相对入场中枢漂移超过
`cycle_exit.max_midline_drift_bps` 时，只禁止继续 `ADD`。部分减仓不会重置周期，
只有对账确认真实双腿回到空仓才会重置。若重启时存在残仓但没有保存锚点，系统会
安全地禁止 `ADD`，同时保留 `REDUCE` 和紧急对冲能力。

回放使用与实盘相同的纯函数，并且不读取未来数据。可以在不修改实盘配置的情况下
比较参数：

```bash
python3 tools/replay.py --csv logs/seconds.csv --config config.yaml \
  --adaptive-entry-sigma-mults 1.5,2,2.5 --cycle-drift-limits 3,4,5
```

## 动态策略模式

`midline_bps` 现在是启动 seed 和 warmup fallback。开启
`dynamic_midline.enabled` 后，引擎每秒只采一条有效溢价，使用滚动 median 与
MAD 估计稳健中枢/波动，并限制 active midline 的移动速度。长短中枢持续分叉
时进入 `SHIFT / REDUCE_ONLY`：禁止 `OPEN`/`ADD`，但 `REDUCE`、紧急 delta
hedge 与 reconcile 不受影响。

动作以对账后的真实双腿仓位为准。空仓可开仓；已有 SHORT_ENTROPY 或
LONG_ENTROPY 时，只能同向 ADD 或按更紧的 `exit_bps` REDUCE。减仓两腿均为
reduce-only，数量不超过 paired inventory；反向信号必须先回到 FLAT，不能单笔
穿仓。资金费异常、过期或不利成本过高也只阻止新增 exposure。执行采用
Entropy 首腿：Entropy 零成交时不发送对冲腿；实际成交后才按相同数量在
Lighter 完成第二腿。两腿使用独立价格保护，最终紧急回补使用更宽保护。

## 快速开始

```bash
git clone https://github.com/your-quantguy/entropy-arb.git && cd entropy-arb
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # 数据采集只需要这些

cp config.example.yaml config.yaml       # 策略配置（阈值、规模、风控）
cp .env.example .env                     # 密钥——交易必填
```

交易哪个市场**不在**配置文件中——每次启动时用命令行参数显式指定：
`--symbol`（两个交易所共同交易的品种）和 `--hedge`（三选一：
`lighter`、`lighter-rh`、`tradexyz`；Entropy 永远是
另一条腿）。

本机器人**没有模拟盘**——要么采集数据（`--record-only`），要么实盘交易。
请用采集的数据和最小的仓位上限来验证策略，而不是模拟成交。

### 选择策略

现在项目只有一个启动入口，但支持两套明确的策略。现有的 Entropy 首腿吃单套利
保持向后兼容，策略名为 `legacy`，省略 `--strategy` 时默认使用它。新的单 Pair
Entropy maker / Lighter hedge 策略用 `--strategy maker-hedge` 选择。两套策略使用
各自的 YAML 配置、SQLite 状态、日志和看板端口，避免不同执行状态机误读彼此的
检查点；两者都继续复用现有 `.env`，不会复制或打印密钥。

```bash
# 现有策略——与历史启动命令等价
python3 main.py --strategy legacy --symbol SNDK --hedge lighter-rh

# 新策略——纸面模式，不发真实订单
python3 main.py --strategy maker-hedge --paper --no-dashboard

# 新策略——只读连接和账户状态检查
python3 main.py --strategy maker-hedge --preflight-only
```

新策略源代码仍保留在旁边的 `entropy-maker-hedge/` 项目中，可独立回滚。统一入口
只负责选择启动哪套策略，不会把另一套策略的 active cycle 强行接管过来；如果交易所
已有仓位但当前策略没有本地 cycle，程序会安全停止并要求人工 reconcile。

同一账户同时只允许一套 live 策略运行。两套启动路径共用
`logs/entropy-arb.lock`；`--preflight-only`、`--record-only` 和 maker-hedge 的
`--paper` 不占用 live 锁。

### 微信 ClawBot 小时报告

现有 `legacy` 引擎可以通过微信 ClawBot/iLink 每小时发送一条只读报告，内容包括：

- 当前运行状态、策略名、配对状态、对账状态、两边持仓和净敞口；
- 过去一小时账户权益变动、成交账本净额、手续费、资金费；
- 今日账户权益盈亏、已实现/未实现盈亏和归因；
- Entropy 成交量、Entropy 未配对单腿成交量；
- 本小时执行笔数、今日执行笔数和账本历史累计执行笔数。

先在 `config.yaml` 中启用：

```yaml
wechat_clawbot:
  enabled: true
  interval_sec: 3600
  send_startup: false
```

推荐直接运行二维码配置（此命令不会启动交易）：

```bash
python3 main.py --wechat-setup
```

命令会在默认浏览器打开短时有效的二维码，等待手机扫码确认，然后把返回的凭据保存到
被 git 忽略的本地文件 `logs/wechat_clawbot_credentials.json`，文件权限为 `600`。它不会
修改 `.env`、占用交易锁，也不会发出任何订单。如果浏览器没有自动打开，可以加
`--no-open`，再手动打开终端输出的二维码链接。

扫码成功后，重启通知/交易进程一次，并用目标微信给 ClawBot 发一条消息，完成接收人和
会话上下文绑定。`logs/wechat_clawbot_state.json` 只保存接收人、会话上下文和增量游标；
通知失败只记录日志并在下一小时重试，不会停止或改变交易。

下面的 `.env` 变量仍作为兼容旧 Token 配置的本地覆盖项保留，但二维码配置不再要求填写
它们（不要提交真实值）：

```dotenv
WECHAT_CLAWBOT_TOKEN=
WECHAT_CLAWBOT_TARGET_USER_ID=
# 可选；留空时，首次给 ClawBot 发消息会自动保存会话上下文
WECHAT_CLAWBOT_CONTEXT_TOKEN=
```

二维码 Token 不会写入看板状态或日志；如果同时设置了旧的环境变量，它会优先于二维码凭据。
小时权益变动来自引擎持续写入的 SQLite 权益快照，即使使用 `--no-dashboard` 也会记录。

**第一步：先采集数据**（不需要任何密钥）：

```bash
python3 main.py --record-only --symbol SNDK --hedge lighter-rh
```

建议至少运行 24–72 小时。兼容的分钟数据写入 `logs/minutes.csv`，每秒
top-of-book 数据写入 `logs/seconds.csv`；秒级数据同时记录盘口年龄、跨市场时间差、
样本是否有效、拒绝原因、中枢延迟状态和独立 sigma 窗口。

**第二步：分析数据、设定阈值：**

```bash
python3 tools/analyze.py
```

它会输出溢价分布、各档带宽的历史触发频率，以及可直接粘贴进
`config.yaml` 的 `thresholds:` 配置块。

然后运行收敛、MAE/MFE、持仓时间、资金费和滑点压力测试：

```bash
python3 tools/replay.py --csv logs/seconds.csv --config config.yaml --hours 72
```

Replay 默认调用实盘 `Engine` 的候选、经济检查、限价、重试和恢复逻辑，交易所 I/O 由离线模拟替代。
每次订单确认至少使用下一条观测；可用 `--first-delay-sec`、`--hedge-delay-sec`、
`--first-fill-fraction`、`--hedge-fill-fraction` 和 `--zero-fill-every` 注入执行压力。
输出分别展示已完成周期收益、未平库存的中价估值及扣退出费用/滑点后的估值，残仓不会计成完整闭环。
`--orders-per-minute` 是不超过配置限额的模拟上限；退出物理限价沿用配置。
`--legacy-ideal-fills` 保留原来的理想成交研究模式。

秒级 top-of-book 不含完整 L2、tick/step 元数据或真实成交回执；模拟默认共同数量步长为 0.0001、
交易所最小金额为 $10，资金费按连续费率近似。它支持按历史顺序检查政策和执行压力，不能保证真实成交或证明正期望。

**第三步：实盘** —— 填写 `.env`，安装签名 SDK，仓位上限从刚好满足
交易所最小名义的水平开始：

```bash
pip install -r requirements-live.txt
python3 main.py --symbol SNDK --hedge lighter-rh
```

不带 `--record-only` 运行时，只要两边行情就绪且溢价越过带宽，就会立即
发送真实订单。

**仪表盘。** 在终端运行时会显示实时 Rich 仪表盘：两边盘口（含数据龄/点差）、
持仓与上限、账户权益与本次会话盈亏、两个方向的可成交溢价对比完整门槛
（已含手续费与库存加价，● 表示已武装）、数据采集进度、最近成交，以及日志
尾部（完整日志写入 `logging.file`，默认 `logs/engine.log`）。`--record-only`
模式同样可用。加 `--cn` 参数可使仪表盘全部以中文显示。`--no-dashboard`
可切换为纯日志输出（nohup/systemd 等非终端环境会自动退回纯日志），也可
设置 `logging.dashboard: false`。

### 只读 Web 看板

Web 看板是独立、仅监听本机的监控界面。先开启顶层配置
`dashboard.enabled`，引擎会原子写入 `logs/status.json`；然后构建前端并
启动只读 API：

```bash
cd dashboard && npm install && npm run build && cd ..
uvicorn entropy_arb.dashboard_api.app:app --host 127.0.0.1 --port 8765
```

打开 `http://127.0.0.1:8765`。开发前端时在 `dashboard/` 运行
`npm run dev`，Vite 会把 `/api` 代理到 8765 端口。API 只读本地快照与
SQLite，不提供交易控制；看板或 API 故障不会中断策略。`/api/dashboard` 一次
返回同一时点的状态、历史、成交和盈亏，前端不会再把四个不同时间点拼成一屏。

当 SQLite 文件已存在时，API 会以只读方式打开，不会在启动时执行旧 CSV 或
数据库结构迁移。只有需要一次性迁移时才临时设置 `ENTROPY_ARB_MIGRATE_LEGACY=1`，
迁移后应恢复为 `0` 或取消该变量。

实盘启动后，后台每 10 秒回查一次两家交易所的逐笔成交：首轮回看最近 7 天，
之后回看最近 15 分钟。Hyperliquid 使用 `userFillsByTime` 的实际 `fee`，
Lighter 使用认证的 `/api/v1/trades` 中的 maker/taker fee；匹配成功后会把
`fills` 中的配置估算替换为实际费用，并在看板标注“已核实/配置估算/待回查”。
回查失败只影响费用标注，不会阻塞下单。

运行期间，`logs/entropy_arb.sqlite3` 是执行检查点、订单、成交增量、仓位对账、
资金费估算、PnL 归因和历史行情的持久账本。首次启动会幂等迁移已有的
`trades.csv`、`seconds.csv`、`minutes.csv` 及 `.old` 文件；也可以手动运行：

```bash
python3 tools/migrate_legacy.py --db logs/entropy_arb.sqlite3
```

第一腿成交后，执行状态会保持在 `first_leg → hedge_leg → reconciling → verified`，
持续查询同一订单引用并只发送剩余对冲数量。进程重启先读取交易所仓位，再恢复
同一 execution；对账未完成或仍有净敞口时不会开新执行。

实盘可用 macOS launchd 托管，配置内置单实例 flock、异常自动拉起和启动日志：

```bash
./scripts/install_launchd.sh
launchctl print gui/$(id -u)/com.entropy.arb
```

故障回放测试覆盖“第一腿已成交、第二腿超时、进程重启、解析原订单后继续对冲”，
运行全部测试：

```bash
uv run --with pytest --with httpx2 --with-requirements requirements.txt pytest -q
```

## 数据采集与分析

采集器在所有模式下自动运行（`recorder.enabled: true`）：每秒采样一次两边
的真实盘口，每分钟写一行：

| 列 | 含义 |
|---|---|
| `minute_ts`, `time_utc` | 分钟起点（epoch 秒 / ISO UTC） |
| `entropy_bid/ask`, `hedge_bid/ask` | 该分钟最后一次有效盘口 |
| `premium_open/high/low/close/mean/std_bps` | Entropy 相对对冲腿的中间价溢价 |
| `sell_edge_mean/max_bps` | 卖出 Entropy 方向的可成交溢价（Entropy 买一 / 对冲腿卖一 − 1） |
| `buy_edge_mean/max_bps` | 买入 Entropy 方向的可成交溢价（对冲腿买一 / Entropy 卖一 − 1） |
| `samples` | 该分钟约 60 秒中两边盘口同时有效的秒数 |

秒级文件还包含 `premium_sample_valid`、`premium_sample_reject_reason`、
`entropy_book_age_ms`、`hedge_book_age_ms`、`cross_venue_skew_ms`、
`short_center_bps`、`long_center_bps`、`active_midline_bps`、
`midline_lag_bps`、`midline_lag_state`、`midline_move_speed_bps_per_10m`、
`sigma_center_bps`、`robust_sigma_bps`、`sigma_window_sec` 和
`premium_data_degraded`。

采集的 edge 为费前口径；分析工具在统计触发频率前会先扣除 `--fees-bps`
（请传入**两边吃单费之和**——零费交易所默认 0.0，对冲腿为 `tradexyz` 时
约为 1.0），因此其表格与建议值可直接填入配置。`--hours 24`
可只分析最近数据；溢价中枢会漂移，请定期重新分析并更新 `config.yaml`。

## 配置说明

策略在 `config.yaml`（严格校验——未知键名直接报错），密钥在 `.env`。
交易市场由命令行指定（`--symbol`、`--hedge`）。完整的双语注释参考：
[config.example.yaml](config.example.yaml)。核心项：

| 键 | 含义 | 默认值 |
|---|---|---|
| `thresholds.midline_bps` | 溢价中枢（必须实测！） | — |
| `thresholds.upper_bps` / `lower_bps` | 入场带宽（> 0） | — |
| `entropy.dex` | Entropy 在 Hyperliquid 上的 dex 名 | `io` |
| `*.taker_fee_bps` | 各所吃单费 | 0.0（tradexyz 对冲腿：1.0） |
| `*.max_position_usd` | 各所持仓上限 | 1000 |
| `*.max_orders_per_min` | 各所每分钟下单预算（滑动 60 秒） | 120；Lighter 对冲腿 30 |
| `sizing.take_fraction` | 吃掉可套利深度的比例 | 0.5 |
| `sizing.max_order_notional_usd` | 单笔名义上限 | 500 |
| `inventory.scale_bps` / `floor_frac` | 库存阶梯（仓位超过上限的 `floor_frac` 后额外加价） | 10 / 0.5 |
| `execution.premium_persist_sec` | 信号需持续多久才触发 | 0.3 |
| `execution.*` | 滑点保护、超时、对账周期等 | 见配置文件 |
| `recorder.*` | 分钟与秒级数据 | `logs/minutes.csv`、`logs/seconds.csv` |
| `logging.dashboard` / `logging.file` | 终端仪表盘；开启时日志写入文件 | 开启，`logs/engine.log` |

## 密钥配置（`.env`，仅实盘需要）

- **Entropy / tradexyz（Hyperliquid）** —— 在
  <https://app.hyperliquid.xyz/API> 创建 API（agent）钱包。`HL_PRIVATE_KEY`
  填 **agent 钱包私钥**，`HL_ACCOUNT_ADDRESS` 填主账户地址。当
  `--hedge tradexyz` 时两条腿默认共用该账户（内部自动共享 nonce 序列）；
  如需分开，设置 `HL_PRIVATE_KEY_XYZ` / `HL_ACCOUNT_ADDRESS_XYZ`。注意给
  所交易的各 dex 分别充入保证金。
- **Lighter** —— `LIGHTER_ACCOUNT_INDEX`、`LIGHTER_API_KEY_INDEX`、
  `LIGHTER_API_PRIVATE_KEY`，必须注册在与启动参数 `--hedge` **相同的部署**上
  （主网与 Robinhood 链是两套独立的账户和密钥——参见
  [lighter-python](https://github.com/elliottech/lighter-python)）。

## 执行机制

- 两条腿**同时发出吃单**：Lighter 用带均价保护的市价单，在鉴权 websocket
  上异步确认成交；Hyperliquid 用 IOC 限价单同步结算（结果未知时轮询
  orderStatus 兜底）。
- **持续性闸门**（`premium_persist_sec`）：信号先"武装"，持续存在才触发，
  过滤单 tick 的假信号。
- **库存阶梯**：仓位超过上限的 `floor_frac` 后，同方向加仓需要线性递增的
  额外溢价，满仓时最高加 `scale_bps`。
- **净敞口对冲**：两腿成交不对等时立即用 reduce-only 单（带滑点保护）
  削减敞口，并每 `reconcile_sec` 与链上仓位对账。
- **故障隔离**：被限频的交易所短暂暂停；交易所不可达（如例行维护）时暂停
  交易并每 `venue_probe_sec` 探测直至恢复；连续 `max_consecutive_errors`
  次执行异常则整体停机。
- **仅实盘**：没有模拟成交模式。`--record-only` 是唯一无风险的运行方式，
  其余都是真金白银。

## 目录结构

```
main.py                  入口（--strategy legacy|maker-hedge）
entropy_arb/config.py    YAML + .env 配置契约与校验
entropy_arb/book.py      订单簿 + 含手续费的套利规模计算
entropy_arb/feeds.py     官方 HL ws + zkLighter ws 行情
entropy_arb/venue_hl.py  Hyperliquid dex 适配器（Entropy、tradexyz）
entropy_arb/venue_lighter.py  zkLighter 适配器（主网、Robinhood 链）
entropy_arb/engine.py    双交易所策略主循环
entropy_arb/strategy_launcher.py  启动策略选择器与 maker-hedge 桥接
entropy_arb/dashboard.py Rich 终端仪表盘
entropy_arb/dashboard_state.py Web 看板原子状态快照
entropy_arb/dashboard_api/ 只读 FastAPI 接口与静态前端
dashboard/                React/Vite 监控界面
entropy_arb/signal.py    动态中枢、regime 与仓位状态纯逻辑
entropy_arb/funding.py   统一资金费快照与成本计算
entropy_arb/recorder.py  分钟聚合与秒级 top-of-book 数据
tools/analyze.py         minutes.csv -> 阈值建议
tools/replay.py          收敛研究与时间序列策略回放
tests/                   python3 -m pytest tests/
```

## 已知风险

- **中枢填错就是亏钱策略。** 溢价中枢会漂移，请定期重新测量并保持
  `config.yaml` 与市场同步。
- **USDG 基差**（`lighter-rh`）：对冲腿以 USDG 计价，持续溢价中有
  一部分是稳定币本身的基差；midline 吸收其水平，但 USDG 的*变动*是真实盈亏。
- **资金费覆盖**：实盘过滤使用公开当前费率而非预测；Replay 会明确报告历史
  覆盖率，不会把长时间缺失伪装成完整数据。
- **薄盘口**：Entropy 深度可能很小；`take_fraction` 与名义上限控制单笔规模，
  但部分成交后对冲腿的滑点是真实存在的。
- **交易时段**：股票类永续（如 SNDK）盘后各所预言机行为不同，建议加宽带宽
  或避开盘后。
- **单腿风险**：一条腿成交后另一条可能失败。机器人会自动对冲并对账，但
  仍需人工关注。

风险自负。本软件直接操作真实资金，本文档不构成任何投资建议。请从最小的
仓位上限开始。

## 开源协议

[MIT](LICENSE)
