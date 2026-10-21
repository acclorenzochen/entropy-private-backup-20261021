# 开源交易框架借鉴记录

更新日期：2026-09-20。

本项目只借鉴执行模型和不变量，不复制第三方源代码。选型优先级是：真实多场所执行、未知订单结果处理、部分成交、重启恢复、持续维护和明确许可证。普通价差扫描器、只按同一时刻报价假设双腿全成的示例机器人不作为实盘设计依据。

## 主要参考

### Hummingbot

- 仓库：<https://github.com/hummingbot/hummingbot>
- 审阅基线：`2bfaccc48dd49e71a5b6d9b3011808e127dd00cd`
- 许可证：Apache-2.0
- 重点实现：
  - `hummingbot/strategy/cross_exchange_market_making/cross_exchange_market_making.py`
  - `hummingbot/connector/client_order_tracker.py`
  - `hummingbot/strategy_v2/executors/arbitrage_executor/`

采用的思路：

1. Maker 单存续期间持续按当前 taker 对冲价格重算盈利性。若 maker 余量此刻成交后已无法以最低收益要求完成对冲，先撤掉余量；不能只因 maker 价格仍在本地盘口最优档就继续挂单。
2. 每个 maker fill 以交易所成交标识去重，再按实际新增成交量触发 hedge；taker 失败或取消后只补未对冲余量。
3. 活跃订单、已终结但仍可能收到迟到成交的订单、丢失订单分开管理；“本次没查到”不等于“交易所从未接受”。

本轮落地：`entropy-maker-hedge` 新增受保护对冲价、maker/taker 手续费后的 entry 盈利性检查。创建订单前先检查；订单存续期间每轮重算。跌破 `maker.min_entry_profitability_bps` 时确认撤单：无成交则关闭本轮，已有部分成交则只对冲已成交量。

### NautilusTrader

- 仓库：<https://github.com/nautechsystems/nautilus_trader>
- 审阅基线：`46a5658a2f66cf0a798d414dc1b63d98cf10fcc1`
- 许可证：LGPL-3.0
- 重点实现：
  - `docs/concepts/execution/reconciliation.md`
  - `crates/live/src/execution/failure.rs`
  - `crates/live/src/execution/reconciliation.rs`
  - `crates/execution/src/reconciliation/`

采用的思路：

1. 下单失败分成 `NotSent`、`Ambiguous`、`VenueRejected`。是否可重试与交易所是否可能已经执行是两个不同问题；网络超时不能生成终态拒绝，也不能换 ID 盲目重发。
2. 启动时先恢复本地事件，再获取订单、成交和仓位报告，最后才启动策略。运行时持续核对未完成订单和账户仓位。
3. 成交以稳定 `trade_id` 去重；缺失成交只能由交易所报告或可验证的仓位差异生成，并显式标记为 reconciliation 事实。

当前 `entropy` 主执行器已经具备对应不变量：发送前持久化稳定 client order ID、未知结果仅查询原 ID、累计成交按订单上限幂等应用、启动仓位屏障、未完成 execution 恢复和真实仓位对账。本轮没有为形式一致而重写这些已存在的路径。

## 明确不照搬的部分

- Hummingbot 的通用 arbitrage executor 会同时发两边目标数量，但本项目的 IOC 可能部分成交，且两个场所没有原子提交。未建立“双 pending order + 双边独立恢复”状态前，不直接把顺序执行替换成简单 `gather()`。
- NautilusTrader 是完整事件驱动交易平台。本项目是一 symbol、一个 pair 的专用执行器；不引入消息总线、插件系统或 Rust 核心来换取架构外观。
- GitHub 上高星的通用 crypto arbitrage 示例多为扫描器、三角套利或同刻全成模拟，不能证明双场所永续实盘的订单结果、仓位恢复和账务正确性。

## 后续采用顺序

1. 先用本轮 maker 盈利性撤单规则积累 `resting edge -> fill -> hedge markout` 数据，验证减少了负 entry edge，同时没有把真实 maker 成交全部过滤掉。
2. 若顺序 taker 路径的跨腿漂移仍主导损失，再实现两边独立 pending 状态后试验 Concurrent IOC；验收必须覆盖一边未知、一边部分成交和重启恢复。
3. 只有在单执行器的事件量或多 symbol 需求确实出现后，才继续抽取 Nautilus 式统一事件总线。
