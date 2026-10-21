# Lessons

- 顶部运行诊断只使用新鲜的当前 status/同圈诊断；泛化 no_send_entry_check 不能解释成价差不足，历史零成交不能代替当前原因。缺少细分时明说未上报，暂停刷新、旧状态和未来时间须单独标注。

- 看板时间修复须核对运行中的 API 响应、截图圈次及真实账本；构建通过不代表展示链已生效。交易所首笔时间缺一腿时，保留缺失证据并独立展示有来源标签的本地正成交确认/估算时长，不能把确认时间、末笔成交或周期创建时间冒充真实首笔成交。

- 发布实盘重试命令前，离线验证完整 shell → 连接配置 → V3 Settings → 批次授权 → owner 参数边界链。`ENTROPY_V3_CONFIG` 不覆盖 `ENTROPY_CONFIG`；不能只看到 V3 的 200U 就假定连接配置的双腿上限也为 200U。配置校验应在账户初始化前完成，测试不得读取真实 `.env`。

- Treat venue fill quantities as cumulative snapshots. Cap each order at its planned quantity and retain duplicate status reports only as audit data; never apply the same snapshot to inventory or the execution state twice.

- Exchange minimum size/notional rules must be checked for the actual order type and exact reduce-only close. Maker metadata must not silently gate taker recovery; use venue documentation and authoritative order outcomes, while retaining size precision and exposure-increase checks.

- A higher admission threshold can cap losses but does not create positive expectancy. When live closed-loop results are negative, identify the structural source of edge and validate conditional post-cost returns before presenting a stricter threshold as the optimization.

- V3 的亏损预算按策略已实现亏损加活动仓位可执行浮亏判断；原始账户总权益会混入划转和其他仓位，不能直接写入策略停开仓 latch。账户快照缺失或非有限值必须视为未知，不能补成零。

- 增仓门禁必须在真实提交前的最后一个等待之后检查当前周期、双腿有符号仓位和原始有效期，不能只检查退出标记；可靠空仓证据与请求失败无证据要分别处理。追踪适配器内部的旧订单 ACK/nonce 等待，测试等待期间风险变化后零新增提交且旧订单不被取消。

- 费用证据必须绑定所覆盖的累计成交量；数量增长而费用未更新时保留已知部分、清空完整费用。完整逐成交价格与费用不能被重复累计回执降级，价格覆盖和费用覆盖分开验证。双腿证据独立落账，重核从毛价格现金减已知费用绝对重算，不能把旧净现金当毛现金或把未知费用补零。
## 2026-09-10 — 先确认可交易市场边界

- 当时 Entropy 端仅确认 SNDK；市场清单是时变事实，新增品种必须重新读取真实元数据，不能永久沿用旧结论。2026-10-09 公共元数据已确认 `io:NBIS` 和 Lighter 主网 NBIS market 189。

- 离线验证前先读构建脚本，确认输出目录不会被运行服务直接提供；组合 npm 脚本的追加参数只传给最后一个命令。需要隔离产物时直接运行 `vite build --outDir /tmp/...`，不要假定 `npm run build -- --outDir ...` 会重定向 Vite。

- 收益看板先用一行状态与关键收益、一个全局筛选条和紧凑主表建立阅读顺序；不要在页面标题、范围说明、卡片和明细里重复同一状态或金额。改版前后量化主表距页顶的高度，次要诊断折叠但关键风险和未知费用始终可见。

- 看板易读优先于压缩高度：正文和表格保持 14–16px、足够对比与明确中文层级。首页只显示一次汇总和一张交易表；默认统一自然日，不混合不同部署的最近启动时间。内部状态码留在技术详情，未知净收益直接说明缺少什么。

- 总览的请求身份只包含列表筛选；详情的 source/cycle/tab 不得触发列表重取或整页 remount。手动刷新保留上次快照，用 generation/abort 与 source-qualified cycle 防止晚到响应串轮；用真实浏览器验证 DOM 身份、滚动、返回与重复点击。
- 执行解释以同一实际成交量的可执行盘口和 VWAP 为基准，分别展示本腿滑点与两腿间缓存盘口移动；复合价差 bp 不直接换成 U。并行两腿从共同触发分别计时，确认回报时间与交易所成交时间必须明确区分，执行差异不能再从净额扣一次。
- 新面板使用 footer 等语义标签时，检查旧全局选择器是否带入横排、固定宽度或小字号；在实际窄栏和 320px 页面验证标签单行、金额对齐及横向溢出，不能只看 DOM 文本。

- 用户明确确认特定账户零手续费时，可用有账户、批次和订单类型范围的用户政策作为独立核算证据；保留交易所原 NULL，不再仅为零费查成交历史。Entropy 实际费用和资金费仍独立核对，实际非零费优先并使旧最终金额重新待核；不能将该授权推广到其他账户、未来批次、Premium、TWAP 或 integrator。
- 手动双腿平仓在持仓卡只添加简洁入口；确认、进度、结果放点击后的原地浮层，关闭后不占总览空间。历史退出证据融入现有状态行与圈次表，不在持仓卡底部新增常驻结果区。
- 启动/停止的操作意图与单圈平仓、批次风险锁分开建模；用户要求平完继续运行时，一次性 MANUAL_STOP 退出工具不能冒充运行中控制。必须由持锁引擎处理命令与最后提交门禁，停止期间继续管理已有仓位。
- 主导航返回总览/交易记录时清除圈次、详情与分页筛选，保留全局时间/模式；非默认圈次筛选要可见且可直接清除。时间或列表筛选变化重置本地页码，避免隐藏的 open/旧页码让历史看似消失。
