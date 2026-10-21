# V3 本地 owner 控制适配层

本次交付仅接线和离线验收。未应用核心包、启用生产 owner 或启动真实控制服务；生产没有 owner socket 时三个动作不可用。8776 仍是 GET-only。旧 `v3_exit_app` 和一次性 exit CLI 不能替代本协议。

## 边界与前置条件

- 控制服务独立监听 `127.0.0.1:8786`，一个 worker，不接受代理头。不能对外开放或反向代理到公网。
- owner 是既有账户锁的唯一拥有者；适配层不能启动/停止进程、kill、接管锁、解除 MANUAL_STOP、修改授权或交易参数。
- 只有普通 basis 接入。funding/NBIS 返回 `unsupported_source`。完全没有引擎进程时，启动按钮不能启动一个进程。
- owner 核心包与 A/B r2 修改相同核心文件，须合并和复测；不能顺序套用两个 patch。
- socket 路径与五项绑定来自服务端显式配置。私有目录须当前用户拥有且权限0700，socket须当前用户拥有且权限0600，拒绝符号链接与权限扩大。
- 只发送有界 Unix socket JSON；没有 shell、CLI、订单客户端、凭据或 supervisor。
- owner 响应时间 `source_ts` 不能当作 `position_observed_ts`；未知仓位保留 null，不用账本0伪装账户0。

## HTTP 协议

所有响应 `Cache-Control: no-store`。精确 UI Origin，不允许 `*`；要求 loopback peer 与 Host，拒绝 cross-site/same-site Fetch Metadata。会话为 HttpOnly、SameSite=Strict cookie（有效一小时）；每次 POST 要求 Origin、`X-Entropy-CSRF` 和 JSON，体积最多2048字节。重复字段、非有限数与额外 schema 字段被拒绝。浏览器不能选账户、配置、socket、argv、revision 或 owner receipt。

| 请求 | 浏览器输入 | 响应 |
|---|---|---|
| `GET /api/v3-control/session?source=basis` | 无 | enabled、reason_code/reason、csrf_token、state |
| `GET /api/v3-control/state?source=basis` | 已有会话 | enabled、reason_code/reason、state |
| `POST /api/v3-control/preview` | `{source:"basis",action:"start"\|"stop"\|"close",expected_cycle_id?:string}` | preview_id、action、expected_cycle_id、expires_at、state、valuation |
| `POST /api/v3-control/confirm` | `{preview_id,idempotency_key}` | operation_id、operation、state |
| `GET /api/v3-control/operations/{UUID}` | 已有会话 | 同上，刷新 owner 的 canonical 核算 |
| `GET /api/v3-control/operations/by-key/{UUID}?source=basis` | 已有会话 | 按持久幂等键恢复动作 |

close 必须给出 expected_cycle_id；start/stop 不允许给出周期。预检只读，有效15秒，私有 owner receipt 只保存在适配层。confirm 转发预检时的 action/revision/cycle/receipt 和同一幂等键；owner 最终重新验证实例、绑定、意图、周期、有符号数量和订单水位。初次确认之前 owner 重启须重新预检。

当前 owner 协议无毛盈亏估值，`valuation.executable_gross_usd/fees_usd/funding_usd` 都是 null；UI 显示“预计毛盈亏未知，费用和资金费待核算”。不能以旧 quote 补造实时预估。

operation 的顶层 intent/revision 是接受动作时的历史值，当前意图读响应 `state`。腿的 filled_units 是累计快照，重复轮询不能相加。operation 状态直接保留 accepted、executing、flat_pending_accounting、completed、blocked；UNKNOWN 保留原订单引用。仅 `accounting_final=true` 时输出净结果，否则强制 null。完成后查询也刷新 owner，因此迟到真实费可回到 flat_pending_accounting。

失败返回 HTTP 409/503，`detail:{code,message}`。无 owner/不匹配/错误证据都不声明能力。confirm 传输中断返回 `outcome_unknown`，保留原 key；先 by-key 核对，再重试原确认，不能生成另一份动作。HTTP 重启后原 preview 不再可用，但浏览器保留的 key 能在新会话中查询 owner 持久回执。适配层不持久保存凭据或订单状态。

## 离线测试与浏览器 fixture

```bash
.venv/bin/python -m pytest tests_v3/test_owner_control_api.py -q
```

临时 Unix socket 测试只有 fake JSON，没有引擎、签名器、凭据或真实订单。沙盒禁止 Unix socket bind 时，仅该离线测试需要本地执行权限。

独立浏览器 QA 可启动显式 fake fixture（示例 UI 端口5187、控制18786）；它不导入真实引擎，不接任何真实 socket。fixture 初始两腿是模拟257/-257，UNKNOWN 订单是模拟原ID。

```bash
V3_OWNER_UI_ORIGIN=http://127.0.0.1:5187 V3_OWNER_CONTROL_PORT=18786 \
  .venv/bin/python -m uvicorn tests_v3.test_owner_control_api:fake_app \
  --factory --host 127.0.0.1 --port 18786 --workers 1 --no-proxy-headers --no-access-log
```

生产开启仍是单独明确授权的工作。服务端设置如下显式环境变量，缺任何绑定拒绝启动；本次未执行：

```text
V3_OWNER_CONTROL_ENABLED=1
V3_OWNER_UI_ORIGIN=http://127.0.0.1:5186
V3_OWNER_CONTROL_PORT=8786
V3_OWNER_SOCKET=<journal parent>/control/owner.sock
V3_OWNER_ACCOUNT_BINDING_HASH=<owner实际公共账户绑定SHA256>
V3_OWNER_PAIR_IDENTITY=<owner实际pair_identity>
V3_OWNER_DEPLOYMENT=<owner实际deployment>
V3_OWNER_BATCH_ID=<owner实际batch_id>
V3_OWNER_CONFIG_HASH=<owner实际config_hash>
```

已审批且核心完成合并、owner启用后，才可使用 `bash scripts/run_v3_owner_control.sh`。服务正常启动不代表动作已执行，最终用户确认仍必需。
