# Entropy 私有备份 · 2026-10-21

本仓库是当前工作区的独立备份，包含尚未提交的代码和非敏感配置，不包含原仓库的 Git 历史。原项目和原 `origin` 均未修改。

## 下载完整备份

请登录拥有本仓库访问权限的 GitHub 账户，从 **Releases → backup-20261021** 下载：

- `entropy-project-and-trades-20261021.tar.zst`：代码和交易记录完整包。
- `SHA256SUMS`：完整包的 SHA-256 校验值。

也可在已登录 GitHub CLI 的终端执行：

```bash
gh release download backup-20261021 --repo acclorenzochen/entropy-private-backup-20261021 --dir entropy-backup-download
cd entropy-backup-download
shasum -a 256 -c SHA256SUMS
zstd -d -c entropy-project-and-trades-20261021.tar.zst | tar -xf -
```

解压得到 `project/` 和 `trading-records/`。只下载仓库的源码 ZIP 不会包含 Release 中的交易数据库。

## 保留内容

- 当前 Python 交易程序、前端、测试、脚本、文档和非敏感配置。
- SNDK / NBIS V3 交易日志数据库、旧版交易台账、历史交易数据库备份。
- 订单、成交、执行步骤、交易周期、费用、资金费、账户损益、必要的批次和状态元数据。
- 交易 CSV 记录。

明细、逐文件校验值、各数据库表的行数见 `BACKUP_MANIFEST.json`。历史数据库含重叠记录，不应把不同备份的成交数量或收益简单相加。记录原有的未最终结算/待核账状态保持原样，不代表已完成最终损益核算。

## 明确排除

- `.env`、私钥、API 密钥、令牌、微信凭据及含私有配置的旧压缩包。
- 行情原始记录、盘口/行情回放 JSONL、秒线/分钟线 CSV、看板行情缓存数据库。
- 旧版台账中的 `market_samples`、`minute_samples`、`model_state`、`opportunity_events` 表数据。为保留数据库结构兼容性，这些表可以存在，但没有数据。
- 依赖目录、编译产物、缓存、原 `.git`、旧源码发布副本和分析报告。

与某笔实际交易直接相关的报价、成交价格和执行分析字段属于交易记录，予以保留；不包含连续行情录制。

## 一致性和恢复注意

每个源数据库在只读事务中导出到新数据库，并通过 `PRAGMA quick_check` 和逐表行数校验；原数据库不作修改。不同数据库是分别导出的，不声称跨数据库的原子时间点一致性。

这是归档，不是可直接启动实盘的部署包。不会附带密钥，也不会自动恢复交易。恢复前请自行重新配置凭据，核对交易所真实持仓、未知订单及账户互斥状态；历史授权和控制状态仅作审计，不应直接用于重新开仓。

仓库和 Release 必须保持 **Private**。本次备份不会把原先公开的 `acclorenzochen/entropy-arb` 仓库改成私有，也没有向它推送任何内容。
