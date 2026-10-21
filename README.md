# entropy-arb

**[中文文档 / Chinese documentation → README.zh-CN.md](README.zh-CN.md)**

Open-source two-venue perp arbitrage bot. One leg is always **Entropy**
(the `io` builder dex on Hyperliquid); the other leg — the hedge — is one of:

| `--hedge` | venue | quote | taker fee | protocol |
|---|---|---|---|---|
| `lighter` | Lighter mainnet | USDC | 0 bps | zkLighter ws (diff books, async settle) |
| `lighter-rh` | Lighter Robinhood chain | **USDG** | 0 bps | zkLighter ws |
| `tradexyz` | Hyperliquid trade.xyz dex | USDC | ~1 bps | HL l2Book, sync IOC settle |

> **Referral links** — signing up through these supports this project:
> - Entropy — Tier 4 referral, 100% rebates: <https://entropy.io/?r=yourquantguy>
> - Lighter Robinhood chain: <https://robinhoodchain.lighter.xyz/?referral=QUANT>
> - trade.xyz (Hyperliquid): <https://app.hyperliquid.xyz/join/QUANTGUY>

When the same symbol trades rich on one venue and cheap on the other, the bot
simultaneously sells the rich book and buys the cheap book with taker orders,
carrying a delta-neutral position until the premium reverts and the opposite
crossing unwinds it. Every price it acts on is the **actual order book of the
exchange that will fill the order** — Hyperliquid books come from the official
websocket (`wss://api.hyperliquid.xyz/ws`), Lighter books from Lighter's
official websocket.

While it runs — even with no credentials and no strategy — it records both
books as compatible **1-minute CSV bars** and **1-second top-of-book rows**.
The bundled analyzer and replay tool turn those rows into threshold,
convergence, holding-risk, funding and execution-cost evidence.

## The signal

The band is three numbers in `config.yaml`, derived by you from recorded
data:

```
premium_bps = (Entropy price / hedge price − 1) × 10 000

                          ┌──────────────  SELL entropy + BUY hedge
midline + upper  ───────────────────────────────────────────────────
                                       ▲
midline          ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─   the premium's usual level
                                       ▼
midline − lower  ───────────────────────────────────────────────────
                          └──────────────  BUY entropy + SELL hedge
```

- `midline_bps` — where the premium normally sits. Cross-venue premiums are
  rarely centered at zero (different oracles, different quote assets, listing
  premia), so a zero-centered band would fire one direction only, cap out and
  never unwind. Measure where the premium actually sits and type it in.
- `upper_bps` / `lower_bps` — the entry bands on each side of the midline.

Both hurdles are applied to **executable** prices (entropy bid vs hedge ask,
and vice versa) and are **net of both venues' taker fees** — the engine adds
fees on top before a slice qualifies. A full round trip therefore nets
**≥ upper + lower bps after fees by construction**.

One consequence worth understanding: with `midline_bps: 5`, the buy-entropy
hurdle is `lower − midline`, which can be **negative**. That is intentional —
if entropy is persistently 5 bps rich, buying it at a 0 bps premium is 5 bps
cheap versus its own equilibrium, and that trade is the profitable unwind of
an earlier sell at `midline + upper`. It also means a **wrong midline loses
money**: if you type `midline_bps: 5` while the true premium sits at 0, the
bot happily buys entropy at fair value all day. Measure first, then trade —
that is what the recorder and analyzer are for.

## Adaptive entry bands

`thresholds.upper_bps` and `thresholds.lower_bps` remain the minimum entry
bands. When `adaptive_bands.enabled` is true, each band is widened from the
same robust dispersion estimate used by the dynamic midline model:

```text
effective_band = min(max(base_band, configured_min, entry_sigma_mult × robust_sigma), max_band)
```

The effective band can never be below the original base threshold. If
`robust_sigma_bps` exceeds `max_sigma_bps`, the volatility guard blocks
`OPEN` and `ADD`; `REDUCE`, emergency `HEDGE`, and `RECONCILE` remain allowed.

## Configuration hot reload

`hot_reload.enabled` is on by default. The bot checks `config.yaml` every
`hot_reload.poll_sec` seconds, fully parses and validates the candidate, and
applies signal/model settings from the next strategy scan. An in-flight order
is not repriced, and the recorded entry context of an existing cycle is not
rewritten.

The runtime-reloadable settings include `thresholds`, `dynamic_midline`,
`premium_model`, `adaptive_bands`, `midline_lag_guard`, and the execution
cadence settings `premium_persist_sec`, `cooldown_sec`, and
`min_edge_surplus_bps`. For example, changing
`adaptive_bands.min_upper_bps` from `5.0` to `3.0` affects the next OPEN/ADD
scan without restarting.

Account, venue, credentials, leverage, position caps, order sizing, inventory,
funding jobs, order-price protection, data paths, and dashboard ports remain
restart-only. If those fields change while the bot is running, the log marks
them as `restart required` and the current process keeps the old values;
`hot_reload.enabled` also requires a restart to change, while
`hot_reload.poll_sec` is reloadable. An invalid candidate is rejected and the
last valid configuration remains active.

## Premium data quality

Every premium sample is admitted only when both books have a recent
`last_update_ts` and the two timestamps are within
`premium_model.max_cross_venue_skew_ms`. A sample older than
`premium_model.max_book_age_ms`, missing either book, or exceeding the skew
limit is recorded as invalid with its reason and is excluded from both rolling
midlines and sigma. Invalid samples are not forward-filled. After the
continuous-invalid grace period, `premium_data_degraded` blocks new exposure;
reductions, emergency hedges, and reconciliation remain available.

## Midline lag guard

The model compares the 20-minute `short_center` with the rate-limited
`active_midline`. A persistent warning uses the base movement speed; a
persistent larger lag switches to the accelerated speed, capped by
`absolute_max_move_bps_per_10m`. A lag at or above the block threshold for its
configured persistence window blocks `OPEN` and `ADD` only. The active midline
continues moving toward the 3-hour `long_center`, so the system can recover;
`REDUCE`, `HEDGE`, and `RECONCILE` are not blocked by this guard.

## Separate sigma window

The dynamic midline uses the configured long window (3 hours by default),
while robust sigma uses its own valid-premium window (30 minutes by default).
Sigma is `1.4826 × MAD`, where the MAD is centered on the median of that sigma
window, preventing a slow center migration from being mistaken for current
book noise. No automatic optimizer is included.

## Cycle-anchored exits

After a paired `OPEN`, the engine stores that cycle's entry midline, robust
sigma, band, threshold, direction, and paired quantity. Existing inventory is
then reduced against the cycle's entry midline plus the fixed `exit_bps`, so a
later dynamic-midline move cannot rewrite the original mean-reversion
assumption. A midline drift beyond `cycle_exit.max_midline_drift_bps` blocks
only further `ADD` actions. Partial reductions keep the cycle active; state is
reset only after reconciliation confirms a genuinely flat pair. A restart
with real inventory but no saved anchor is fail-closed: `ADD` is blocked while
`REDUCE` and emergency hedging remain available.

The replay tool uses the same pure adaptive-band and cycle helpers without
lookahead. Compare parameters without changing live configuration:

```bash
python3 tools/replay.py --csv logs/seconds.csv --config config.yaml \
  --adaptive-entry-sigma-mults 1.5,2,2.5 --cycle-drift-limits 3,4,5
```

## Dynamic strategy mode

`midline_bps` is now the startup seed and warmup fallback. With
`dynamic_midline.enabled`, the engine samples one fresh premium per second,
uses rolling medians and MAD for robust centers/dispersion, and rate-limits
how quickly the active midline can move. Persistent divergence enters
`SHIFT / REDUCE_ONLY`: `OPEN` and `ADD` stop, while `REDUCE`, emergency delta
hedging, and reconciliation remain available.

Actions are based on reconciled venue positions. A flat pair may open; an
existing short-Entropy or long-Entropy pair may add in the same direction or
reduce using the tighter `thresholds.exit_bps` band. Both reduction legs are
reduce-only, quantity is capped by paired inventory, and an opposite signal
cannot cross through flat in one execution.

Public funding is normalized to bps/hour. Missing, stale, or excessive
adverse funding blocks only new exposure; acceptable adverse cost raises the
entry hurdle, while favorable funding never lowers the base hurdle. Normal
Execution is Entropy-first: an Entropy zero-fill never sends the hedge order;
an actual Entropy fill is hedged on Lighter at that exact quantity. The first
leg and second leg use independent price-protection limits, while final
emergency flattening keeps a wider bound.

## Quick start

```bash
git clone https://github.com/your-quantguy/entropy-arb.git && cd entropy-arb
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # data collection needs only this

cp config.example.yaml config.yaml       # the strategy (thresholds, sizing, risk)
cp .env.example .env                     # credentials — required to trade
```

The markets are **not** in the config file — you state them explicitly on
every start: `--symbol` (traded on both venues) and `--hedge` (one of
`lighter`, `lighter-rh`, `tradexyz`; Entropy is always the
other leg).

There is **no paper mode** — the bot either collects data (`--record-only`)
or trades live. Validate with recorded data and tiny position caps, not with
simulated fills.

### Selecting a strategy

The repository has one startup entry point and two explicit strategies. The
existing Entropy-first taker arbitrage remains the backward-compatible
default (`legacy`). The new single-pair Entropy maker / Lighter hedge strategy
is selected with `--strategy maker-hedge`; it keeps its own YAML schema,
SQLite state, logs, and dashboard port so the two execution state machines do
not share checkpoints accidentally. Both strategies reuse the existing `.env`
credentials; the maker-hedge launcher does not copy or print secrets.

```bash
# Existing strategy — equivalent to the historical command
python3 main.py --strategy legacy --symbol SNDK --hedge lighter-rh

# New strategy — paper mode, no real orders
python3 main.py --strategy maker-hedge --paper --no-dashboard

# New strategy — read-only connectivity/state check
python3 main.py --strategy maker-hedge --preflight-only
```

The maker-hedge source remains in the sibling `entropy-maker-hedge/` project
so it can be rolled back independently. The unified launcher changes only
which strategy starts; it does not merge or reinterpret an active cycle from
the other strategy. If the exchange has positions but that strategy has no
local cycle, it fails closed and requires manual reconciliation.

Only one live strategy may use the shared account at a time. Both startup
paths use `logs/entropy-arb.lock`; `--preflight-only`, `--record-only`, and
maker-hedge `--paper` do not acquire the live lock.

### WeChat ClawBot hourly reports

The `legacy` engine can send one read-only WeChat ClawBot/iLink report per
hour. It includes the current process/strategy/position state, reconciliation
state, hourly account-equity change, ledger PnL, today's PnL attribution,
Entropy fill volume, Entropy unpaired-leg volume, hourly and daily execution
counts, and the lifetime count retained by SQLite.

Enable it explicitly in the local, ignored `config.yaml`:

```yaml
wechat_clawbot:
  enabled: true
  interval_sec: 3600
  send_startup: false
```

Configure the notification channel by QR code; this does not start trading:

```bash
python3 main.py --wechat-setup
```

The command opens the short-lived QR image in the default browser, waits for
scan confirmation, and saves the returned credentials to the ignored local
file `logs/wechat_clawbot_credentials.json` with restrictive permissions. It
does not modify `.env`, acquire the trading lock, or submit orders. If the
browser does not open automatically, use `--no-open` and open the printed QR
link manually.

After the scan succeeds, restart the notification/trading process once and
send any message to ClawBot from the target WeChat account to bind the
recipient and session context. The local state file
`logs/wechat_clawbot_state.json` stores only the recipient, context token, and
incremental cursor. Delivery failures are logged and retried on the next
interval without stopping or changing trading.

The legacy environment variables below remain supported as local overrides
but are not required for QR setup (never commit real values):

```dotenv
WECHAT_CLAWBOT_TOKEN=
WECHAT_CLAWBOT_TARGET_USER_ID=
# Optional; leave blank to learn the session from the first inbound message
WECHAT_CLAWBOT_CONTEXT_TOKEN=
```

The QR token is never included in dashboard state or logs. If you use the
legacy environment override, it takes precedence over the QR credentials.
Equity snapshots are written by the engine even with `--no-dashboard`, so the
hourly account-PnL change remains measurable in headless mode.

**1. Collect data first** (no credentials needed):

```bash
python3 main.py --record-only --symbol SNDK --hedge lighter-rh
```

Let it run for at least 24–72 hours. It writes the compatible
`logs/minutes.csv` plus one top-of-book row per second to `logs/seconds.csv`.
Rows include book ages, cross-venue skew, sample validity, rejection reason,
both rolling centers, active midline, lag state, and the independent sigma
window.

**2. Analyze and set your thresholds:**

```bash
python3 tools/analyze.py
```

It prints the premium distribution, how often each candidate band would have
fired, and a ready-to-paste `thresholds:` block for `config.yaml`.

Then test convergence, MAE/MFE, holding time, funding and slippage stress:

```bash
python3 tools/replay.py --csv logs/seconds.csv --config config.yaml --hours 72
```

Replay is a chronological top-of-book simulation, not full-L2 history or a
guarantee of real fills.

**3. Go live** — fill in `.env`, install the signing SDKs, and start with
the smallest position caps that clear the venue minimums:

```bash
pip install -r requirements-live.txt
python3 main.py --symbol SNDK --hedge lighter-rh
```

Running without `--record-only` sends real orders immediately once both
feeds are fresh and the band is crossed.

**Dashboard.** On a terminal the bot shows a live Rich dashboard: both
books with age/spread, positions and caps, equity and session PnL, the
executable premium of each direction against its full hurdle (fees and
inventory surcharge included, ● = armed), recorder progress, the last
executions, and a tail of the log (the full log goes to `logging.file`,
default `logs/engine.log`). It works in `--record-only` too. Add `--cn` to
display the dashboard in Chinese. Use `--no-dashboard` for plain console
logs (nohup/systemd — off-terminal runs fall back automatically), or set
`logging.dashboard: false`.

### Read-only web dashboard

The optional web dashboard is a separate localhost-only monitor. Enable the
top-level `dashboard.enabled` setting so the engine atomically publishes
`logs/status.json`, then build and start the two read-only processes:

```bash
cd dashboard && npm install && npm run build && cd ..
uvicorn entropy_arb.dashboard_api.app:app --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765`. For frontend development, run `npm run dev` in
`dashboard/`; Vite proxies `/api` to port 8765. The API reads only the local
snapshot and SQLite ledger and has no trading controls. A dashboard or API
failure cannot interrupt the strategy. `/api/dashboard` returns status,
history, trades, and PnL in one coherent response instead of combining four
different polls.

When the SQLite file already exists, the API opens it read-only and does not
run legacy CSV or schema migrations. Set `ENTROPY_ARB_MIGRATE_LEGACY=1` only
for an explicit one-time migration, then unset it again.

In live mode, a background audit polls both venues every 10 seconds. The first
pass looks back seven days and later passes look back 15 minutes. Hyperliquid
uses the authoritative `fee` in `userFillsByTime`; Lighter uses authenticated
`/api/v1/trades` maker/taker fees. Once matched, the ledger replaces the
config-based estimate with the actual fee and the dashboard labels the result
as verified, estimated, or pending. A reporting failure changes the fee status
only; it never blocks order submission.

During runtime, `logs/entropy_arb.sqlite3` is the durable source for execution
checkpoints, orders, observed fill increments, position reconciliation,
estimated funding, PnL attribution, and historical market samples. Startup
idempotently imports `trades.csv`, `seconds.csv`, `minutes.csv`, and their
`.old` rotations. To run that migration explicitly:

```bash
python3 tools/migrate_legacy.py --db logs/entropy_arb.sqlite3
```

After the first leg fills, the execution remains in
`first_leg → hedge_leg → reconciling → verified`, keeps resolving the same
order reference, and sends only the remaining hedge quantity. On restart the
engine reads exchange positions before resuming the same execution; it will
not start a new execution while reconciliation or net exposure is unresolved.

For macOS production runs, launchd can supervise the bot. The launcher uses a
single-process flock, restarts abnormal exits, and writes supervisor output:

```bash
./scripts/install_launchd.sh
launchctl print gui/$(id -u)/com.entropy.arb
```

The fault-replay test covers first-leg fill, second-leg timeout, process
restart, original-order resolution, and continued hedge completion. Run all
tests with:

```bash
uv run --with pytest --with httpx2 --with-requirements requirements.txt pytest -q
```

## Data collection & analysis

The recorder runs automatically in every mode (`recorder.enabled: true`).
Once per second it samples both live books; once per minute it writes a row:

| column | meaning |
|---|---|
| `minute_ts`, `time_utc` | minute start (epoch seconds, ISO UTC) |
| `entropy_bid/ask`, `hedge_bid/ask` | last fresh top-of-book of the minute |
| `premium_open/high/low/close/mean/std_bps` | mid-to-mid premium of Entropy over the hedge |
| `sell_edge_mean/max_bps` | executable premium for SELL entropy (entropy bid / hedge ask − 1) |
| `buy_edge_mean/max_bps` | executable premium for BUY entropy (hedge bid / entropy ask − 1) |
| `samples` | how many of the ~60 seconds both books were fresh |

The seconds file also contains `premium_sample_valid`,
`premium_sample_reject_reason`, `entropy_book_age_ms`,
`hedge_book_age_ms`, `cross_venue_skew_ms`, `short_center_bps`,
`long_center_bps`, `active_midline_bps`, `midline_lag_bps`,
`midline_lag_state`, `midline_move_speed_bps_per_10m`, `sigma_center_bps`,
`robust_sigma_bps`, `sigma_window_sec`, and `premium_data_degraded`.

Recorded edges are pre-fee; the analyzer subtracts `--fees-bps` (pass the
**sum** of both venues' taker fees — default 0.0 for the zero-fee venues,
~1.0 with a `tradexyz` hedge) before counting firings, so its table and
suggestions translate directly into config values. `--hours 24` restricts to
recent data; premiums drift, so re-run it regularly and update
`config.yaml`.

## Configuration

Strategy lives in `config.yaml` (validated — unknown keys are startup
errors), credentials in `.env`, and the markets on the command line
(`--symbol`, `--hedge`). Full commented reference:
[config.example.yaml](config.example.yaml). The essentials:

| key | meaning | default |
|---|---|---|
| `thresholds.midline_bps` | premium center (measure it!) | — |
| `thresholds.upper_bps` / `lower_bps` | entry bands (> 0) | — |
| `entropy.dex` | Entropy's dex name on Hyperliquid | `io` |
| `*.taker_fee_bps` | per-venue taker fee | 0.0 (tradexyz hedge: 1.0) |
| `*.max_position_usd` | per-venue position cap | 1000 |
| `*.max_orders_per_min` | per-venue send budget (sliding 60 s) | 120; lighter hedges 30 |
| `sizing.take_fraction` | fraction of crossable depth taken | 0.5 |
| `sizing.max_order_notional_usd` | per-slice cap | 500 |
| `inventory.scale_bps` / `floor_frac` | inventory ladder (extra bps past `floor_frac` of the cap) | 10 / 0.5 |
| `execution.premium_persist_sec` | edge must persist before firing | 0.3 |
| `execution.*` | slippage bounds, timeouts, reconcile cadence… | see file |
| `recorder.*` | minute + one-second data | `logs/minutes.csv`, `logs/seconds.csv` |
| `logging.dashboard` / `logging.file` | Rich dashboard on a tty; log file while it runs | on, `logs/engine.log` |

## Credentials (`.env`, live only)

- **Entropy / tradexyz (Hyperliquid)** — create an API ("agent") wallet at
  <https://app.hyperliquid.xyz/API>. `HL_PRIVATE_KEY` is the **agent** key,
  `HL_ACCOUNT_ADDRESS` your main account address. With `--hedge tradexyz`
  both legs share this account by default (one nonce sequence is handled
  internally); set `HL_PRIVATE_KEY_XYZ` / `HL_ACCOUNT_ADDRESS_XYZ`
  to split them. Fund the dex-specific clearinghouses you trade.
- **Lighter** — `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`,
  `LIGHTER_API_PRIVATE_KEY`, registered on the **same deployment** as your
  `--hedge` flag (mainnet and the Robinhood chain are separate accounts and
  keys — see [lighter-python](https://github.com/elliottech/lighter-python)).

## How execution works

- Both legs are **taker** orders sent concurrently: Lighter market orders
  with average-price protection settling on the authenticated account
  websocket; Hyperliquid IOC limits settling synchronously (with
  orderStatus polling for unknown outcomes).
- A **persistence gate** (`premium_persist_sec`) arms each direction and only
  fires if the edge survives — one-tick phantoms are filtered.
- **Inventory ladder**: past `floor_frac` of a venue's cap, adding to the
  position requires linearly more edge, up to `scale_bps` extra at the cap.
- **Net-delta hedge**: if legs fill unevenly, the imbalance is immediately
  reduced (reduce-only, price-protected), and positions are reconciled
  against the chain every `reconcile_sec`.
- **Failure containment**: a rate-limited venue pauses briefly; an
  unreachable venue (e.g. exchange maintenance) pauses trading and is probed
  every `venue_probe_sec` until it recovers; `max_consecutive_errors`
  execution pathologies halt the engine entirely.
- **Live-only**: there is no simulated-fill mode. `--record-only` is the
  risk-free way to run it; anything else trades real money.

## Layout

```
main.py                  entry point (--strategy legacy|maker-hedge)
entropy_arb/config.py    YAML + .env contract, validation
entropy_arb/book.py      order books + fee-aware crossing/sizing math
entropy_arb/feeds.py     official HL ws + zkLighter ws book feeds
entropy_arb/venue_hl.py  Hyperliquid dex adapter (Entropy, tradexyz)
entropy_arb/venue_lighter.py  zkLighter adapter (mainnet, Robinhood chain)
entropy_arb/engine.py    the two-venue strategy loop
entropy_arb/strategy_launcher.py  startup selector and maker-hedge bridge
entropy_arb/dashboard.py Rich terminal dashboard
entropy_arb/dashboard_state.py atomic web-dashboard snapshot contract
entropy_arb/dashboard_api/ read-only FastAPI endpoints + static frontend
dashboard/                React/Vite monitoring interface
entropy_arb/signal.py    rolling midline, regime and pair-state pure logic
entropy_arb/funding.py   venue-neutral funding snapshots and cost math
entropy_arb/recorder.py  1-minute bars + 1-second top-of-book rows
tools/analyze.py         minutes.csv -> suggested thresholds
tools/replay.py          convergence study + chronological strategy replay
tests/                   python3 -m pytest tests/
```

## Known risks

- **A wrong midline is a losing strategy.** The premium center drifts;
  re-measure regularly and keep `config.yaml` current.
- **USDG basis** (`lighter-rh`): the hedge quotes in USDG. Part of any
  persistent premium is the stablecoin itself; your midline absorbs the
  level, but a USDG *move* is real PnL.
- **Funding coverage**: live filtering uses public current rates, not a
  forecast. Replay reports historical coverage and does not fill long gaps.
- **Thin books**: Entropy depth can be tiny; `take_fraction` and notional
  caps keep clips small, but slippage on the hedge leg after a partial fill
  is real.
- **Market hours**: for equity perps (e.g. SNDK), off-hours oracle regimes
  differ per venue; consider wider bands or not trading them.
- **One-leg risk**: a leg can fail after the other filled. The bot hedges
  and reconciles automatically, but you should still watch it.

Use at your own risk. This is trading software operating with real money;
nothing here is investment advice. Start with tiny position caps.

## License

[MIT](LICENSE)
