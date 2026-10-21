# Entropy Arbitrage — Control Plane Simplification

## 2026-10-10 — NBIS multi-round support (code/tests only)

- [x] Remove the one-round guard without a hard-coded five-round cap; retain positive explicit batch authorization and optional session limits. Omitted session limit does not grant unlimited batch authorization; the example remains unapproved with a safe one-round placeholder.
- [x] Separate provisional funding posting from verified next-round admission; require real normal funding completion, authoritative flat/terminal orders, verified trading accounting and successful closed-window ledger polls. Persist early-exit batch stops and retain late accounting refresh after limits. Old schema evidence flags migrate to false, never assumed verified.
- [x] Verify multiple rounds, restart counts, delayed fees/settlements, native partial/UNKNOWN recovery, and unchanged basis-mode accounting gates offline. `790 passed` across all `tests_v3` and affected fee/config tests with DNS/socket connections denied; 3 dependency deprecation warnings. Source/new-test whitespace checks passed. Updated manual runbook; no live launch or leverage/authorization changes.

## 2026-10-09 — NBIS funding-hedge mode (no live launch authorized)

- [x] Implement opt-in funding-hedge lifecycle using V3 execution/recovery, durable settlement IDs and reversal samples, liquidation-priority reduce-only exits, and one position cycle only. Entry common lot is .01 NBIS; internal accounting is .0001 to preserve native Lighter partial fills. Funding estimates use actual oracle/index prices, liquidation distance uses mark.
- [x] Verify zero/late funding, restart idempotency, normalized signed pair income, 3 samples + 60s reversal threshold, $0.50 remaining-cost gate, partial/UNKNOWN recovery and account exclusion. 760 offline V3 and affected adapter/config checks passed with DNS/socket connections blocked; targeted diff whitespace check passed. Added regressions for deferred/zero-fill finality, persistent UNKNOWN opening handoff, in-flight risk gating and late funding with missing fee history.
- [x] Run public-only NBIS market/depth/funding preflight: 13/13 checks passed, no signer/leverage/orders/production Journal writes. Independent unsigned account reads confirmed both NBIS positions 0. Public matched quantity .88 NBIS, notionals 199.1616 / 199.2144 USDC; market leverage maxima 15x / 10x. Separate public oracle/index read returned 226.18 / 226.19 with fresh funding. Manual launch and limits are in NBIS_FUNDING_RUNBOOK.md.
- User explicitly disabled fixed round/batch loss thresholds: both are null. Funding reversal remaining-cost cap .50 USDC and liquidation distance .02 remain enabled. Authorization example is still approved_live=false; NBIS was not launched, leverage was not changed, and existing SNDK PID 59991 was preserved.
- Private settlement ledger / delayed real posting behavior / account fee tier were not verified live. Explicit zero ledger entries count, missing records never do; funding PnL stays provisional without a posting watermark and is recomputed from a durable trade-cash baseline plus actual settlement total, never appended twice.
- Public metadata confirmed `io:NBIS` (size precision 2, max leverage 15) and Lighter mainnet NBIS market 189 (size precision 4); old SNDK-only market assumption no longer applies.

## 2026-10-09 — V3 live leverage and notional update

- [x] Normal V3 live startup applies configured venue leverage before account streams/order entry; Hyperliquid now verifies `activeAssetData` readback. Preflight/unlock do not change exchange leverage.
- [x] User-approved settings: Entropy and Lighter leverage 10; V3 per-leg planned notional cap and batch authorization cap both $200. Existing batch ID, $3 loss budget, 100-cycle limit, and accumulated accounting remain unchanged.
- [x] Offline verification: 476 V3 and affected adapter/config tests passed with socket connections and DNS blocked; targeted `git diff --check` passed.
- [x] Flat preflight passed and workspace live process restarted as PID 59991; `READY` confirmed. Exchange readback: Entropy isolated 10x; Lighter cross, initial margin fraction 10.00%. Dashboard reports configured/authorized/effective notional caps all 200.
- Evidence/backups: `logs/v3/leverage10-notional200-20261009.1VQtJ3/`. Read-only recorder and dashboard processes were preserved; old release/launchd services were not changed or enabled.

## 2026-10-04 — V3 source baseline unification

- [x] Compare workspace and `entropy-edge-v2-strategy-risk-20260924`; identify release fixes and preserve workspace-only changes.
- [x] Integrate the deployed execution, order-evidence, accounting, and observer fixes with their regression tests into the workspace source baseline.
- [x] Run isolated offline V3 and affected adapter/reporting checks; record the unified source baseline and remaining differences.
- Scope: no strategy/authorization parameter changes, live Journal writes, process or launchd changes, production restart, or module splitting.
- Unified source: `main_v3.py`, all V3 production modules, both venue adapters, and the economics/opportunities/validation/evidence tools match the release byte-for-byte. Runtime SHA-256: `4833a7b84eacd6f61241a243e6b66cbadf3efb08d0d975bbe9f8f891a1fe3c53`.
- Preserved: workspace-only V3 dashboard and tests, SOCKS dependency, pinned Lighter SDK, and the original strategy-risk regression. Test fixtures and the offline benchmark explicitly isolate reference checkpoints; the benchmark's isolation-only additions intentionally differ from the release.
- Verification: 420 V3 tests plus 36 affected shared-adapter/legacy-path tests passed together with socket connections and DNS blocked (`456 passed`, 3 dependency deprecation warnings). Offline handoff benchmark: 8/8 samples passed with Journal `FULL`; fixture wall times are not production latency measurements. `git diff --check` passed.
- Protection check: release source files, live config, both authorization files, saved live effective manifest, and live Journal main/WAL hashes stayed unchanged. Source integration does not switch the launch root or prove a currently running process has loaded the workspace.
- Remaining limitation: dashboard manifest compatibility still compares absolute module paths; workspace/release directory differences may appear as mismatches despite equal file hashes. That preexisting dashboard behavior and live cutover are outside this source-only task.

## 2026-09-08 review implementation

- [x] A — terminal-order recovery risk release, venue-only exit minimums, deep-book close bounds, shared finite exit policy.
- [x] B — live/time-aware execution costs, policy-scoped hold/EV calibration, effective policy diagnostics.
- [x] C — cycle window/final fee accounting, delayed/partial execution replay and liquidation valuation.
- [x] D — action send budgets with complete-cycle reserves, focused fault regressions and full offline suite.
- Final verification: 334 offline tests passed; CLI replay and `git diff --check` passed. See `tasks/cycle-review-implementation-2026-09-09.md`.
- Live config/services were not changed. A legacy fixture wrote the default cycle checkpoint; its verified test artifact was quarantined and fixture paths were isolated. The original checkpoint was not recoverable from a known backup; no substitute trading state was invented.
- Empirical profitability and higher live throughput remain unproven; this rollout does not authorize live tuning or restart.

## Goal

Implement the latest “Control-Plane Simplification & High-Frequency Clean-Circle Plan” without changing live trading state until the code and replay tests pass.

## Work plan

- [x] 1A — simplify admission: pure statistical signal, funding/reference costs only in whole-cycle EV, retain only hard data/position/order/margin/absolute-limit safety gates.
- [x] 1B — execution: add bounded OPEN/CLOSE room selection from fill telemetry, remove fixed minimum-room admission, keep unknown-order and hedge-completion invariants, and make sustained zero-fill retry rate-bounded.
- [x] 1C — accounting/observability: distinguish Clean Circle from Recovery Circle, expose the execution funnel and attribution fields without retaining unbounded in-memory history.
- [x] 1D — update example/config compatibility and focused regressions.
- [x] 1E — run focused tests, then the full pytest suite and an offline replay/smoke check. The live bot was intentionally not restarted.

## Acceptance checks

- OPEN/ADD signal thresholds do not change when funding/fees/execution estimates change.
- Funding, fees, depth, execution cost, and reference uncertainty are charged once in whole-cycle EV.
- Non-invalid volatility/lag/regime states are telemetry/soft economics, not global OPEN blockers.
- Normal negative-EV OPEN cannot pass economic admission; Bootstrap may only sample a bounded negative-EV OPEN within its explicit loss budget.
- A known terminal zero-fill may retry within a bounded rate budget; an unknown order never duplicates.
- After the first REDUCE fill, hedge completion ignores the economic floor and finishes the already-started reduce-only execution.
- Clean and Recovery circles are reported separately and PnL attribution remains invariant.

## 2026-10-10 approved dashboard readability update

- [x] Materialized and viewed approved design `libfile_4719d4ba685c8191837ee4ed2a094e72` on this Mac at `/tmp/entropy-approved-design-4j8h6lwc/approved-design.png`.
- [x] Implemented the approved three-item navigation, readable summary and completed-cycle table, two-leg expansion, Chinese runtime status, shared Beijing-day default, and unknown-fee accounting in the existing dashboard. Changes are frontend and GET-only display projections.
- [x] App/Node TypeScript checks, isolated `/tmp` build, 101 related pytest cases, 11 router checks, and diff whitespace check passed. Build preserved at `/tmp/entropy-dashboard-approved-build` and `/tmp/entropy-dashboard-profit-build`; production dist was not replaced.
- [x] Recovered the Mac CUA connection and detached only the authorized preview services: 5186 frontend and 8776 GET-only API. HTTP and real data remained available across calls.
- [x] Actual desktop/390px checks passed: today empty state, basis/funding modes, missing fees, two-leg prices, four deduplicated orders, refresh, repeat expansion, source/cycle deep links and browser back. Fixed numeric 0/1 reduce-only display matching and mobile detail/popup clipping. Final browser console had no warnings/errors; main table font is 15px and mobile selectors are 14px.
- [x] Saved actual screenshots to Library: desktop `libfile_fa8b9674c5ec81919ffad01486186927`, mobile `libfile_93c669b153ac8191aea87589d91493d4`. Local originals and GET evidence are in `/tmp/entropy-approved-qa`. Screenshots show actual recent-24-hour SNDK/NBIS data; the default Beijing today view is empty.
- No trader, journal, ledger, account or live config was modified; the isolated preview uses `/tmp` assets.

## 2026-10-11 approved same-page cycle detail

- [x] Viewed the approved right-panel design and complaint screenshots locally. Implemented a stable overview table with same-page tabs, source-qualified detail fetches, preserved refresh state, and a full-width mobile bottom panel.
- [x] Replaced mixed bp chains with five stages based on persisted same-quantity executable books. Own slippage and cached book movement are separate; parallel close uses each leg's own clock and shared trigger. Fee/net accounting is not changed or charged again.
- [x] Passed app/Node/fixture TypeScript, isolated /tmp builds, 102 related pytest cases, 24 execution checks, 3 real cycle replays, 11 routing checks, 18 real-browser fixture checks, and actual desktop/390px interactions. Production dist and trader PID 48206 were not modified.
- [x] Actual desktop screenshot saved to Library: libfile_7242a398453481918cbd1ba0bf35bb62. Mobile and parallel-close originals are /tmp/entropy-panel-qa; their Library batch timed out with unknown save outcome, and a read-only filename lookup did not confirm them. No duplicate write was attempted.

## 2026-10-11 dashboard time range

- [x] Default first visits to rolling 7 days; add unbounded all history, remove 1h/6h options, and retain today/24h/session/batch/custom. Retired scopes fall back to 7 days and discard stale pagination cursors.
- [x] Apply the same scope/source to overview, complete-cycle list, and detail requests. All-history queries omit time bounds and inactive batch/session drafts; retain full-cycle detail and unknown-fee accounting.
- [x] App/Node TypeScript, isolated build, router check, 110 related pytest cases, and diff whitespace check passed. History fixtures cover 160 source-qualified cycles, undated records, and pagination beyond 100 entries.
- [x] Deployed only the isolated preview assets and GET-only API at localhost:5186 / 8776. Actual Mac Chrome verified default, options, all history, old links, historical same-page detail, refresh/reload, and batch-to-all switching. Real API pagination returned all 25 complete cycles across 4 pages versus 10 in 7 days, matching overview IDs.
- Evidence: /tmp/entropy-time-range-qa, including task.patch, real-api-verification.json, browser request captures and actual screenshots. No trading process, parameters, authorization or ledger was operated on.

## 2026-10-11 cycle-panel accounting layout

- [x] Materialized and viewed the supplied Library screenshot on this Mac with Library identity/xattrs preserved. Reproduced global footer flex/width/9px-font rules squeezing the detail accounting footer.
- [x] Isolated the footer layout and restored four single-line rows: price PnL, funding, fees, net profit. Values stay right aligned; unknowns remain pending. Repeated accounting text and provenance remain in the existing disclosure.
- [x] App/Node/fixture TypeScript, isolated Vite builds, time-range check, 68 related pytest cases, 20 actual browser fixture checks, and whitespace check passed. Updated only the existing /tmp preview assets; display PIDs 55695/85448 stayed running.
- [x] Actual Mac Chrome checked desktop narrow right panel, 390px and 320px, disclosure, three tabs, close/reopen, repeat view and refresh. Accounting labels/values remain single-line and right aligned; footer/panel have no horizontal overflow. 7d/all still show 10/25 complete cycles.
- [x] Saved actual screenshots to Library: desktop libfile_1d546f1e51f48191a51a6957c4bb3d86, 390px libfile_4af8e97b96b08191bd227360debf784f, 320px libfile_2af59508494081919a3175e72797bf87. Evidence and scoped patch: /tmp/entropy-panel-layout-CsjgkK. No trading/accounting logic, live process, ledger, parameters or Lighter scheduling changes.
