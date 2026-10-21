# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

React, Vite, TypeScript, Recharts, plain CSS, FastAPI, and uvicorn. The dashboard is a separate read-only web surface beside the Python trading engine.

## Users

The primary user is the individual operator of one Entropy arbitrage bot. They monitor the bot continuously on desktop and use mobile web for quick status checks.

## Product Purpose

Entropy Arb observes and executes a two-venue perpetual arbitrage strategy. The dashboard lets the operator understand within seconds whether the bot is alive, its current paired position and delta, where premium sits relative to dynamic strategy thresholds, what is blocking new exposure, today's activity and PnL when reliable, and the most recent strategy actions.

## Positioning

The product exposes the strategy's actual decision state—dynamic midline, entry and exit thresholds, regime, funding gate, paired inventory, and execution actions—rather than presenting a generic market terminal or recomputing strategy logic in the browser.

## Operating Context

The trading engine writes atomic status snapshots and append-only CSV market/trade history. A local FastAPI process reads those files and a single-page dashboard polls the read-only API. The trading engine, API, and browser are independent processes; dashboard failure must never affect execution.

## Capabilities and Constraints

- One bot, one strategy, one symbol, and one configured venue pair.
- Read-only observation surface with no trade, pause, configuration, or position-control actions.
- Status polling every two seconds; history, trades, and PnL refresh every five seconds.
- Four core APIs plus health; no WebSocket, database, authentication, complex routing, or global frontend state store.
- Premium history supports 1H, 6H, and 24H ranges.
- Desktop-first responsive layout; mobile is for inspection, with a horizontally scrollable trade table.
- API binds to localhost by default. Remote access and authentication are outside this MVP.
- Timestamps are stored as Unix seconds/UTC and displayed in the browser's local timezone.
- Today means the configured dashboard day timezone; the initial default is Asia/Taipei.
- An unavailable or unclear PnL value is displayed as unavailable, never replaced by a guessed metric.

## Brand Commitments

The product name is “Entropy Arb.” The interface language is concise operational English. It should feel like a restrained dark trading instrument, but not a Bloomberg clone, TradingView clone, or cyberpunk display.

## Evidence on Hand

- Engine state and strategy logic in `entropy_arb/engine.py`, `entropy_arb/signal.py`, and `entropy_arb/funding.py`.
- One-second market history in `logs/seconds.csv`.
- Strategy executions in `logs/trades.csv`.
- Existing terminal dashboard in `entropy_arb/dashboard.py` as content evidence, not visual authority for the new web surface.
- No logo, photography, customer claims, or external proof assets are available and none should be fabricated.

## Product Principles

1. Strategy truth comes from the engine; the dashboard never re-derives trading decisions.
2. Health, opportunity, inventory risk, and recent actions must be readable in five seconds.
3. Observation is isolated from execution so interface failure cannot affect trading.
4. Missing or stale data is communicated explicitly and prior useful context remains visible.
5. Accuracy, readability, and stability outrank decoration or feature count.

## Accessibility & Inclusion

Status and risk must not rely on color alone. The page must support keyboard-visible controls, readable contrast, reduced motion, tabular numerals, and responsive viewing down to a 390-pixel mobile viewport.
