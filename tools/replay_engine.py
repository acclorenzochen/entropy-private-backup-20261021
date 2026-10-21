"""Chronological offline venue harness around the production Engine.

Only exchange I/O and the clock are simulated. Entry, close limits, campaign
retries, recovery, accounting and quotas run through Engine._evaluate. Snapshot
resolution is a constraint: every submit settles on a later row, even at zero
configured latency. Funding is rate-accrual stress, not exchange settlement.
Run sequentially in a dedicated process (module clock patches are not threadsafe).
"""
from __future__ import annotations

import asyncio
import copy
import importlib
import math
import os
import sys
import tempfile
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook, floor_step
from entropy_arb.engine import Engine
from entropy_arb.funding import FundingSnapshot
from entropy_arb.cycle_policy import exit_net_pnl
from tools.replay import ExecutionScenario, _row_quality


class ReplayVenue:
    def __init__(self, harness, conf, *, step, min_quote):
        self.harness, self.key, self.name = harness, conf.key, conf.label
        self.conf, self.kind = conf, conf.kind
        self.cap_usd, self.fee_bps = conf.cap_usd, conf.fee_bps
        self.orders_per_min = min(conf.orders_per_min, harness.scenario.orders_per_minute)
        self.min_base, self.min_quote = step, min_quote
        self.size_decimals = max(0, round(-math.log10(step)))
        self.position = self.exchange_position = self.cash = self.volume_usd = 0.0
        self.last_traded_ts = 0.0
        self.book, self.funding = OrderBook(), None
        self.results = {}

    def ready_to_trade(self):
        return self.book.ready

    def px_round(self, px, round_up):
        # No tick metadata in seconds.csv; this assumption is reported.
        return px

    async def fetch_position(self):
        return self.exchange_position

    async def resolve_order(self, order_ref, **kwargs):
        return self.results[order_ref]

    async def send_taker(self, *, is_buy, qty, limit_px, reduce_only=False, **kwargs):
        h = self.harness
        state = h.engine.active_execution
        recovery = state is not None and state.recovery_attempt > 0
        phase = 'first' if self.key == 'entropy' else 'hedge'
        delay = (h.scenario.recovery_delay_sec if recovery else
                 h.scenario.first_delay_sec if phase == 'first' else h.scenario.hedge_delay_sec)
        submitted = h.now
        order_ref = kwargs.get('order_ref') or f'{self.key}-{len(self.results)+1}'
        if order_ref in self.results:
            return self.results[order_ref]
        self.results[order_ref] = dict(order_ref=order_ref, status='timeout',
            filled_base=0.0, avg_px=None, unresolved=True)
        event = dict(submitted_ts=submitted, ts=None, venue=self.key, phase=phase,
            action=state.action if state else 'recovery', reduce_only=reduce_only,
            requested_qty=qty, limit_px=limit_px, qty=0.0, price=None, unresolved=True)
        h.events.append(event)
        if not h.advance_to(submitted + delay, require_next=True):
            return self.results[order_ref]
        sign = 1 if is_buy else -1
        quote = self.book.best_ask() if is_buy else self.book.best_bid()
        size = self.book.asks.get(quote, 0.0) if is_buy else self.book.bids.get(quote, 0.0)
        px = quote * (1 + sign * h.slippage_bps / 1e4)
        fraction = (h.scenario.first_fill_fraction if phase == 'first'
                    else h.scenario.hedge_fill_fraction)
        filled = floor_step(min(qty * fraction, size), h.step)
        if reduce_only:
            filled = min(filled, max(-sign * self.exchange_position, 0.0))
        zero = h.scenario.zero_fill_every
        if (not self.book.ready or sign * (px - limit_px) > 1e-10
                or (zero and len(h.events) % zero == 0)):
            filled = 0.0
        fee = filled * px * self.fee_bps / 1e4
        self.exchange_position += sign * filled
        net = sum(v.exchange_position for v in h.engine.venues.values())
        h.max_unhedged_usd = max(h.max_unhedged_usd, abs(net * px))
        h.cashflow -= sign * filled * px + fee
        h.fees += fee
        h.turnover += filled * px
        result = dict(order_ref=order_ref, status='filled' if filled >= qty-1e-12 else 'canceled',
            filled_base=filled, avg_px=px if filled else None, fee_usd=fee,
            fee_source='exchange_api', unresolved=False)
        self.results[order_ref] = result
        event.update(ts=h.now, qty=filled, price=px, unresolved=False)
        return result


class ReplayHarness:
    def __init__(self, rows, cfg, scenario, slippage_bps, step, min_quote):
        if not rows or any(b.ts <= a.ts for a, b in zip(rows, rows[1:])):
            raise ValueError('replay requires nonempty strictly chronological rows')
        if (not math.isfinite(slippage_bps) or slippage_bps < 0
                or not math.isfinite(step) or step <= 0 or min_quote < 0):
            raise ValueError('invalid replay cost or venue sizing')
        self.rows, self.cfg, self.scenario = rows, cfg, scenario
        self.slippage_bps, self.step, self.min_quote = slippage_bps, step, min_quote
        self.index, self.now = -1, rows[0].ts
        self.events, self.decisions = [], []
        self.cashflow = self.fees = self.turnover = self.funding = 0.0
        self.unhedged_seconds = self.recovery_seconds = self.max_unhedged_usd = 0.0
        self.engine = None

    def advance(self):
        if self.index + 1 >= len(self.rows):
            return False
        row = self.rows[self.index + 1]
        if self.index >= 0:
            dt = row.ts - self.now
            net = sum(v.exchange_position for v in self.engine.venues.values())
            self.unhedged_seconds += dt if abs(net) > self.step / 2 else 0.0
            self.recovery_seconds += dt if self.engine.active_execution and (
                self.engine.active_execution.recovery_attempt > 0) else 0.0
            for v in self.engine.venues.values():
                if self.cfg.funding_enabled and v.funding is not None:
                    rate, mark = v.funding.rate_bps_per_hour, v.book.mid()
                    pnl = -v.exchange_position * mark * rate / 1e4 * dt / 3600
                    self.cashflow += pnl
                    self.funding += pnl
                    self.engine.ledger.record_funding_event(ts=row.ts, venue=v.key,
                        position_qty=v.exchange_position, mark_px=mark,
                        rate_bps_h=rate, pnl_usd=pnl, source='replay_rate_accrual')
        self.index += 1
        self.now = row.ts
        for key, prefix in [('entropy', 'e'), ('hedge', 'h')]:
            v = self.engine.venues[key]
            bid, ask = getattr(row, prefix+'_bid'), getattr(row, prefix+'_ask')
            v.book.apply_hl([[dict(px=bid, sz=getattr(row, prefix+'_bid_size'))],
                             [dict(px=ask, sz=getattr(row, prefix+'_ask_size'))]])
            age = getattr(row, key+'_book_age_ms', None) or 0.0
            v.book.last_update_ts = row.ts - age / 1000
            v.book.alive_ts = v.book.last_update_ts
            # An explicitly invalid recording cannot provide executable books.
            v.book.ready = row.premium_sample_valid
            rate = getattr(row, prefix+'_funding')
            if rate is not None:
                v.funding = FundingSnapshot(rate, row.ts, 'replay')
        quality = _row_quality(row, self.cfg)
        if self.cfg.dynamic_midline_enabled:
            if quality.valid:
                self.engine.premium_model.observe(row.ts, row.premium)
            self.engine.last_premium_sample_quality = quality
            self.engine.premium_health.observe(row.ts, quality)
        self.engine.model_snapshot = self.engine.premium_model.snapshot(row.ts)
        self.engine._record_hedge_quote(row.ts)
        net = sum(v.exchange_position for v in self.engine.venues.values())
        self.max_unhedged_usd = max(self.max_unhedged_usd, abs(net * row.h_bid))
        return True

    def advance_to(self, target, *, require_next=False):
        if require_next and not self.advance():
            return False
        while self.now < target:
            if not self.advance():
                return False
        return True

    async def sleep(self, delay):
        self.advance_to(self.now + max(delay, 0.0))
        await asyncio.sleep(0)

    async def run(self):
        self.engine.startup_reconciled = True
        self.engine._schedule_poke = lambda delay: None  # next recorded observation wakes it
        failures = []
        execute = self.engine._execute
        async def checked_execute(trade):
            try:
                return await execute(trade)
            except Exception as exc:
                failures.append(exc)
                raise
        self.engine._execute = checked_execute
        scan = self.engine._scan
        def traced_scan(now):
            trade = scan(now)
            self.decisions.append(dict(ts=now,
                selected=None if trade is None else trade.candidate.action.value+':'+trade.candidate.direction,
                budgets={key: {name: value.get(name) for name in (
                    'allowed', 'reason', 'exit_reason', 'net_profit_usd',
                    'projected_cycle_pnl_usd', 'calibrated_cycle_pnl_usd',
                    'cycle_model_samples', 'risk_exit')}
                    for key, value in self.engine.profit_checks.items()
                    if value.get('checked_ts') == now}))
            return trade
        self.engine._scan = traced_scan
        while self.advance():
            await self.engine._evaluate()
            if failures:
                raise RuntimeError("offline engine execution failed") from failures[0]
            self.engine._sync_cycle_with_positions()
        summary = self.engine.ledger.cycle_summary(since=self.rows[0].ts, until=self.now)
        cycles = [dict(r) for r in self.engine.ledger._conn.execute(
            'SELECT * FROM cycle_records ORDER BY open_first_fill_ts')]
        completed = [c for c in cycles if c['flat_ts'] is not None and c['flat_ts'] <= self.now]
        realized = sum(float(c['total_cycle_pnl_usd'] or 0) for c in completed)
        e, h = self.engine.entropy, self.engine.hedge
        last = self.rows[-1]
        mtm = self.cashflow + e.exchange_position * (last.e_bid+last.e_ask)/2 + h.exchange_position * (last.h_bid+last.h_ask)/2
        liquidation = exit_net_pnl(cashflow_usd=self.cashflow,
            entropy_qty=e.exchange_position, hedge_qty=h.exchange_position,
            entropy_bid=last.e_bid, entropy_ask=last.e_ask, hedge_bid=last.h_bid, hedge_ask=last.h_ask,
            entropy_fee_bps=e.fee_bps, hedge_fee_bps=h.fee_bps, slippage_bps=self.slippage_bps)
        return dict(decision_source='Engine._evaluate/_scan/_execute',
            validation_mode='prequential: only earlier fill outcomes train later decisions',
            period_sec=self.now-self.rows[0].ts, samples=len(self.rows),
            round_trips=len(completed), completed_cycles_per_hour=summary['cycles_per_hour'],
            completed_net_pnl_usd=realized, net_pnl_usd=realized,
            open_mtm_pnl_usd=mtm-realized, open_liquidation_pnl_usd=liquidation-realized,
            total_liquidation_pnl_usd=liquidation, fees_usd=self.fees,
            funding_estimate_usd=self.funding, turnover_usd=self.turnover,
            entropy_position=e.exchange_position, hedge_position=h.exchange_position,
            inventory_flatness=self.engine._inventory_flatness(),
            unfinished_cycles=sum(c['open_first_fill_ts'] is not None and c['flat_ts'] is None for c in cycles),
            order_attempts=len(self.events),
            zero_fills=sum(not x['unresolved'] and x['qty']==0 for x in self.events),
            partial_fills=sum(not x['unresolved'] and 0<x['qty']<x['requested_qty']-1e-12 for x in self.events),
            unhedged_seconds=self.unhedged_seconds, recovery_seconds=self.recovery_seconds,
            max_unhedged_usd=self.max_unhedged_usd,
            order_budget=self.engine._order_budget_snapshot(self.now),
            pending_order=self.engine.active_execution.to_dict() if self.engine.active_execution else None,
            execution_events=self.events, decisions=self.decisions, cycle_kpis=summary,
            assumptions=dict(depth='recorded top level only', price_tick='continuous',
                size_step=self.step, min_quote=self.min_quote, funding='continuous rate accrual',
                future_fills='strictly later observation; unobserved acknowledgements stay unresolved'))


def simulate_engine(rows, cfg, *, notional=100.0, slippage_bps=0.0,
                    execution_scenario=None, size_step=0.0001, min_quote=10.0,
                    adaptive_entry_sigma_mult=None, cycle_drift_limit=None,
                    long_window_sec=None, base_move_bps_per_10m=None,
                    sigma_window_sec=None, max_cross_venue_skew_ms=None):
    if not math.isfinite(notional) or notional <= 0:
        raise ValueError('notional must be positive and finite')
    cfg = copy.deepcopy(cfg)
    cfg.max_order_notional = notional
    for field, value in [('dynamic_long_window_sec', long_window_sec),
                         ('dynamic_base_move_bps_per_10m', base_move_bps_per_10m)]:
        if value is not None:
            setattr(cfg, field, value)
    if adaptive_entry_sigma_mult is not None:
        cfg.adaptive_bands = replace(cfg.adaptive_bands, enabled=True,
                                    entry_sigma_mult=adaptive_entry_sigma_mult)
    if sigma_window_sec is not None:
        cfg.adaptive_bands = replace(cfg.adaptive_bands, sigma_window_sec=sigma_window_sec)
    if cycle_drift_limit is not None:
        cfg.cycle_exit = replace(cfg.cycle_exit, max_midline_drift_bps=cycle_drift_limit)
    if max_cross_venue_skew_ms is not None:
        cfg.premium_model = replace(cfg.premium_model, max_cross_venue_skew_ms=max_cross_venue_skew_ms)
    h = ReplayHarness(rows, cfg, execution_scenario or ExecutionScenario(),
                      slippage_bps, size_step, min_quote)
    with tempfile.TemporaryDirectory(prefix='entropy-replay-') as directory, ExitStack() as stack:
        root = Path(directory)
        for field in ('trades_csv', 'recorder_seconds_csv', 'recorder_csv',
                      'dashboard_pnl_state_json', 'dashboard_status_json'):
            setattr(cfg, field, str(root / field))
        cfg.cycle_exit = replace(cfg.cycle_exit, state_json=str(root/'cycle.json'))
        cfg.persistence = replace(cfg.persistence, migrate_legacy=False)
        for name in ('engine', 'book', 'trade_ledger', 'execution_state', 'execution_cost', 'signal'):
            module = importlib.import_module('entropy_arb.'+name)
            if hasattr(module, 'time'):
                clock = SimpleNamespace(**vars(module.time))
                clock.time = clock.monotonic = clock.perf_counter = lambda: h.now
                stack.enter_context(patch.object(module, 'time', clock))
        module = importlib.import_module('entropy_arb.engine')
        async_proxy = SimpleNamespace(**vars(asyncio))
        async_proxy.sleep = h.sleep
        stack.enter_context(patch.object(module, 'asyncio', async_proxy))
        h.engine = Engine(cfg, env_file='/dev/null', ledger_path=':memory:')
        try:
            h.engine.entropy = ReplayVenue(h, cfg.entropy, step=size_step, min_quote=min_quote)
            h.engine.hedge = ReplayVenue(h, cfg.hedge, step=size_step, min_quote=min_quote)
            h.engine.venues = {v.key: v for v in (h.engine.entropy, h.engine.hedge)}
            h.engine._step = h.engine._min_base = size_step
            h.engine._min_notional = max(cfg.min_order_notional, min_quote)
            return asyncio.run(h.run())
        finally:
            h.engine.ledger.close()
