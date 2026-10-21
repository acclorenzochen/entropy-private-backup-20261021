"""Engine signal math: midline band directions, inventory ladder, scan.

Run:  python3 -m pytest tests/  (or  python3 tests/test_engine.py)
"""
import asyncio
import os
import sys
import tempfile
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from entropy_arb.book import OrderBook  # noqa: E402
from entropy_arb.config import load_config  # noqa: E402
from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.funding import FundingSnapshot  # noqa: E402
from entropy_arb.signal import PairState, StrategyAction  # noqa: E402

NO_ENV = os.path.join(tempfile.gettempdir(), "entropy-arb-no-such.env")


def make_cfg(midline=5.0, upper=4.0, lower=3.0):
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    f.write(f"""
thresholds:
  midline_bps: {midline}
  upper_bps: {upper}
  lower_bps: {lower}
  exit_bps: {min(0.5, upper / 2, lower / 2)}
dynamic_midline:
  enabled: false
profit_guard:
  enabled: false   # These fixtures exercise the legacy statistical gates.
execution:
  premium_persist_sec: 0.0
""")
    f.close()
    cfg = load_config(f.name, NO_ENV, symbol="SNDK", hedge_venue="lighter-rh")
    # Every fixture path is isolated even when a test later enables persistence
    # or cycle exits. Never read or overwrite the workspace's trading state.
    root = tempfile.mkdtemp(prefix="entropy-test-")
    for name in ("trades_csv", "recorder_csv", "recorder_seconds_csv",
                 "dashboard_pnl_state_json", "dashboard_status_json"):
        setattr(cfg, name, os.path.join(root, name))
    cfg.cycle_exit = replace(cfg.cycle_exit, state_json=os.path.join(root, "cycle.json"))
    cfg.persistence = replace(cfg.persistence, migrate_legacy=False)
    return cfg


class StubVenue:
    def __init__(self, key, label, cap=10000.0, fee=0.0):
        self.key, self.name = key, label
        self.cap_usd, self.fee_bps = cap, fee
        self.size_decimals, self.min_base, self.min_quote = 4, 1e-4, 10.0
        self.position, self.cash, self.volume_usd = 0.0, 0.0, 0.0
        self.orders_per_min = 30
        self.last_traded_ts = 0.0
        self.book = OrderBook()
        self.sent = []
        self.next_result = None
        self.next_results = []
        self.resolve_results = []
        self.resolved = []

    def ready_to_trade(self):
        return True

    def set_book(self, bid, ask, sz=50.0):
        self.book.apply_hl([[{"px": str(bid), "sz": str(sz)}],
                            [{"px": str(ask), "sz": str(sz)}]])

    def px_round(self, px, round_up):
        return px

    async def send_taker(self, **kw):
        self.sent.append(kw)
        if self.next_results:
            result = self.next_results.pop(0)
            return {"order_ref": f"{self.key}-{len(self.sent)}", **result}
        if self.next_result is not None:
            result, self.next_result = self.next_result, None
            return {"order_ref": f"{self.key}-{len(self.sent)}", **result}
        return {"order_ref": f"{self.key}-{len(self.sent)}",
                "status": "filled", "filled_base": kw["qty"],
                "avg_px": kw["limit_px"], "err": None,
                "unresolved": False}

    async def resolve_order(self, order_ref, **_kw):
        self.resolved.append(order_ref)
        if self.resolve_results:
            result = self.resolve_results.pop(0)
            return {"order_ref": order_ref, **result}
        return {"order_ref": order_ref, "status": "timeout",
                "filled_base": 0.0, "avg_px": None, "err": None,
                "unresolved": True}


def make_engine(**thr):
    cfg = make_cfg(**thr)
    cfg.trades_csv = os.path.join(tempfile.mkdtemp(), "trades.csv")
    eng = Engine(cfg)
    eng.entropy = StubVenue("entropy", "ENTROPY")
    eng.hedge = StubVenue("hedge", "RH")
    eng.venues = {"entropy": eng.entropy, "hedge": eng.hedge}
    eng._step, eng._min_base, eng._min_notional = 1e-4, 1e-4, 10.0
    return eng


def approx(a, b, tol=1e-9):
    assert abs(a - b) <= tol, f"{a} != {b}"


def test_eff_threshold_directions():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    e, h = eng.entropy, eng.hedge
    # sell entropy: hurdle = midline + upper = 9
    approx(eng._eff_threshold(buy=h, sell=e), 9.0)
    # buy entropy: hurdle = lower - midline = -2 (unwind side of a positive
    # midline is deliberately cheap — that's what completes the round trip)
    approx(eng._eff_threshold(buy=e, sell=h), -2.0)
    # round trip nets upper + lower regardless of midline sign
    for m in (-7.0, 0.0, 12.5):
        eng.cfg.midline_bps = m
        total = eng._eff_threshold(buy=h, sell=e) + eng._eff_threshold(buy=e, sell=h)
        approx(total, 7.0)


def test_inventory_ladder():
    eng = make_engine()
    eng.cfg.inventory_scale_bps, eng.cfg.inventory_floor_frac = 10.0, 0.5
    e, h = eng.entropy, eng.hedge
    e.set_book(99.9, 100.1)   # mid 100
    h.set_book(99.9, 100.1)
    approx(eng._inv_add_bps(e, h), 0.0)          # flat: dead zone
    e.position = 90.0                             # long $9k of $10k cap
    v = eng._inv_add_bps(e, h)                    # buying entropy adds long
    assert 7.5 < v < 8.5, v                       # u=0.9 -> ~+8
    approx(eng._inv_add_bps(h, e), 0.0)           # selling entropy reduces
    h.position = -90.0                            # hedge short $9k too
    v2 = eng._inv_add_bps(e, h)                   # both legs add -> max()
    assert abs(v2 - v) < 0.6, (v, v2)             # max, not sum


def run_scan(eng):
    async def go():
        # first pass arms the direction, second passes the persistence gate
        # (premium_persist_sec is 0 in the test config)
        eng._scan(__import__("time").time())
        return eng._scan(__import__("time").time())
    return asyncio.run(go())


def test_scan_fires_sell_entropy_above_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # Entropy is materially rich vs hedge: above midline+upper=9 -> sell
    # entropy, while the projected round trip also clears the new economic
    # execution-cost floor.
    eng.entropy.set_book(100.50, 100.52)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert sell.key == "entropy" and buy.key == "hedge"
    assert plan.exp_edge_usd > 0


def test_scan_quiet_inside_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # entropy 5 bps rich = exactly on the midline: inside the band, no trade
    eng.entropy.set_book(100.04, 100.06)
    eng.hedge.set_book(99.99, 100.01)
    assert run_scan(eng) is None


def test_scan_fires_buy_entropy_below_band():
    eng = make_engine(midline=5.0, upper=4.0, lower=3.0)
    # Entropy is materially cheap (below midline-lower=+2): buy entropy,
    # while the projected round trip also clears the new economic
    # execution-cost floor.
    eng.entropy.set_book(99.50, 99.52)
    eng.hedge.set_book(99.99, 100.01)
    best = run_scan(eng)
    assert best is not None
    buy, sell, plan = best
    assert buy.key == "entropy" and sell.key == "hedge"


def test_open_direction_policy_blocks_only_new_exposure():
    eng = make_engine(midline=0.0, upper=4.0, lower=4.0)
    eng.entropy.set_book(99.99, 100.01)
    eng.hedge.set_book(99.99, 100.01)
    eng.cfg.allow_sell_entropy_open = False
    eng.cfg.allow_buy_entropy_open = True
    candidates = eng._build_candidates(__import__("time").time())
    assert [candidate.direction for candidate in candidates] == ["buy_entropy"]

    eng.entropy.position, eng.hedge.position = -1.0, 1.0
    candidates = eng._build_candidates(__import__("time").time())
    assert any(candidate.action == StrategyAction.REDUCE for candidate in candidates)


def test_scan_respects_position_caps():
    eng = make_engine(midline=0.0, upper=1.0, lower=1.0)
    eng.entropy.set_book(100.14, 100.16)
    eng.hedge.set_book(99.99, 100.01)
    eng.entropy.position = -100.0   # entropy already short at its cap
    eng.entropy.cap_usd = 10000.0
    eng.hedge.position = 100.0
    eng.hedge.cap_usd = 10000.0
    assert run_scan(eng) is None


def test_position_state_builds_only_same_direction_add_and_reduce():
    eng = make_engine(midline=0, upper=4, lower=4)
    eng.entropy.set_book(100, 100.02)
    eng.hedge.set_book(100, 100.02)
    eng.entropy.position, eng.hedge.position = -10, 10
    cs = eng._build_candidates(__import__("time").time())
    assert [c.action for c in cs] == [StrategyAction.REDUCE, StrategyAction.ADD]
    assert cs[0].direction == "buy_entropy" and cs[0].reduce_only
    assert cs[1].direction == "sell_entropy" and not cs[1].reduce_only


def test_reduce_cap_and_no_crossing_through_flat():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98.0, 98.01, sz=100)
    eng.hedge.set_book(99.99, 100.0, sz=100)
    eng.entropy.position, eng.hedge.position = -10, 10
    candidates = eng._build_candidates(__import__("time").time())
    reduce = candidates[0]
    assert reduce.action == StrategyAction.REDUCE
    assert reduce.cap_notional <= 10 * eng.entropy.book.mid()
    plan, _ = eng._plan(reduce.buy, reduce.sell, reduce.cap_notional,
                        reduce.threshold_bps)
    assert plan.qty <= 10


def test_regime_and_funding_never_block_reduce():
    eng = make_engine(midline=0, upper=4, lower=4)
    eng.entropy.set_book(100, 100.02)
    eng.hedge.set_book(100, 100.02)
    eng.entropy.position, eng.hedge.position = -5, 5
    shifted = __import__("dataclasses").replace(eng.model_snapshot,
                                                regime_shift=True)
    eng._premium_snapshot = lambda now: shifted
    cs = eng._build_candidates(__import__("time").time())
    assert any(c.action == StrategyAction.REDUCE for c in cs)
    eng.cfg.funding_enabled = True
    eng.cfg.funding_on_unavailable = "block_open"
    cs = eng._build_candidates(__import__("time").time())
    assert any(c.action == StrategyAction.REDUCE for c in cs)


def test_funding_is_charged_in_cycle_economics_not_signal_threshold():
    eng = make_engine(midline=0, upper=5, lower=5)
    eng.cfg.funding_enabled = True
    eng.entropy.funding = FundingSnapshot(2, 100, "test")
    eng.hedge.funding = FundingSnapshot(1, 100, "test")
    eng.cfg.funding_stale_sec = 1e12
    eng.cfg.funding_expected_hold_hours = 2
    cs = eng._build_candidates(101)
    short = next(c for c in cs if c.direction == "sell_entropy")
    long = next(c for c in cs if c.direction == "buy_entropy")
    assert short.threshold_bps == 5
    assert long.threshold_bps == 5
    eng.cfg.funding_max_adverse_cost_bps = 1
    cs = eng._build_candidates(101)
    assert {c.direction for c in cs} == {"sell_entropy", "buy_entropy"}


def test_entropy_first_leg_uses_fixed_protection_and_negative_exit_threshold():
    eng = make_engine(midline=0, upper=5, lower=5)
    eng.cfg.leg_slippage_bps = 6
    eng.cfg.min_edge_surplus_bps = .5
    eng.entropy.set_book(100.50, 100.51)
    eng.hedge.set_book(99.99, 100.0)
    c = eng._build_candidates(__import__("time").time())[0]
    c.threshold_bps = 5
    plan, _ = eng._plan(c.buy, c.sell, 500, 5)
    trade = eng._execution_bounds(c, plan)
    assert trade is not None
    approx(trade.per_leg_slippage_bps, 6)
    c.threshold_bps = -1
    trade = eng._execution_bounds(c, plan)
    assert trade is not None and trade.edge_surplus_bps > 0


def test_reduce_execution_sets_reduce_only_on_both_legs():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    eng.entropy.position, eng.hedge.position = -1, 1
    trade = run_scan(eng)
    assert trade is not None
    asyncio.run(eng._execute(trade))
    assert eng.entropy.sent[-1]["reduce_only"] is True
    assert eng.hedge.sent[-1]["reduce_only"] is True
    assert eng._pair_state() == PairState.FLAT


def test_entropy_zero_fill_never_sends_lighter_and_retries_quickly():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.cfg.cooldown_sec = 30
    eng.cfg.no_fill_retry_sec = 3
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    eng.entropy.next_result = {
        "status": "canceled", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": False}
    before = __import__("time").time()
    asyncio.run(eng._execute(trade))
    assert len(eng.entropy.sent) == 1
    assert eng.hedge.sent == []
    assert eng.trades == 0
    assert 26 <= before - eng.last_trade_ts <= 28


def test_entropy_partial_fill_hedges_only_actual_quantity_on_lighter():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    partial = round(plan.qty / 2, 4)
    eng.entropy.next_result = {
        "status": "partial", "filled_base": partial, "avg_px": 98.01,
        "err": None, "unresolved": False}
    asyncio.run(eng._execute(trade))
    assert len(eng.hedge.sent) == 1
    assert eng.hedge.sent[0]["qty"] == partial
    assert eng.trades == 1
    assert abs(eng.entropy.position + eng.hedge.position) < eng._step / 2


def test_second_leg_cancel_is_retried_and_is_not_a_circuit_failure():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    eng.hedge.next_result = {
        "status": "canceled", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": False}
    asyncio.run(eng._execute(trade))
    assert len(eng.entropy.sent) == 1
    assert len(eng.hedge.sent) == 2
    assert eng._pair_state() == PairState.LONG_ENTROPY
    assert eng.trades == 1
    assert eng.consec_errors == 0


def test_second_leg_partial_is_retried_until_pair_is_balanced():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    partial = round(plan.qty / 2, 4)
    eng.hedge.next_results = [
        {"status": "partial", "filled_base": partial, "avg_px": 100.0,
         "err": None, "unresolved": False},
        {"status": "filled", "filled_base": plan.qty - partial,
         "avg_px": 100.0, "err": None, "unresolved": False},
    ]

    asyncio.run(eng._execute(trade))

    assert len(eng.hedge.sent) == 2
    assert abs(eng.hedge.sent[1]["qty"] - (plan.qty - partial)) < 1e-9
    assert eng._pair_state() == PairState.LONG_ENTROPY
    assert eng.active_execution is None


def test_second_leg_timeout_is_resolved_before_retrying_remaining_quantity():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    partial = round(plan.qty / 2, 4)
    eng.hedge.next_results = [{
        "status": "timeout", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": True,
    }]
    eng.hedge.resolve_results = [{
        "status": "partial", "filled_base": partial, "avg_px": 100.0,
        "err": None, "unresolved": False,
    }]

    asyncio.run(eng._execute(trade))

    assert len(eng.hedge.resolved) == 1
    assert len(eng.hedge.sent) == 2
    assert abs(eng.hedge.sent[1]["qty"] - (plan.qty - partial)) < 1e-9
    assert eng._pair_state() == PairState.LONG_ENTROPY
    assert eng.active_execution is None


def test_locked_execution_releases_venue_locks_before_recovery():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    eng.hedge.next_results = [{
        "status": "canceled", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": False,
    }, {
        "status": "filled", "filled_base": plan.qty, "avg_px": 100.0,
        "err": None, "unresolved": False,
    }]

    async def run_locked():
        await eng._vlock(eng.entropy.key).acquire()
        await eng._vlock(eng.hedge.key).acquire()
        await eng._execute_locked(trade)

    asyncio.run(run_locked())

    assert len(eng.hedge.sent) == 2
    assert not eng._vlock(eng.entropy.key).locked()
    assert not eng._vlock(eng.hedge.key).locked()
    assert eng.active_execution is None
    assert eng.trades == 1


def test_unresolved_first_leg_blocks_new_scan_until_it_is_resolved():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    eng.entropy.next_results = [{
        "status": "timeout", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": True,
    }]
    eng.entropy.resolve_results = [{
        "status": "timeout", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": True,
    }]

    asyncio.run(eng._execute(trade))

    assert eng.active_execution is not None
    assert eng.active_execution.phase.value == "reconciling"
    assert eng._scan(__import__("time").time()) is None
    assert eng.hedge.sent == []


def test_unresolved_second_leg_is_not_duplicated_while_waiting_for_settlement():
    eng = make_engine(midline=0, upper=2, lower=2)
    eng.entropy.set_book(98, 98.01)
    eng.hedge.set_book(99.99, 100)
    c = eng._build_candidates(__import__("time").time())[1]
    plan, _ = eng._plan(c.buy, c.sell, c.cap_notional, c.threshold_bps)
    trade = eng._execution_bounds(c, plan)
    eng.hedge.next_results = [{
        "status": "timeout", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": True,
    }]
    eng.hedge.resolve_results = [{
        "status": "timeout", "filled_base": 0.0, "avg_px": None,
        "err": None, "unresolved": True,
    }]

    asyncio.run(eng._execute(trade))

    assert len(eng.hedge.sent) == 1
    assert eng.active_execution is not None
    assert eng.active_execution.pending_leg == "hedge"
    assert eng._scan(__import__("time").time()) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"{name:40s} OK")
