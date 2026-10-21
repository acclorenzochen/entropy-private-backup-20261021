from __future__ import annotations

import json
import hashlib
from decimal import Decimal
from datetime import datetime
from pathlib import Path
import sqlite3

from fastapi.testclient import TestClient
import pytest

from entropy_arb.dashboard_api.v3_app import create_app
from entropy_arb.dashboard_api.v3_projection import (
    conservative_attribution, cycle_detail, cycle_facts, market_points, overview,
    resolve_window, trader_cycle_view)
from entropy_arb.dashboard_api.v3_readmodel import (
    MarketTail, SourcePaths, compact_market, journal_source, requote_market)


def fixture_snapshot():
    live = {"process_id": 1, "start_ts": 100.0, "build_hash": "build",
            "batch_id": "batch", "current_state": "HOLDING"}
    journal = {name: [] for name in (
        "batches", "cycles", "live_cycles", "orders", "live_steps",
        "cycle_economics", "execution_economics_steps", "request_events")}
    for n in range(40):
        cid = f"cycle-{n}"
        paired = n < 8
        done = n < 7 or not paired
        journal["live_cycles"].append({
            "cycle_id": cid, "batch_id": "batch", "started_ts": 110.0 + n,
            "updated_ts": 150.0 + n, "phase": "DONE" if done else "OPEN",
            "entropy_units": 0 if done else 10, "hedge_units": 0 if done else -10,
            "position_evidence_authoritative": 1 if done else 0})
        journal["cycles"].append({"cycle_id": cid, "batch_id": "batch",
                                  "completed_ts": 160.0 + n if done else None,
                                  "accounting_final": int(done and paired),
                                  "pnl_usd": "-0.000242" if done and paired else None,
                                  "result_kind": "clean" if paired else "zero_fill"})
        journal["orders"].append({
            "order_ref": f"e-{n}", "cycle_id": cid, "venue": "entropy",
            "side": "buy", "reduce_only": 0, "submitted_ts": 111.0 + n,
            "state": "TERMINAL", "filled_units": 10 if paired else 0})
        if paired:
            journal["orders"].append({
                "order_ref": f"h-{n}", "cycle_id": cid, "venue": "hedge",
                "side": "sell", "reduce_only": 0, "submitted_ts": 112.0 + n,
                "state": "TERMINAL", "filled_units": 10})
    return {"id": "fixture", "generated_at": 200.0, "journal": journal,
            "market": [], "sources": {"live": {"data": live},
                                      "live_diagnostics": {"data": None}}}


def test_campaigns_are_not_orders_or_zero_fill_cycles():
    snap = fixture_snapshot()
    window = resolve_window({"scope": "session"}, snap["sources"]["live"]["data"],
                            now=200)
    kpi = overview(snap, window)["kpis"]
    assert kpi["open_campaigns_submitted"] == 40
    assert kpi["paired_opened_cycles"] == 8
    assert kpi["paired_round_trips"] == 7
    assert kpi["terminal_zero_fill_campaigns"] == 32
    assert kpi["campaign_success_rate"] == "0.2"
    assert kpi["finalized_net_pnl_usd"] == "-0.001694"
    assert kpi["current_executable_net_usd"] is None
    assert kpi["losing_cycles"] == 7
    assert len(overview(snap, window)["recent_cycles"]) == 8
    assert all(row["has_confirmed_fill"] for row in overview(snap, window)["recent_cycles"])


def test_normal_exit_dashboard_uses_live_gross_rule_and_hides_stale_evidence():
    snap = fixture_snapshot()
    live = snap['sources']['live']['data']
    live.update(cycle_id='cycle-7', source_ts=195,
                executable_gross_usd='0.03', executable_net_usd='-0.7',
                normal_exit_min_gross_profit_usd='0.03', normal_exit_rule=True)
    window = resolve_window({'scope': 'session'}, live, now=200)
    diagnostic = overview(snap, window)['diagnostic']['exit']
    assert diagnostic['executable_gross_usd'] == '0.03'
    assert diagnostic['executable_net_usd'] == '-0.7'
    assert diagnostic['normal_exit_rule'] is True
    for stamp in (189, 201):
        live['source_ts'] = stamp
        assert 'exit' not in overview(snap, window)['diagnostic']


def test_trader_filters_exclude_zero_fill_before_pagination_and_keep_risk_fills():
    snap = fixture_snapshot()
    snap["journal"]["orders"].append({
        "order_ref": "single-leg", "cycle_id": "cycle-39", "venue": "entropy",
        "side": "buy", "reduce_only": 0, "submitted_ts": 151.0,
        "state": "TERMINAL", "filled_units": 2})
    window = resolve_window({"scope": "session"}, snap["sources"]["live"]["data"], now=200)
    rows = cycle_facts(snap, window)["created"]
    filled = trader_cycle_view(rows, "filled", "latest")
    assert len(filled) == 9
    assert filled[0]["cycle_id"] == "cycle-39"
    assert filled[0]["paired_opened"] is False
    assert len(trader_cycle_view(rows, "complete", "latest")) == 7
    assert len(trader_cycle_view(rows, "loss", "loss")) == 7
    assert len(trader_cycle_view(rows, "unfilled", "latest")) == 31


def test_cycle_loss_breakdown_reconciles_and_withholds_missing_evidence():
    snap = fixture_snapshot()
    snap["journal"]["cycle_economics"].append({
        "cycle_id": "cycle-0", "accounting_final": 1,
        "actual_fees_usd": "0.000300", "actual_funding_usd": "-0.000042",
        "finalized_pnl_usd": "-0.000242"})
    detail = cycle_detail(snap, "cycle-0")
    assert detail is not None
    assert detail["cycle"]["paired_round_trip"] is True
    bridge = detail["pnl_breakdown"]
    assert bridge is not None
    assert (Decimal(bridge["trade_cash_usd"]) - Decimal(bridge["fees_usd"])
            + Decimal(bridge["funding_usd"])) == Decimal(bridge["net_usd"])
    assert bridge["net_usd"] == "-0.000242"
    window = resolve_window({"scope": "session"}, snap["sources"]["live"]["data"], now=200)
    assert next(row for row in cycle_facts(snap, window)["created"]
                if row["cycle_id"] == "cycle-0")["largest_negative_component"] == "手续费"
    snap["journal"]["cycle_economics"][0]["actual_fees_usd"] = None
    assert cycle_detail(snap, "cycle-0")["pnl_breakdown"] is None


def test_cycle_detail_exposes_persisted_same_quantity_execution_evidence():
    snap = fixture_snapshot()
    quote = json.dumps({"quantity_step": "0.01", "entropy": {
        "asks": [["100", 10]], "bids": [["99", 10]]}})
    snap["journal"]["live_steps"].append({
        "cycle_id": "cycle-0", "step_key": "e-open", "order_ref": "e-0"})
    trace = {"cycle_id": "cycle-0", "step_key": "e-open", "purpose": "open_first",
             "quantity_step": "0.01", "decision_quote_json": quote,
             "submit_quote_json": quote, "fill_mark_quote_json": None,
             "fill_mark_time_source": "terminal_observed_local"}
    snap["journal"]["execution_economics_steps"].append(trace)
    detail = cycle_detail(snap, "cycle-0")
    attempt = next(a for g in detail["groups"] for a in g["attempts"]
                   if a["order_ref"] == "e-0")
    for key in ("quantity_step", "decision_quote_json", "submit_quote_json",
                "fill_mark_quote_json", "fill_mark_time_source"):
        assert attempt[key] == trace[key]
    assert attempt["actual_fee_usd"] is None
    assert attempt["filled_units"] == 10


def test_api_trader_views_filter_before_cursor_pagination(tmp_path: Path):
    paths = SourcePaths(*(tmp_path / part for part in (
        "journal.sqlite3", "live.json", "live-manifest.json", "diagnostics.json",
        "recorder.json", "recorder-manifest.json", "recorder-data", "reports", "cache")))
    with TestClient(create_app(paths, start_collector=False)) as client:
        client.app.state.v3_read_model.snapshot = fixture_snapshot()
        first = client.get("/api/v3/cycles?view=filled&limit=2")
        assert first.status_code == 200
        assert first.json()["data"]["total"] == 8
        assert all(row["has_confirmed_fill"] for row in first.json()["data"]["items"])
        cursor = first.json()["data"]["next_cursor"]
        client.app.state.v3_read_model.snapshot = {
            **client.app.state.v3_read_model.snapshot, "id": "next-poll"}
        second = client.get("/api/v3/cycles", params={"view": "filled", "limit": 2,
                                                        "cursor": cursor})
        assert second.status_code == 200
        assert first.json()["data"]["items"][0]["cycle_id"] != second.json()["data"]["items"][0]["cycle_id"]
        assert client.get("/api/v3/cycles?view=unfilled").json()["data"]["total"] == 32
        assert client.get("/api/v3/cycles?view=complete").json()["data"]["total"] == 7
        assert client.get("/api/v3/cycles?view=invalid").status_code == 422


def test_unknown_submission_and_accounting_are_not_zero():
    snap = fixture_snapshot()
    order = next(row for row in snap["journal"]["orders"]
                 if row["cycle_id"] == "cycle-8")
    order["state"] = "UNKNOWN"
    snap["journal"]["cycles"][0]["accounting_final"] = 0
    window = resolve_window({"scope": "session"}, snap["sources"]["live"]["data"],
                            now=200)
    facts = cycle_facts(snap, window)
    assert facts["kpis"]["terminal_zero_fill_campaigns"] == 31
    assert facts["kpis"]["pending_accounting_cycles"] >= 1
    assert facts["kpis"]["finalized_net_pnl_usd"] == "-0.001452"


def test_second_submitted_open_attempt_still_one_campaign():
    snap = fixture_snapshot()
    snap["journal"]["orders"].append({
        "order_ref": "retry-8", "cycle_id": "cycle-8", "venue": "entropy",
        "side": "buy", "reduce_only": 0, "submitted_ts": 120,
        "state": "TERMINAL", "filled_units": 0})
    window = resolve_window({"scope": "session"}, snap["sources"]["live"]["data"],
                            now=200)
    facts = cycle_facts(snap, window)
    assert facts["kpis"]["open_campaigns_submitted"] == 40
    assert facts["kpis"]["terminal_zero_fill_campaigns"] == 32
    assert next(row for row in facts["created"] if row["cycle_id"] == "cycle-8")[
        "opening_attempts_submitted"] == 2


def test_taipei_today_boundary_is_not_device_timezone():
    now = datetime.fromisoformat("2026-09-23T01:00:00+08:00").timestamp()
    window = resolve_window({"scope": "today", "timezone": "Asia/Taipei"},
                            None, now=now)
    expected = datetime.fromisoformat("2026-09-23T00:00:00+08:00").timestamp()
    assert window.from_ts == expected
    assert window.contains(expected)
    assert not window.contains(now)


@pytest.mark.parametrize('scope', ['7d', '1h', '6h'])
def test_seven_day_window_and_retired_short_ranges(scope):
    now = 2_000_000
    window = resolve_window({'scope': scope}, None, now=now)
    assert window.scope == '7d'
    assert window.from_ts == now - 7 * 86400
    assert window.to_ts == now
    assert window.contains(window.from_ts)
    assert not window.contains(window.from_ts - 1)
    assert not window.contains(now)


def test_all_window_has_no_time_bounds_and_keeps_invalid_dates_unknown():
    window = resolve_window({'scope': 'all'}, None, now=2_000_000)
    assert window.from_ts is None and window.to_ts is None
    assert window.public()['from_ts'] is None and window.public()['to_ts'] is None
    assert all(window.contains(stamp) for stamp in (-1, 1, 2_000_001))
    assert not window.contains(None)
    assert not window.contains('NaN')


def test_all_market_window_reads_the_entire_read_only_cache(tmp_path: Path):
    tail = MarketTail(tmp_path, tmp_path / 'cache')
    stamps = (1, 2_000_000, 90_000_000)
    with tail._cache_connection() as db:
        for seq, stamp in enumerate(stamps):
            row = {'capture_wall_ts': stamp, 'pair_id': 'p', 'valid': False}
            db.execute('INSERT INTO market_samples VALUES (?, ?, ?)',
                       (str(seq), stamp, json.dumps(row)))
    before = hashlib.sha256((tail.cache_dir / 'readmodel.sqlite3').read_bytes()).hexdigest()
    window = resolve_window({'scope': 'all'}, None, now=2_000_000)
    snap = fixture_snapshot()
    snap['sources']['market'] = {'quality': 'available', 'backfill_complete': True}
    output = market_points(snap, window, units=1, cached_rows=tail.iter_window(None, None, 'p'))
    assert output['coverage']['loaded_rows'] == 3
    assert [p['ts'] for p in output['points']] == list(stamps)
    assert output['coverage']['requested_from_ts'] is None
    assert output['coverage']['requested_to_ts'] is None
    assert hashlib.sha256((tail.cache_dir / 'readmodel.sqlite3').read_bytes()).hexdigest() == before


def test_fixed_q_requotes_same_frame_and_declares_depth_gap():
    row = {"capture_wall_ts": 120, "valid": True, "reference_bps": "0",
           "entropy": {"asks": [["100", 10]], "bids": [["99", 10]]},
           "lighter": {"asks": [["100", 10]], "bids": [["99", 10]]}}
    quote = requote_market(row, 10, "3")
    assert abs(Decimal(quote["buy_basis_bps"]) -
               (Decimal(100) / Decimal(99) - 1) * 10000) <= Decimal("0.000001")
    assert quote["long_edge_bps"] is not None
    assert requote_market(row, 11)["quality_reason"] == "insufficient_depth_for_fixed_q"


def test_compact_depth_marks_cache_gap_without_blame_on_exchange():
    row = {"capture_wall_ts": 120, "valid": True,
           "entropy": {"asks": [["100", 1]] * 40, "bids": [["99", 1]] * 40},
           "lighter": {"asks": [["100", 1]] * 40, "bids": [["99", 1]] * 40}}
    compact = compact_market(row)
    assert len(compact["lighter"]["bids"]) == 32
    assert requote_market(compact, 40)["quality_reason"] == "cache_depth_truncated_for_q"


def test_market_tail_handles_half_line_rotation_and_duplicate_seq(tmp_path: Path):
    line = {"kind": "v3_market_snapshot", "schema_version": 2,
            "pair_id": "p", "session_id": "s", "sample_seq": 1,
            "capture_wall_ts": 100, "valid": False}
    active = tmp_path / "record-observer.edge.jsonl"
    active.write_bytes(json.dumps(line).encode())
    tail = MarketTail(tmp_path, tmp_path)
    tail.poll()
    assert tail.points == []
    with active.open("ab") as output:
        output.write(b"\n")
    tail.poll()
    assert len(tail.points) == 1
    rotated = tmp_path / "record-observer.edge.20260923T000000Z-1.jsonl"
    active.rename(rotated)
    active.write_text(json.dumps(line) + "\n")
    tail.poll()
    assert len(tail.points) == 1


def test_market_cold_tail_eventually_backfills_prefix(tmp_path: Path):
    active = tmp_path / "record-observer.edge.jsonl"
    rows = [{"kind": "v3_market_snapshot", "schema_version": 2,
             "pair_id": "p", "session_id": "s", "sample_seq": n,
             "capture_wall_ts": n, "valid": False} for n in range(1, 9)]
    active.write_text("".join(json.dumps(row) + "\n" for row in rows))
    tail = MarketTail(tmp_path, tmp_path / "cache")
    for _ in range(20):
        tail.poll(max_bytes=200)
        if tail.backfill_complete:
            break
    assert {row["sample_seq"] for row in tail.points} == set(range(1, 9))
    assert tail.backfill_complete


def test_market_history_reads_cache_beyond_memory_window(tmp_path: Path):
    tail = MarketTail(tmp_path, tmp_path / "cache")
    db_path = tail.cache_dir / "readmodel.sqlite3"
    with tail._cache_connection() as db:
        for seq, stamp in enumerate((100.0, 200.0, 300.0), start=1):
            row = {"kind": "v3_market_snapshot", "schema_version": 2,
                   "pair_id": "p", "session_id": "s", "sample_seq": seq,
                   "capture_wall_ts": stamp, "valid": True,
                   "reference_bps": "0",
                   "entropy": {"asks": [["100", 10]], "bids": [["99", 10]]},
                   "lighter": {"asks": [["100", 10]], "bids": [["99", 10]]}}
            db.execute("INSERT INTO market_samples VALUES (?, ?, ?)",
                       (f"v2:p:s:{seq}", stamp, json.dumps(row)))
    before = hashlib.sha256(db_path.read_bytes()).hexdigest()
    snap = fixture_snapshot()
    snap["sources"]["market"] = {"quality": "available", "backfill_complete": True}
    snap["sources"]["live_manifest"] = {"data": {"settings": {"entry_band_bps": "3"}}}
    snap["market"] = [{"capture_wall_ts": 300, "valid": False}]
    window = resolve_window({"scope": "custom", "from_ts": 99, "to_ts": 301},
                            snap["sources"]["live"]["data"], now=301)
    output = market_points(snap, window, units=10,
                           cached_rows=tail.iter_window(window.from_ts, window.to_ts, "p"))
    assert output["coverage"]["loaded_rows"] == 3
    assert [p["ts"] for p in output["points"]] == [100, 200, 300]
    assert output["coverage"]["quality"] == "partial"
    assert output["coverage"]["reason"] == "recorder_samples_have_gaps_or_invalid_frames"
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == before


def test_market_cache_rechecks_backfill_artifacts_in_capture_order(tmp_path: Path):
    tail = MarketTail(tmp_path, tmp_path / "cache")
    with tail._cache_connection() as db:
        for seq in range(1, 5):
            row = {"schema_version": 2, "pair_id": "p", "session_id": "s",
                   "sample_seq": seq, "capture_wall_ts": 100 + seq,
                   "valid": True, "continuity_break": seq in (2, 3, 4),
                   "coverage_reason": "capture_clock_rollback" if seq == 2
                   else "sequence_gap_or_reordered_backfill" if seq == 3 else None}
            db.execute("INSERT INTO market_samples VALUES (?, ?, ?)",
                       (f"v2:p:s:{seq}", row["capture_wall_ts"], json.dumps(row)))
    rows = list(tail.iter_window(100, 105, "p"))
    assert [row["continuity_break"] for row in rows] == [False, False, False, True]


def test_dashboard_cache_reader_does_not_block_collector_writer(tmp_path: Path):
    tail = MarketTail(tmp_path, tmp_path / "cache")
    with tail._cache_connection() as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        db.execute("INSERT INTO market_samples VALUES ('one', 1, '{}')")
    reader = sqlite3.connect(tail.cache_dir / "readmodel.sqlite3")
    try:
        reader.execute("BEGIN")
        assert reader.execute("SELECT COUNT(*) FROM market_samples").fetchone() == (1,)
        with tail._cache_connection() as writer:
            writer.execute("INSERT INTO market_samples VALUES ('two', 2, '{}')")
        assert reader.execute("SELECT COUNT(*) FROM market_samples").fetchone() == (1,)
    finally:
        reader.close()


def test_market_downsample_keeps_extreme_and_gap():
    snap = fixture_snapshot()
    template = {"schema_version": 2, "pair_id": "p", "session_id": "s",
                "valid": True, "reference_bps": "0",
                "entropy": {"asks": [["100", 10]], "bids": [["99", 10]]},
                "lighter": {"asks": [["100", 10]], "bids": [["99", 10]]}}
    snap["sources"]["market"] = {"quality": "available", "backfill_complete": True}
    snap["sources"]["live_manifest"] = {"data": {"settings": {"entry_band_bps": "3"}}}
    snap["market"] = [{**template, "capture_wall_ts": 100 + n * .01,
                        "sample_seq": n,
                        "entropy": {**template["entropy"],
                                    "asks": [["110" if n == 1000 else "100", 10]]},
                        "valid": n != 1500} for n in range(3000)]
    window = resolve_window({"scope": "session"}, snap["sources"]["live"]["data"],
                            now=200)
    output = market_points(snap, window, units=10, max_points=600)
    assert len(output["points"]) <= 600
    assert any(point["sample_seq"] == 1000 for point in output["points"])
    assert any(not point["valid"] or point["continuity_break"]
               for point in output["points"])


def test_observed_episode_breaks_on_invalid_and_is_not_live_decision():
    snap = fixture_snapshot()
    template = {"schema_version": 2, "pair_id": "p", "session_id": "s",
                "reference_bps": "105", "valid": True,
                "entropy": {"asks": [["100", 10]], "bids": [["99", 10]]},
                "lighter": {"asks": [["100", 10]], "bids": [["99", 10]]}}
    snap["sources"]["market"] = {"quality": "available", "backfill_complete": True}
    snap["sources"]["live_manifest"] = {"data": {"settings": {"entry_band_bps": "3"}}}
    snap["market"] = [{**template, "capture_wall_ts": 110 + n,
                       "sample_seq": n, "valid": n != 2} for n in range(4)]
    window = resolve_window({"scope": "session"}, snap["sources"]["live"]["data"],
                            now=200)
    episodes = market_points(snap, window, units=10)["episodes"]
    assert len(episodes) == 2
    assert all(row["live_decision_link"] == "unproven" for row in episodes)


def test_matched_execution_attribution_keeps_fee_out_of_stage_loss():
    quote = {"entropy": {"asks": [["100", 10]], "bids": [["99", 10]]},
             "hedge": {"asks": [["100", 10]], "bids": [["99", 10]]}}
    first = {"step_key": "e", "venue": "entropy", "side": "buy", "result_units": 10,
             "actual_fill_vwap": "100.1", "decision_quote_json": json.dumps(quote),
             "submit_quote_json": json.dumps(quote), "fill_mark_quote_json": json.dumps(quote),
             "fill_mark_time_source": "runtime_consumed_local", "quantity_step": "0.01"}
    hedge = {"step_key": "h", "parent_step_key": "e", "venue": "hedge",
             "side": "sell", "result_units": 10, "actual_fill_vwap": "98.9",
             "quantity_step": "0.01"}
    result = conservative_attribution([first, hedge])
    assert result["measurement_quality"] == "complete"
    assert result["matched_units"] == 10
    assert Decimal(result["entropy_execution_loss_usd"]) == Decimal("0.01")
    assert Decimal(result["hedge_execution_loss_usd"]) == Decimal("0.01")
    assert Decimal(result["total_execution_loss_usd"]) == Decimal("0.02")


def test_missing_production_journal_is_not_created(tmp_path: Path):
    missing = tmp_path / "missing.sqlite3"
    source = journal_source(missing)
    assert source["quality"] == "unavailable"
    assert not missing.exists()


def test_ro_connection_does_not_migrate_database(tmp_path: Path):
    path = tmp_path / "journal.sqlite3"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT)")
    db.execute("INSERT INTO metadata VALUES ('economics_schema_version', '99')")
    db.commit()
    db.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    source = journal_source(path)
    assert source["quality"] == "unsupported_schema"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    with sqlite3.connect(path) as check:
        assert check.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [
            ("metadata",)]


def test_api_is_read_only_and_missing_sources_stay_missing(tmp_path: Path):
    paths = SourcePaths(*(tmp_path / part for part in (
        "journal.sqlite3", "live.json", "live-manifest.json", "diagnostics.json",
        "recorder.json", "recorder-manifest.json", "recorder-data", "reports", "cache")))
    with TestClient(create_app(paths, start_collector=False)) as client:
        health = client.get("/api/v3/health")
        assert health.status_code == 200
        assert health.json()["data"]["source_health"]["journal"]["quality"] == "unavailable"
        assert client.post("/api/v3/overview").status_code == 405
        assert client.get("/api/v3/cycles/../../etc/passwd").status_code in (400, 404)
        assert client.get("/api/v3/market?sql=SELECT+*").status_code == 400
        research = client.get("/api/v3/research")
        assert research.status_code == 200
        assert research.json()["data"]["reason"] == "INSUFFICIENT_DATA"
        assert research.json()["meta"]["window"] is None
        assert client.get("/api/v3/research?path=secrets").status_code == 400
    assert not paths.journal.exists()
