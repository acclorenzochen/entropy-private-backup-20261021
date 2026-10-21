import importlib
import sys

import pytest


def test_main_v3_does_not_import_legacy_engine(monkeypatch):
    # Restore preexisting modules after the isolation check: otherwise running
    # V3 tests first splits legacy Engine classes from its patched replay clock.
    monkeypatch.delitem(sys.modules, "main_v3", raising=False)
    monkeypatch.delitem(sys.modules, "entropy_arb.engine", raising=False)
    module = importlib.import_module("main_v3")
    assert module.STRATEGY_VERSION.startswith("entropy-v3")
    assert "entropy_arb.engine" not in sys.modules


def test_default_mode_is_record_only():
    from main_v3 import parser

    args = parser().parse_args(["--symbol", "SNDK", "--hedge", "lighter"])
    assert not args.live
    assert not args.record_only


def test_live_and_record_only_are_mutually_exclusive():
    from main_v3 import parser

    with pytest.raises(SystemExit):
        parser().parse_args([
            "--symbol", "SNDK", "--hedge", "lighter", "--live", "--record-only"])
