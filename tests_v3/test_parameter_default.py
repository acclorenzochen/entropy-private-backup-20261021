"""The selected config and absent-field default agree at five bp."""
from pathlib import Path
from decimal import Decimal

from main_v3 import load_settings
from entropy_arb.v3.core import Settings


def test_default_and_candidate_entry_are_five_bp():
    assert Settings().entry_band_bps == Decimal("5")
    assert Settings.from_mapping({}).entry_band_bps == Decimal("5")
    settings = load_settings(Path(__file__).resolve().parents[1] / "config.v3.edge-next.json")
    assert settings.entry_band_bps == Decimal("5")
    assert settings.exit_min_gross_profit_bps == Decimal("2.5")
    assert settings.max_notional_usd == Decimal("200")
    for policy in (settings.first_leg_execution_policy, settings.hedge_execution_policy):
        assert policy("close") == ((Decimal("12"),) * 3, Decimal("12"))
