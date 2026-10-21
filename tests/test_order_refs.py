import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from entropy_arb.engine import Engine  # noqa: E402
from entropy_arb.execution_state import ExecutionState  # noqa: E402
from entropy_arb.venue_lighter import (  # noqa: E402
    MAX_CLIENT_ORDER_INDEX,
    LighterVenue,
)


LIGHTER_MAX_CLIENT_ORDER_INDEX = (1 << 48) - 1


def test_deterministic_order_ref_fits_lighter_client_order_index():
    state = ExecutionState(
        cycle_id="1788040519483-1",
        action="open",
        direction="short_entropy",
        planned_qty=0.0402,
        entropy_is_buy=False,
        hedge_is_buy=True,
        reduce_only=False,
    )

    for leg, attempt in (("first", 0), ("hedge", 0), ("hedge", 133)):
        ref = int(Engine._deterministic_order_ref(state, leg, attempt))
        assert 0 < ref <= LIGHTER_MAX_CLIENT_ORDER_INDEX


def test_lighter_ignores_out_of_range_client_order_index():
    venue = object.__new__(LighterVenue)
    venue.name = "LIGHTER"
    venue._coi = 100

    assert venue._next_coi(str(MAX_CLIENT_ORDER_INDEX + 1)) == 101
