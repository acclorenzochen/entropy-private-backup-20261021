from entropy_arb.v3.core import Settings
from tools.v3_opportunities import candidate_calibration


def cycle(i, **changes):
    return {'cycle_id': str(i), 'signal_ts': float(i),
            'first_fill_observed_ts': i + .1,
            'authoritative_reconcile_complete_ts': i + .5,
            'direction': 'long_entropy', 'planned_notional_usd': '10',
            'entry_dislocation_bps': '4', 'accounting_final': 1,
            'actual_entry_notional_usd': '10', 'actual_fees_usd': '.01',
            'actual_funding_usd': '0', 'finalized_pnl_usd': '0', **changes}


def test_training_never_consumes_holdout_completion_and_never_selects_candidate():
    rows = [cycle(i) for i in range(10)]
    rows[6]['authoritative_reconcile_complete_ts'] = 8.
    result = candidate_calibration([], rows, Settings())
    candidate = next(r for r in result['thresholds'] if r['entry_band_bps'] == '4')
    assert result['split']['holdout_start_ts'] == 7.
    assert candidate['train']['admitted_cycles'] == 5
    assert candidate['train']['complete_cost_cycles'] == 4
    assert candidate['train']['missing']['not_finalized_before_next_split'] == 1
    assert candidate['holdout']['complete_cost_cycles'] == 3
    assert candidate['train']['mean_net_usd'] == '0'
    assert result['recommendation'] is None
    assert result['convergence']['expected_net_usd'] is None


def test_missing_fees_funding_and_accounting_never_become_zero_pnl():
    rows = [cycle(i, actual_fees_usd=None, actual_funding_usd=None,
                  accounting_final=0) for i in range(10)]
    result = candidate_calibration([], rows, Settings())
    held = result['strata']['direction']['long_entropy']['holdout']
    assert held['status'] == 'UNESTIMABLE'
    assert held['mean_net_usd'] is held['fees_usd'] is held['signed_funding_usd'] is None
    assert held['missing']['actual_funding_usd'] == 3
    assert result['strata']['predecision_volatility_bps']['unknown']


def test_depth_is_directional_and_unknown_attempt_is_not_a_terminal_latency():
    rows = [cycle(i) for i in range(10)]
    attempts = [{'cycle_id': str(i), 'purpose': 'open_first', 'side': 'buy',
                 'submit_ts': i + .01, 'terminal_result_ts': i + .11,
                 'order_state': 'UNKNOWN', 'room_bps': '3', 'requested_units': 10,
                 'decision_quote_json': {'entropy': {'asks': [['100', 20]],
                                                      'bids': [['99', 1]]},
                                         'hedge': {'bids': [['100', 30]],
                                                   'asks': [['101', 1]]}}}
                for i in range(10)]
    result = candidate_calibration(attempts, rows, Settings())
    assert result['strata']['decision_depth_multiple']['1..10']['holdout']['admitted_cycles'] == 3
    assert list(result['strata']['terminal_receipt_seconds']) == ['unknown']
    assert result['strata']['first_attempt_room_bps']['3']['train']['admitted_cycles'] == 5
    assert result['price_room']['candidate_effect'] is None
