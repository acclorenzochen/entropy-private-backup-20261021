from dataclasses import replace
from decimal import Decimal as D
import pytest
from entropy_arb.v3.core import (Book,Direction,Instrument,Market,Settings,Side,
                                dec,exit_decision,plan_open,price_limit)
from entropy_arb.v3.demo import fixture_market


def instrument():
    return Instrument('P',D('.01'),D('.01'),D('.01'),D('.9'),D('0'))


def test_units_are_exact_not_float_floor():
    i=Instrument('P',D('.0001'),D('.1'),D('.01'),D('.9'),D('0'))
    assert i.exact_units('0.0085')==85
    assert i.quantity(85)==D('.0085')
    with pytest.raises(ValueError): i.exact_units('0.00851')


@pytest.mark.parametrize('side',[Side.BUY,Side.SELL])
def test_tick_rounding_never_crosses_hard_cap(side):
    top=D('1762.5'); hard=D('10')
    for target in ('0','.1','.5','3','5','8','15'):
        p=price_limit(top,top,side,D(target),hard,D('.1'))
        assert p%D('.1')==0
        if side is Side.BUY: assert top <= p <= top*(1+hard/D(10000))
        else: assert top*(1-hard/D(10000)) <= p <= top


def test_depth_outside_hard_cap_rejected():
    with pytest.raises(ValueError):
        price_limit(D(100),D(101),Side.BUY,D(3),D(10),D('.01'))


def test_zero_extra_room_can_still_be_marketable():
    assert price_limit(D(100),D(100),Side.BUY,D(0),D(10),D('.01'))==D(100)


def test_entry_fees_remain_diagnostic_without_scenario_calculation():
    m=fixture_market(); i=instrument(); cfg=Settings()
    p=plan_open(m,i,cfg,D(0),Direction.LONG_ENTROPY,10)
    assert p and p.entry_quoted_fees_usd==D('9.96')*D('.9')/D(10000)
    p_without_fixed=plan_open(m,i,replace(cfg,execution_allowance_bps=D(0)),D(0),Direction.LONG_ENTROPY,10)
    assert p.reference_scenario_usd is p.scenario_floor_usd is None
    assert p_without_fixed == p
    assert p.distance_bps==D(40)


def test_depth_average_not_best_price_drives_signal():
    m=Market(Book(((D('99.58'),100),),((D('99.60'),5),(D('99.62'),5)),40,'e'),
             fixture_market().hedge)
    p=plan_open(m,instrument(),Settings(),D(0),Direction.LONG_ENTROPY,10)
    assert p and p.distance_bps==D(39)


def test_both_directions_use_same_price_coordinate():
    i=instrument(); cfg=Settings()
    long=plan_open(fixture_market(),i,cfg,D(0),Direction.LONG_ENTROPY,10)
    short=plan_open(fixture_market('100.41','100.42'),i,cfg,D(0),Direction.SHORT_ENTROPY,10)
    assert long and short
    assert long.executable_basis_bps==D('-40')
    assert short.executable_basis_bps==(D('100.41')/D('100.01')-1)*D(10000)


def test_notional_cap_never_auto_increased_for_venue_minimum():
    assert plan_open(fixture_market(),instrument(),Settings(),D(0),Direction.LONG_ENTROPY,20) is None


def test_scenario_allowance_does_not_block_executable_entry():
    p=plan_open(fixture_market(),instrument(),replace(Settings(),execution_allowance_bps=D(100)),D(0),Direction.LONG_ENTROPY,10)
    assert p and p.reference_scenario_usd is p.scenario_floor_usd is None


def test_negative_mtm_alone_does_not_trigger_exit():
    result=exit_decision(fixture_market(),instrument(),Settings(),Direction.LONG_ENTROPY,10,D('-40'),D('-1'),
                         accrued_price_cash_usd=D('-1'),
                         profit_entry_notional_usd=D('9.98'))
    assert not result.allowed


def test_reversion_small_loss_does_not_bypass_gross_profit_floor():
    m=fixture_market('99.8','99.81');i=instrument();cfg=Settings()
    ec,_=m.entropy.quote(Side.SELL,10,i.quantity_step)
    hc,_=m.hedge.quote(Side.BUY,10,i.quantity_step)
    fees=(ec*i.entropy_fee_bps+hc*i.hedge_fee_bps)/D(10000)
    accrued=-D('.005')-(ec-hc)+fees
    result=exit_decision(m,i,cfg,Direction.LONG_ENTROPY,10,D('-40'),accrued,
                         accrued_price_cash_usd=accrued,
                         profit_entry_notional_usd=D('9.98'))
    assert not result.allowed and result.executable_net_usd==D('-.005')
    assert result.reversion_bps >= cfg.exit_reversion_bps


@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('profit,allowed', [('0.002494', False), ('0.002495', True)])
@pytest.mark.parametrize('cash_net,fee_bps,buffer', [('-100', '500', '1000'), ('100', '0', '0')])
def test_normal_exit_gross_boundary_ignores_fees_funding_and_buffer(
        direction, profit, allowed, cash_net, fee_bps, buffer):
    m=fixture_market(); i=replace(instrument(),entropy_fee_bps=D(fee_bps))
    cfg=replace(Settings(),execution_allowance_bps=D(buffer))
    es=direction.entropy_side.opposite
    ec,_=m.entropy.quote(es,10,i.quantity_step)
    hc,_=m.hedge.quote(es.opposite,10,i.quantity_step)
    exit_cash=ec-hc if es is Side.SELL else -ec+hc
    opening_e_cash,_=m.entropy.quote(direction.entropy_side,10,i.quantity_step)
    opening_h_cash,_=m.hedge.quote(direction.entropy_side.opposite,10,i.quantity_step)
    opening_matched_notional=(opening_e_cash+opening_h_cash)/2
    result=exit_decision(m,i,cfg,direction,10,None,D(cash_net),
                         accrued_price_cash_usd=D(profit)-exit_cash,
                         profit_entry_notional_usd=opening_matched_notional)
    assert result.allowed is allowed
    assert result.executable_gross_usd == D(profit)
    assert result.profit_entry_notional_usd == D('9.98')
    assert result.profit_bps == D(profit)/opening_matched_notional*D(10000)
    assert result.reason == ('gross_price_profit_exit' if allowed else 'hold')


@pytest.mark.parametrize('notional', [None, D('0'), D('-1'), D('NaN'), D('Infinity'), 'invalid'])
def test_normal_exit_requires_actual_positive_finite_entry_notional(notional):
    result=exit_decision(fixture_market(),instrument(),Settings(),Direction.LONG_ENTROPY,
                         10,None,D('100'),accrued_price_cash_usd=D('100'),
                         profit_entry_notional_usd=notional)
    assert not result.allowed
    assert result.reason == 'hold_missing_entry_notional'
    assert result.profit_bps is None


def test_legacy_usd_setting_does_not_replace_default_basis_profit_threshold():
    cfg=Settings.from_mapping({'exit_min_gross_profit_usd': '100'})
    assert cfg.exit_min_gross_profit_bps == D('2.5')
    result=exit_decision(fixture_market(),instrument(),cfg,Direction.LONG_ENTROPY,
                         10,None,D('0'),accrued_price_cash_usd=D('.05'),
                         profit_entry_notional_usd=D('9.98'))
    assert result.allowed


@pytest.mark.parametrize('field', ['exit_min_gross_profit_bps', 'exit_min_gross_profit_usd'])
@pytest.mark.parametrize('value', ['0', '-0.03', 'NaN'])
def test_gross_profit_threshold_requires_positive_finite_value(field,value):
    with pytest.raises(ValueError):
        Settings.from_mapping({field: value})


def test_good_market_freshness_and_future_timestamp():
    m=fixture_market(now=40)
    assert m.valid_reason(40.5,Settings()) is None
    assert m.valid_reason(42,Settings())=='entropy_book_stale_or_future'
    assert m.valid_reason(39,Settings())=='entropy_book_stale_or_future'


@pytest.mark.parametrize('raw',[{'typo':1},{'live_armed':'false'},
    {'funding_max_adverse_cost_bps':'4'},{'cold_samples':2.2},
    {'entry_band_bps':'NaN'},{'max_notional_usd':True}])
def test_strict_config(raw):
    with pytest.raises((ValueError,ArithmeticError)): Settings.from_mapping(raw)
