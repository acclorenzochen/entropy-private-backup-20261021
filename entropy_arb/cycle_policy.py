"""Pure realized-cost exit policy shared by live execution and replay."""
import math



def evaluate_cycle_exit(cfg, *, net_pnl_usd, notional_usd, held_sec,
                        adverse_drift_bps=0.0, adverse_persist_sec=0.0,
                        regime_warning=False, regime_shift=False):
    values = (net_pnl_usd, notional_usd, held_sec,
              adverse_drift_bps, adverse_persist_sec)
    if not all(math.isfinite(float(x)) for x in values) or notional_usd <= 0:
        return dict(allowed=False, floor_usd=0.0, reason="invalid_exit_inputs",
                    risk_exit=False, risk_exit_type=None, forced=False,
                    exit_class=None, hold_state=None)
    # One policy source: an initial profit target and the configured small-loss
    # release floor after FAST. No second hard-coded bps ladder overrides it.
    floor = float(cfg.normal_min_profit_usd)
    risk = None
    if cfg.hard_timeout_enabled and held_sec >= cfg.hard_timeout_sec:
        risk = "hard_timeout"
    elif (cfg.pnl_loss_stop_enabled and cfg.max_cycle_loss_bps is not None
          and net_pnl_usd <= -notional_usd * cfg.max_cycle_loss_bps / 1e4):
        risk = "pnl_loss_stop"
    elif ((cfg.adverse_drift_exit_enabled or cfg.adverse_drift_enabled)
          and held_sec >= cfg.adverse_drift_min_age_sec
          and net_pnl_usd <= cfg.adverse_drift_min_loss_usd
          and adverse_drift_bps >= cfg.adverse_drift_bps
          and adverse_persist_sec >= cfg.adverse_drift_persist_sec):
        risk = "adverse_drift"
    elif (cfg.regime_stop_enabled and regime_shift and held_sec >= cfg.regime_stop_min_age_sec
          and net_pnl_usd <= cfg.regime_stop_min_loss_usd
          and (regime_warning or not cfg.regime_stop_require_midline_warning)):
        risk = "regime_stop"
    allowed = net_pnl_usd + 1e-10 >= floor
    reason = "normal_profit" if allowed else "normal_hold"
    if held_sec >= cfg.fast_exit_after_sec:
        floor = min(floor, float(cfg.fast_exit_min_profit_usd))
        if not allowed and net_pnl_usd + 1e-10 >= floor:
            allowed, reason = True, "fast_exit"
    hold = None
    if risk:
        allowed, reason = True, risk + "_release"
        hold = reason
    elif not allowed:
        if held_sec >= cfg.long_hold_after_sec:
            hold = reason = "long_hold"
        elif net_pnl_usd + 1e-10 < cfg.fast_exit_min_profit_usd:
            hold = reason = "loss_recovery_hold"
    return dict(allowed=allowed, floor_usd=floor, reason=reason,
                risk_exit=bool(risk), risk_exit_type=risk, forced=bool(risk),
                exit_class="risk" if risk else ("economic" if allowed else None),
                hold_state=hold)


def exit_net_pnl(*, cashflow_usd, entropy_qty, hedge_qty, entropy_bid,
                 entropy_ask, hedge_bid, hedge_ask, entropy_fee_bps,
                 hedge_fee_bps, slippage_bps=0.0):
    """Cash plus directional liquidation proceeds, including exit fees/slip.

    This top-of-book mark is a stress estimate, not an executable guarantee.
    Historical slippage already in cashflow is never deducted again.
    """
    value = cashflow_usd
    for qty, bid, ask, fee in (
            (entropy_qty, entropy_bid, entropy_ask, entropy_fee_bps),
            (hedge_qty, hedge_bid, hedge_ask, hedge_fee_bps)):
        px = bid * (1 - slippage_bps / 1e4) if qty >= 0 else ask * (1 + slippage_bps / 1e4)
        value += qty * px - abs(qty * px) * fee / 1e4
    return value
