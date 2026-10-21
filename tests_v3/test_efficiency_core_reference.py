"""Offline boundaries for audit settings and current-snapshot reference recovery."""
from decimal import Decimal as D
import socket

import pytest

from entropy_arb.v3.core import Settings
from entropy_arb.v3.reference import FastReference


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('network is forbidden in these tests')
    monkeypatch.setattr(socket.socket, 'connect', denied)
    monkeypatch.setattr(socket.socket, 'connect_ex', denied)
    monkeypatch.setattr(socket, 'getaddrinfo', denied)


def anchored_reference():
    model = FastReference('pair', Settings())
    for second in range(31):
        model.observe(second, D('-30'))
    assert model.view(30).usable
    return model


def test_current_snapshot_restores_immediately_without_sampling_or_reanchoring():
    model = anchored_reference()
    before = model.view(30)
    checkpoint = model.checkpoint()
    model.observe(30.1, D('-30'), valid=False)
    restored = model.restore_market_valid(30.2)
    assert restored.usable
    assert restored.center_bps == before.center_bps
    assert restored.live_samples == before.live_samples
    assert restored.live_span_seconds == before.live_span_seconds
    assert model.checkpoint() == checkpoint
    # A later scheduled sample in this same second remains deduplicated.
    assert model.observe(30.3, D('500')).live_samples == before.live_samples
    assert model.checkpoint() == checkpoint


def test_repeated_restores_do_not_renew_last_sample_age():
    model = anchored_reference()
    for now in (31, 35, 40):
        assert model.restore_market_valid(now).usable
    reset = model.restore_market_valid(40.01)
    assert not reset.usable
    assert reset.center_bps is None
    assert reset.live_samples == 0
    assert model.checkpoint() == {}
    assert model.observe(40.1, D('-10')).live_samples == 1


def test_restore_cannot_make_cold_samples_usable_before_thirty_seconds():
    model = FastReference('pair', Settings())
    for second in range(20):
        model.observe(second, D('-30'))
    model.observe(19.1, D('-30'), valid=False)
    assert not model.restore_market_valid(19.2).usable
    assert model.view(19.2).live_samples == 20
    for second in range(20, 30):
        assert not model.observe(second, D('-30')).usable
    assert model.observe(30, D('-30')).usable


def test_restore_cannot_shorten_fresh_checkpoint_validation():
    model = FastReference('pair', Settings(),
                          {'version': 1, 'pair_id': 'pair', 'ts': 99,
                           'center_bps': '-300'}, started_at=100)
    for second in range(100, 105):
        assert not model.observe(second, D('-30')).usable
    model.observe(104.1, D('-30'), valid=False)
    assert not model.restore_market_valid(104.2).usable
    assert model.view(104.2).live_samples == 5
    restored = model.observe(105, D('-30'))
    assert restored.usable
    assert restored.center_bps == D('-30')


def test_empty_or_long_gap_reference_cannot_be_revived_without_samples():
    model = FastReference('pair', Settings())
    assert not model.restore_market_valid(1).usable
    assert model.view(1).live_samples == 0
    model = anchored_reference()
    model.observe(31, D('-30'), valid=False)
    assert not model.restore_market_valid(41).usable
    assert not model.restore_market_valid(41.1).usable
    assert model.view(41.1).live_samples == 0


@pytest.mark.parametrize('now', [float('nan'), float('inf'), 29])
def test_restore_rejects_invalid_or_out_of_order_clock(now):
    model = anchored_reference()
    with pytest.raises(ValueError, match='invalid clock'):
        model.restore_market_valid(now)


def test_restore_rejects_clock_before_process_start():
    model = FastReference('pair', Settings(), started_at=100)
    with pytest.raises(ValueError, match='invalid clock'):
        model.restore_market_valid(99)


def test_audit_interval_accepts_old_flat_settings_without_changing_them():
    cfg = Settings.from_mapping({'flat_position_probe_sec': 2,
                                 'flat_position_lease_sec': 3})
    assert cfg.position_audit_seconds == 60.0
    assert cfg.flat_position_probe_sec == 2
    assert cfg.flat_position_lease_sec == 3
    assert Settings.from_mapping({'position_audit_seconds': 7}).position_audit_seconds == 7


@pytest.mark.parametrize('value', [0, -1, float('nan'), float('inf'), True])
def test_audit_interval_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        Settings(position_audit_seconds=value)
    with pytest.raises(ValueError):
        Settings.from_mapping({'position_audit_seconds': value})
