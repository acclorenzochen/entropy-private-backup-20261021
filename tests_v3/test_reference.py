from decimal import Decimal as D
import pytest
from entropy_arb.v3.core import Settings
from entropy_arb.v3.reference import FastReference, load_checkpoint, save_checkpoint


def test_cold_start_directly_anchors_current_range():
    model=FastReference('pair',Settings())
    for t in range(30):
        assert not model.observe(t,D('-30')).usable
    view=model.observe(30,D('-30'))
    assert view.usable and view.center_bps==D('-30')


@pytest.mark.parametrize('old_center',['-5','-50','0'])
def test_fresh_restart_anchors_live_not_historic_center(old_center):
    checkpoint={'version':1,'pair_id':'pair','ts':99,'center_bps':old_center}
    model=FastReference('pair',Settings(),checkpoint,started_at=100)
    for t in range(100,105):
        assert not model.observe(t,D('-30')).usable
    assert model.observe(105,D('-30')).center_bps==D('-30')


@pytest.mark.parametrize('gap',[121,10801,86400])
def test_stale_checkpoint_uses_thirty_seconds_not_half_hour(gap):
    model=FastReference('pair',Settings(),{'version':1,'pair_id':'pair','ts':0,'center_bps':'-5'},started_at=gap)
    for t in range(gap,gap+30):
        assert not model.observe(t,D('-30')).usable
    assert model.observe(gap+30,D('-30')).usable


def test_repeated_scans_in_one_second_do_not_warm():
    model=FastReference('pair',Settings())
    for n in range(500):
        view=model.observe(n/1000,D('-30'))
    assert not view.usable and view.live_samples==1


def test_feed_gap_rebuilds_from_new_tail():
    model=FastReference('pair',Settings())
    for t in range(31): model.observe(t,D('-5'))
    assert not model.observe(50,D('-30')).usable
    for t in range(51,81): model.observe(t,D('-30'))
    assert model.view(80).center_bps==D('-30')


def test_wrong_instrument_checkpoint_not_used():
    model=FastReference('new',Settings(),{'version':1,'pair_id':'old','ts':99,'center_bps':'-5'},started_at=100)
    for t in range(100,106): model.observe(t,D('-30'))
    assert not model.view(105).usable


def test_future_checkpoint_is_not_fresh():
    model=FastReference('p',Settings(),{'version':1,'pair_id':'p','ts':200,'center_bps':'-5'},started_at=100)
    for t in range(100,106): model.observe(t,D('-30'))
    assert not model.view(105).usable


def test_out_of_order_observation_rejected():
    model=FastReference('p',Settings())
    model.observe(10,D('-3'))
    with pytest.raises(ValueError): model.observe(9,D('-3'))


def test_invalid_feed_is_not_tradeable_even_when_reference_exists():
    model=FastReference('p',Settings())
    for t in range(31): model.observe(t,D('-30'))
    assert not model.observe(31,D('-30'),valid=False).usable


def test_following_has_no_bps_per_ten_minute_speed_cap():
    model=FastReference('p',Settings())
    for t in range(31): model.observe(t,D('-5'))
    for t in range(31,96): model.observe(t,D('-30'))
    assert model.view(95).center_bps < D('-17')


def test_invalid_observation_remains_invalid_in_status_view():
    model=FastReference('p',Settings())
    for t in range(31): model.observe(t,D('-30'))
    model.observe(31,D('-30'),valid=False)
    assert not model.view(31).usable
    assert model.observe(32,D('-30'),valid=True).usable


def test_checkpoint_round_trip_is_atomic_and_loadable(tmp_path):
    path = tmp_path / 'reference.json'
    model = FastReference('p', Settings())
    for t in range(31):
        model.observe(t, D('-30'))
    save_checkpoint(path, model.checkpoint())
    loaded = load_checkpoint(path)
    warm = FastReference('p', Settings(), loaded, started_at=31)
    for t in range(31, 36):
        assert not warm.observe(t, D('-28')).usable
    assert warm.observe(36, D('-28')).usable
