"""Bounded ordinary-basis parameter overlays; no trading or network actions."""
from dataclasses import replace
from decimal import Decimal, InvalidOperation
import hashlib
import json

from .live_host import canonical_config_hash


LEGACY_PARAMETER_FIELDS = frozenset({'entry_band_bps', 'exit_min_gross_profit_bps',
    'entropy_exit_room_bps', 'lighter_exit_room_bps', 'max_notional_usd'})
OPENING_PARAMETER_FIELDS = frozenset({'entropy_open_room_bps', 'lighter_open_room_bps'})
PARAMETER_FIELDS = LEGACY_PARAMETER_FIELDS | OPENING_PARAMETER_FIELDS


def parameter_scope(binding):
    identity = {key: binding[key] for key in ('account_binding_hash',
        'pair_identity', 'deployment', 'batch_id')}
    return hashlib.sha256(json.dumps(identity, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()


def parameter_limits(settings, authorization, cfg):
    caps = [settings.max_notional_usd, authorization.max_notional_usd]
    for name in ('entropy', 'hedge'):
        venue = getattr(cfg, name, None)
        if venue is not None:
            caps.append(Decimal(str(venue.cap_usd)))
    cap = min(caps)
    if not cap.is_finite() or cap <= 0:
        raise ValueError('parameters_notional_cap_unavailable')
    limits = {key: str(cap if key == 'max_notional_usd'
        else Decimal('12') if key.endswith('_room_bps') else Decimal('100'))
        for key in sorted(LEGACY_PARAMETER_FIELDS)}
    limits.update(entropy_open_room_bps=str(settings.first_leg_execution_policy('open')[1]),
                  lighter_open_room_bps=str(settings.hedge_execution_policy('open')[1]))
    return limits


def parameter_values(settings):
    return dict(entry_band_bps=str(settings.entry_band_bps),
        exit_min_gross_profit_bps=str(settings.exit_min_gross_profit_bps),
        entropy_exit_room_bps=str(settings.first_leg_execution_policy('close')[0][0]),
        lighter_exit_room_bps=str(settings.hedge_execution_policy('close')[0][0]),
        max_notional_usd=str(settings.max_notional_usd),
        entropy_open_room_bps=None, lighter_open_room_bps=None)


def validate_parameters(values, limits):
    if type(values) is not dict or set(values) not in (LEGACY_PARAMETER_FIELDS, PARAMETER_FIELDS):
        raise ValueError('invalid_parameters_fields')
    result = {}
    for key in sorted(values):
        value = values[key]
        if key in OPENING_PARAMETER_FIELDS and value is None:
            result[key] = None
            continue
        if type(value) is not str or not value or len(value) > 64 or value != value.strip():
            raise ValueError('invalid_parameter_' + key)
        try:
            number = Decimal(value)
        except InvalidOperation:
            raise ValueError('invalid_parameter_' + key) from None
        if not number.is_finite() or (number < 0 if key in OPENING_PARAMETER_FIELDS else number <= 0) or number > Decimal(limits[key]):
            raise ValueError('parameter_out_of_bounds_' + key)
        if number.as_tuple().exponent < -8:
            raise ValueError('invalid_parameter_' + key)
        result[key] = '0' if number == 0 else format(number.normalize(), 'f')
    return result


def project_record(record):
    if record is None:
        return None
    return {**record, 'values': {**dict.fromkeys(OPENING_PARAMETER_FIELDS), **record['values']}}


def upgrade_document(document, limits):
    """Pure projection, persisted only in the existing safe save/start transaction."""
    return {**document, 'schema_version': 2, 'limits': limits,
            'current': project_record(document['current']), 'saved': project_record(document['saved'])}


def opening_policies(settings):
    return {name: dict(rooms_bps=[str(v) for v in policy('open')[0]],
                       hard_cap_bps=str(policy('open')[1]))
            for name, policy in (('entropy', settings.first_leg_execution_policy),
                                 ('lighter', settings.hedge_execution_policy))}


def apply_parameters(settings, values):
    entropy = Decimal(values['entropy_exit_room_bps'])
    lighter = Decimal(values['lighter_exit_room_bps'])
    overrides = {}
    for field, prefix, policy in (('entropy_open_room_bps', 'first', settings.first_leg_execution_policy),
                                  ('lighter_open_room_bps', 'hedge', settings.hedge_execution_policy)):
        if values.get(field) is not None:
            room = Decimal(values[field])
            overrides[f'open_{prefix}_rooms_bps'] = (room,) * len(policy('open')[0])
            overrides[f'open_{prefix}_hard_cap_bps'] = room
    return replace(settings, entry_band_bps=Decimal(values['entry_band_bps']),
        exit_min_gross_profit_bps=Decimal(values['exit_min_gross_profit_bps']),
        max_notional_usd=Decimal(values['max_notional_usd']),
        close_first_rooms_bps=(entropy,) * 3, close_first_hard_cap_bps=entropy,
        close_hedge_rooms_bps=(lighter,) * 3, close_hedge_hard_cap_bps=lighter, **overrides)


def parameter_record(settings, *, pair_id, revision, values):
    return dict(revision=revision,
        config_hash=canonical_config_hash(settings, pair_identity=pair_id), values=values)


def validate_document(document, *, base_hash, base_settings, pair_id, limits):
    schema = document.get('schema_version') if type(document) is dict else None
    expected_limits = ({key: limits[key] for key in LEGACY_PARAMETER_FIELDS}
                       if schema == 1 else limits)
    if (type(schema) is not int or schema not in (1, 2) or document.get('base_config_hash') != base_hash
            or document.get('limits') != expected_limits):
        raise ValueError('parameters_base_configuration_changed')
    for name in ('current', 'saved'):
        record = document.get(name)
        if name == 'saved' and record is None:
            continue
        if (type(record) is not dict or type(record.get('revision')) is not int
                or record['revision'] < 0):
            raise ValueError('parameters_persistence_invalid')
        if schema == 1 and (type(record.get('values')) is not dict
                            or set(record['values']) != LEGACY_PARAMETER_FIELDS):
            raise ValueError('parameters_persistence_invalid')
        values = validate_parameters(record.get('values'), limits)
        effective = apply_parameters(base_settings, values)
        # Revision zero represents the full original policy, including its
        # unequal retry rooms. An overlay always uses one fixed room per leg.
        if record['revision'] == 0:
            effective = base_settings
            if project_record({'values': values})['values'] != validate_parameters(parameter_values(base_settings), limits):
                raise ValueError('parameters_persistence_invalid')
        if record.get('config_hash') != canonical_config_hash(effective, pair_identity=pair_id):
            raise ValueError('parameters_persistence_invalid')
    if document.get('saved') is not None and (
            document['saved']['revision'] <= document['current']['revision']):
        raise ValueError('parameters_persistence_invalid')
    return document
