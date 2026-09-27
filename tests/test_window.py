import importlib.util
from pathlib import Path

import pytest


def describe():
    path = Path(__file__).parents[1] / 'evidently' / 'window_metadata.py'
    if not path.exists():
        pytest.fail('Window provenance is not implemented')
    spec = importlib.util.spec_from_file_location('window_metadata', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.describe_window


def test_fingerprint_ignores_prediction_and_client_timestamp_but_tracks_features():
    f = describe()
    rows = [{'x': 1.5, 'timestamp': 'a', 'prediction': 0, '_received_at': 90},
            {'x': 2.5, '_received_at': 95}]
    a = f(rows, ['x'], 'boot', 2, True, now=100)
    b = f([{**r, 'prediction': 2, 'timestamp': 'b'} for r in rows], ['x'], 'boot', 2, True, now=100)
    assert a['window_id'] == b['window_id']
    assert a['latest_age_seconds'] == 5
    assert f([{**r, 'x': 99} for r in rows], ['x'], 'boot', 2, True, now=100)['window_id'] != a['window_id']
    assert f([], ['x'], 'boot', 0, False, now=100)['sample_count'] == 0
