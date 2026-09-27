"""Safety policies survive retries, process restarts and repeated drift windows."""
import importlib

import pytest


def store(tmp_path):
    try:
        cls = importlib.import_module('pipeline.automation').AutomationStore
    except ImportError:
        pytest.fail('Persistent automation policy is not implemented')
    return cls(tmp_path / 'automation.sqlite')


def window(**changes):
    return dict(reference_loaded=True, sample_count=100, window_id='a' * 64,
                epoch='boot-1', cursor=100, latest_age_seconds=10, **changes)


def test_only_sufficient_new_fresh_data_is_analyzed(tmp_path):
    db = store(tmp_path)
    w = window()
    assert db.analysis_reason(w) == 'ready'
    assert db.analysis_reason({**w, 'reference_loaded': False}) == 'no_reference'
    assert db.analysis_reason({**w, 'sample_count': 29}) == 'insufficient_samples'
    assert db.analysis_reason({**w, 'latest_age_seconds': 8000}) == 'stale_data'
    db.record_analysis(w)
    assert store(tmp_path).analysis_reason(w) == 'unchanged_window'
    assert db.analysis_reason({**w, 'window_id': 'b', 'cursor': 110}) == 'insufficient_new_samples'
    assert db.analysis_reason({**w, 'window_id': 'b', 'cursor': 130}) == 'ready'
    assert db.analysis_reason({**w, 'window_id': 'b', 'epoch': 'boot-2', 'cursor': 30}) == 'ready'


def test_retrain_reservation_is_idempotent_and_durable(tmp_path):
    db = store(tmp_path)
    result = {**window(), 'drift_detected': True}
    first = db.reserve_retrain(result, 'parent-1', now=100000)
    assert first['trigger'] is True
    retry = store(tmp_path).reserve_retrain(result, 'parent-1', now=100001, training_busy=True)
    assert retry == first
    db.complete_dispatch(result['window_id'])
    assert db.reserve_retrain(result, 'parent-2', now=200000)['reason'] == 'window_already_requested'
    assert db.reserve_retrain({**result, 'window_id': 'b'}, 'parent-3', now=100100)['reason'] == 'cooldown'


def test_retrain_limits_and_kill_switch(tmp_path):
    db = store(tmp_path)
    result = {**window(), 'drift_detected': True}
    assert db.reserve_retrain(result, 'p', enabled=False)['reason'] == 'automatic_retraining_disabled'
    assert db.reserve_retrain(result, 'p', training_busy=True)['reason'] == 'training_active'
    assert db.reserve_retrain({**result, 'drift_detected': False}, 'p')['reason'] == 'no_drift'
    for i in range(3):
        assert db.reserve_retrain({**result, 'window_id': str(i)}, str(i), now=100000+i,
                                  cooldown=0)['trigger'] is True
    assert db.reserve_retrain({**result, 'window_id': 'four'}, 'four', now=100010,
                              cooldown=0)['reason'] == 'daily_limit'
    assert db.reserve_retrain({**result, 'window_id': 'five'}, 'five', now=200000,
                              cooldown=0)['trigger'] is True


def test_health_alerts_only_on_transition_or_reminder(tmp_path):
    db = store(tmp_path)
    assert db.health_notification(['api'], now=100)['status'] == 'DOWN'
    db.record_health(['api'], now=100)
    assert store(tmp_path).health_notification(['api'], now=101) is None
    assert db.health_notification(['api'], now=4000)['status'] == 'DOWN'
    assert db.health_notification([], now=102)['status'] == 'RECOVERED'
    db.record_health([], now=102)
    assert db.health_notification([], now=103) is None


def test_interrupted_dispatch_recovers_without_spending_another_quota(tmp_path):
    db = store(tmp_path)
    result = {**window(), 'drift_detected': True, 'report_url': '/reports/example.html'}
    db.record_analysis(result)
    assert db.recover_analysis(window()) == result  # Crash before the decision task.
    first = db.reserve_retrain(result, 'parent-1', now=100000)
    restarted = store(tmp_path)
    assert restarted.recover_analysis(window()) == result  # Crash before Airflow accepted child.
    second = restarted.reserve_retrain(result, 'parent-2', now=100001)
    assert second['run_id'] == first['run_id']
    restarted.complete_dispatch(result['window_id'])
    assert restarted.recover_analysis(window()) is None
    assert restarted.reserve_retrain(result, 'parent-3', now=200000)['reason'] == 'window_already_requested'


def test_paused_training_blocks_new_and_pending_dispatch(tmp_path):
    db = store(tmp_path)
    result = {**window(), 'drift_detected': True}
    assert db.reserve_retrain(result, 'one', training_available=False)['reason'] == 'training_paused_or_missing'
    db.reserve_retrain(result, 'one')
    assert db.reserve_retrain(result, 'two', training_available=False)['reason'] == 'training_paused_or_missing'
