"""Reproducible real-service demo; no fabricated metrics, alerts or labels."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import requests

from pipeline.data import FEATURE_NAMES, load_dataset, split_dataset
from scripts.trigger_and_wait import run_dag, wait_for_run


def request(method, url, **kwargs):
    response = requests.request(method, url, timeout=120, **kwargs)
    response.raise_for_status()
    return response.json()


def traffic(rows):
    for row in rows:
        values = [float(v) for v in row]
        prediction = request('POST', 'http://api:8000/predict', json={'features': values, 'feature_names': FEATURE_NAMES})
        request('POST', 'http://evidently:8001/capture', json={
            'features': dict(zip(FEATURE_NAMES, values)), 'prediction': prediction['prediction'],
            'model_version': prediction['model_version'],
        })
    print('Successful predictions captured:', len(rows), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--expect-auto-retrain', action='store_true',
                        help='Require a child run (use on a fresh policy store, before cooldown is consumed)')
    args = parser.parse_args()
    evidence = {'created_at': datetime.now(timezone.utc).isoformat(), 'runs': {}}
    runs = evidence['runs']
    runs['bootstrap'] = run_dag('wine_mlflow_pipeline')
    assert runs['bootstrap']['result']['pipeline_status'] == 'PROMOTED'
    runs['health'] = run_dag('service_health_check')
    train, _ = split_dataset(load_dataset())
    normal = train[FEATURE_NAMES].to_numpy()
    traffic(normal)
    conf = {'window_size': len(normal), 'drift_threshold': 0.3}
    runs['baseline'] = run_dag('wine_drift_check', conf)
    baseline = runs['baseline']['result']
    assert baseline.get('drift_detected') is False or baseline.get('reason') == 'unchanged_window', baseline
    rng = np.random.default_rng(2026)
    shifted = normal + normal.std(axis=0) * (4 + rng.normal(0, .05, normal.shape))
    traffic(shifted)
    runs['drift'] = run_dag('wine_drift_check', conf)
    result = runs['drift']['result']
    assert result['drift_detected'] is True, result
    decision = result['retrain']
    if decision['trigger']:
        runs['auto_training'] = wait_for_run('wine_mlflow_pipeline', decision['run_id'])
        assert runs['auto_training']['result']['pipeline_status'] in ('PROMOTED', 'REJECTED')
    elif args.expect_auto_retrain:
        raise AssertionError('Automatic retraining suppressed: ' + decision['reason'])
    else:
        print('Guard correctly suppressed a new retrain:', decision['reason'], flush=True)
    runs['repeat_window'] = run_dag('wine_drift_check', conf)
    repeat = runs['repeat_window']['result']
    assert repeat['status'] == 'skipped' and repeat['reason'] == 'unchanged_window', repeat
    before = request('GET', 'http://api:8000/health')['model_version']
    runs['rejected'] = run_dag('wine_mlflow_pipeline', {
        'n_estimators': 10, 'max_depth': 1, 'min_accuracy': 1.0, 'min_f1': 1.0})
    assert runs['rejected']['result']['pipeline_status'] == 'REJECTED'
    after = request('GET', 'http://api:8000/health')['model_version']
    assert before == after, 'A rejected candidate changed the serving model'
    evidence['serving_version'] = after
    path = Path('/opt/airflow/pipeline_data/demo_report.json')
    path.write_text(json.dumps(evidence, indent=2))
    print('DEMO PASSED. Evidence:', path, flush=True)


if __name__ == '__main__':
    main()
