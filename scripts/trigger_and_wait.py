"""Trigger/wait through Airflow's REST API; assert the real terminal state."""
import argparse
import ast
from datetime import datetime, timezone
import json
import os
import time

import requests

DAGS = ['wine_mlflow_pipeline', 'wine_drift_check', 'service_health_check']


def airflow_session():
    session = requests.Session()
    session.auth = (os.getenv('AIRFLOW_ADMIN_USER', 'admin'), os.environ['AIRFLOW_ADMIN_PASSWORD'])
    return session


def wait_for_run(dag, run_id, expected='success', timeout=1200):
    session = airflow_session()
    base = 'http://airflow-webserver:8080/api/v1/dags/' + dag + '/dagRuns/' + run_id
    deadline = time.monotonic() + timeout
    previous = None
    while time.monotonic() < deadline:
        response = session.get(base, timeout=15)
        response.raise_for_status()
        state = response.json()['state']
        if state != previous:
            print(dag, run_id, state, flush=True)
            previous = state
        if state in ('success', 'failed'):
            tasks = session.get(base + '/taskInstances', timeout=15)
            tasks.raise_for_status()
            states = {t['task_id']: t['state'] for t in tasks.json()['task_instances']}
            task = {'wine_mlflow_pipeline': 'notify_result', 'wine_drift_check': 'notify_drift',
                    'service_health_check': 'report'}[dag]
            key = 'health_result' if dag == 'service_health_check' else 'return_value'
            result = session.get(base + '/taskInstances/' + task + '/xcomEntries/' + key, timeout=15)
            value = result.json()['value'] if result.ok else None
            if isinstance(value, str):
                value = ast.literal_eval(value)
            output = {'dag': dag, 'run_id': run_id, 'state': state, 'tasks': states, 'result': value}
            print(json.dumps(output, indent=2), flush=True)
            if state != expected:
                raise RuntimeError(f'Expected {expected}, observed {state}; inspect task logs for {run_id}')
            return output
        time.sleep(3)
    raise TimeoutError(f'DAG {dag}/{run_id} did not finish within {timeout} seconds')


def run_dag(dag, conf=None, expected='success'):
    run_id = 'manual_demo_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%f')
    session = airflow_session()
    base = 'http://airflow-webserver:8080/api/v1/dags/' + dag
    # A fresh clone or an intentionally paused demo DAG must be runnable.
    response = session.patch(base, json={'is_paused': False}, timeout=15)
    response.raise_for_status()
    response = session.post(base + '/dagRuns', json={'dag_run_id': run_id, 'conf': conf or {}}, timeout=15)
    response.raise_for_status()
    return wait_for_run(dag, run_id, expected)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dag', choices=DAGS)
    parser.add_argument('--conf', default='{}')
    parser.add_argument('--expect-failure', action='store_true')
    args = parser.parse_args()
    run_dag(args.dag, json.loads(args.conf), 'failed' if args.expect_failure else 'success')


if __name__ == '__main__':
    main()
