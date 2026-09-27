"""Trigger a manual DAG through Airflow's API and report its actual final state."""
import argparse
from datetime import datetime, timezone
import json
import os
import time

import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dag', choices=['wine_mlflow_pipeline', 'wine_drift_check'])
    parser.add_argument('--conf', default='{}')
    parser.add_argument('--expect-failure', action='store_true')
    args = parser.parse_args()
    conf = json.loads(args.conf)
    session = requests.Session()
    session.auth = (os.getenv('AIRFLOW_ADMIN_USER', 'admin'), os.environ['AIRFLOW_ADMIN_PASSWORD'])
    base = 'http://airflow-webserver:8080/api/v1/dags/' + args.dag
    run_id = 'manual_demo_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%f')
    response = session.post(base + '/dagRuns', json={'dag_run_id': run_id, 'conf': conf}, timeout=15)
    response.raise_for_status()
    print('Triggered:', args.dag, run_id, flush=True)
    deadline = time.monotonic() + 1200
    previous = None
    while time.monotonic() < deadline:
        response = session.get(base + '/dagRuns/' + run_id, timeout=15)
        response.raise_for_status()
        state = response.json()['state']
        if state != previous:
            print('DAG state:', state, flush=True)
            previous = state
        if state in ('success', 'failed'):
            tasks = session.get(base + '/dagRuns/' + run_id + '/taskInstances', timeout=15)
            tasks.raise_for_status()
            print(json.dumps({t['task_id']: t['state'] for t in tasks.json()['task_instances']}, indent=2), flush=True)
            final_task = 'notify_result' if args.dag == 'wine_mlflow_pipeline' else 'notify_drift'
            result = session.get(base + '/dagRuns/' + run_id + '/taskInstances/' + final_task + '/xcomEntries/return_value', timeout=15)
            if result.ok:
                print('Result:', result.json()['value'], flush=True)
            expected = 'failed' if args.expect_failure else 'success'
            if state != expected:
                raise RuntimeError('Expected ' + expected + ', observed ' + state + '; inspect Airflow task logs')
            return
        time.sleep(3)
    raise TimeoutError('DAG did not finish in 20 minutes')


if __name__ == '__main__':
    main()
