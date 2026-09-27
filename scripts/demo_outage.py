"""Assert live outage/recovery; invoked by demo.sh which owns container restoration."""
import argparse
import json
from pathlib import Path
import time

import requests
from scripts.trigger_and_wait import run_dag


def wait_for_alert(active, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = requests.get('http://prometheus:9090/api/v1/alerts', timeout=10)
        response.raise_for_status()
        firing = any(a['labels'].get('alertname') == 'APIDown' and a['state'] == 'firing'
                     for a in response.json()['data']['alerts'])
        manager = requests.get('http://alertmanager:9093/api/v2/alerts', timeout=10)
        manager.raise_for_status()
        routed = any(a['labels'].get('alertname') == 'APIDown' for a in manager.json())
        if firing == active and routed == active:
            return {'prometheus_firing': firing, 'alertmanager_active': routed}
        time.sleep(5)
    raise TimeoutError('APIDown did not reach the expected Prometheus/Alertmanager state')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['down', 'recovered'])
    args = parser.parse_args()
    down = args.phase == 'down'
    if not down:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                response = requests.get('http://api:8000/health', timeout=5)
                if response.ok and response.json().get('model_loaded'):
                    break
            except requests.RequestException:
                pass
            time.sleep(3)
        else:
            raise TimeoutError('API did not recover')
    evidence = {'health': run_dag('service_health_check', expected='failed' if down else 'success'),
                'alerts': wait_for_alert(down)}
    if down:
        # Let Alertmanager's grouping delay elapse so a real firing message can be sent.
        time.sleep(20)
    path = Path('/opt/airflow/pipeline_data') / ('outage_' + args.phase + '.json')
    path.write_text(json.dumps(evidence, indent=2))
    print('OUTAGE CHECK PASSED:', args.phase, flush=True)


if __name__ == '__main__':
    main()
