#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T airflow-scheduler python - <<'CHECK'
import requests
for name, url in [('API', 'http://api:8000/health'), ('Evidently', 'http://evidently:8001/health'), ('Grafana', 'http://grafana:3000/api/health')]:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    print(name, response.json())
response = requests.get('http://prometheus:9090/api/v1/targets', timeout=10)
response.raise_for_status()
targets = response.json()['data']['activeTargets']
assert targets and all(target['health'] == 'up' for target in targets), 'A scrape target is down'
print('Prometheus targets: all UP')
CHECK
