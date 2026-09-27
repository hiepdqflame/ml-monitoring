#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T -w /opt/airflow/simulations airflow-scheduler \
  python run_simulation.py -n 100 -r 10 -s normal --analyze
