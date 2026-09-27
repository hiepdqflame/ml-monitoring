#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Uses the existing reference and captured window; never overwrites training data.
docker compose exec -T airflow-scheduler python scripts/trigger_and_wait.py wine_drift_check
