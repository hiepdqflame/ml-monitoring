#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" == "--outage-only" ]]; then
  # Always restart this project's API, even when an assertion or Ctrl-C interrupts.
  trap 'docker compose start api >/dev/null' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  docker compose stop api
  docker compose exec -T airflow-scheduler python scripts/demo_outage.py down
  docker compose start api
  docker compose exec -T airflow-scheduler python scripts/demo_outage.py recovered
else
  docker compose exec -T airflow-scheduler python scripts/demo_pipeline.py "$@"
fi
