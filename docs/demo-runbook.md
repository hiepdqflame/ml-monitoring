# Demonstration runbook

## Preparation

Follow the root README Quick start. Keep Docker running and confirm Airflow shows
three DAGs with no import errors. An unpaused training DAG still has no schedule;
the other two are scheduled. Telegram is optional. Prepare browser tabs for
Airflow, MLflow, Evidently, Grafana, Prometheus and Alertmanager.

## Core demonstration (about two minutes)

```bash
bash scripts/demo.sh --expect-auto-retrain
```

Use `--expect-auto-retrain` on the first run of a fresh policy store. Subsequent
runs should use `bash scripts/demo.sh`; the same-window/cooldown guards may
correctly prevent another automatic attempt. Do not delete real policy state just
to obtain a green demonstration.

The script performs and asserts:

1. Initial training passes and Production loads into the API.
2. All three monitored services are ready.
3. Exactly 142 training-distribution requests are served/captured; baseline has no drift.
4. A deterministic shifted window of 142 requests produces drift.
5. The drift DAG creates a child training run when allowed; the child finishes.
6. The same window is checked again: `unchanged_window`, no child and no repeated message.
7. A deliberately weak candidate fails the gate; serving version stays unchanged.

The exact reference rows in the baseline make this an orchestration check, not an
independent statistical test. The shifted window has no real labels. Retraining
reuses Wine and is not evidence that drift was repaired.

Inspect `notify_drift` XCom/logs for the child run ID and reason, then open that
run in `wine_mlflow_pipeline`. In MLflow inspect `trigger_source`,
`source_drift_run`, `source_window`, `data_sha256` and `quality_gate` tags.
The automatically generated JSON record is stored in the volume:

```bash
docker compose cp airflow-scheduler:/opt/airflow/pipeline_data/demo_report.json /tmp/ml-monitoring-demo-report.json
```

## Real outage and recovery (about two minutes)

```bash
bash scripts/demo.sh --outage-only
```

This explicitly stops only this project's API. An EXIT trap restores it even if
a test fails or the script is interrupted normally. It cannot recover from power
loss or SIGKILL; if interrupted that way, run `docker compose start api`.

Expected evidence:

- `service_health_check` fails with `api` listed as down; Telegram DOWN is sent.
- Prometheus APIDown becomes FIRING after one minute and appears in Alertmanager.
- After restart the API reloads its Production model, health succeeds and sends
  RECOVERED; APIDown clears from Prometheus and Alertmanager sends its resolution.

Inspect Alertmanager's metrics for notification failures. Capture its Alerts page
while APIDown is firing; after recovery, that alert should disappear. The script
writes `outage_down.json` and `outage_recovered.json` beside the main demo record.

## Useful failure/guard cases

- Empty/stale window: no analysis, explanatory `reason`, no automatic retrain.
- Pause training in the UI: drift reports `training_paused_or_missing`; unpause
  and trigger drift again while the same data is fresh to resume its pending intent.
- Repeat a window: no repeated registration; policy persists across task processes.
- Failed automatic dispatch: retry/clear the failed trigger task or rerun drift
  while its window is eligible. Pending state and the deterministic run ID prevent
  a second model job for the same window.
- Disable Telegram: workflows still work; delivery is explicitly `disabled`.
- Disable automatic retraining: drift still produces reports and notifications.

## Before finishing

Confirm API is healthy, Production version matches `/health`, targets are UP and
no unexpected training runs are queued. Screenshots are supporting evidence;
the JSON run record, logs and tests allow the result to be checked independently.
