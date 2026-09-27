# Automated pipeline evidence

These 14 screenshots document the real Docker demonstration on 2026-09-27:
12 browser captures and two Telegram screenshots supplied by the project owner.
All images show actual UIs; no dashboard values or task statuses were rewritten.
See [verification](../../docs/automation-verification.md) and the
[machine-readable run record](../../docs/evidence/demo_report.json).

## Workflow

- [Three DAGs and schedules](airflow_schedules.png): health every 15 minutes,
  drift hourly, training externally triggered; no DAG is paused.
- [Drift triggers training](drift_triggers_training.png): parent run
  `manual_demo_20260927T111552_605130`; `trigger_training` succeeded.
- [Automatic training](automatic_training.png): child run
  `drift__9316d9ed3be0a7c583c885fcf891df3abb6f384eab4945fd54b0c3b89f7d7a17`;
  passed gate, promoted and notified.
- [Repeated window](repeated_window_skipped.png): run
  `manual_demo_20260927T111611_537074`; training branch skipped, reason
  `unchanged_window` in the linked JSON record.
- [MLflow Production](mlflow_production_v4.png): version 4 Production, earlier
  versions Archived. The weak candidate was rejected before registration.

![Guarded drift-to-training workflow](drift_triggers_training.png)

## Operational failure and recovery

- [Health failure](health_failure.png): run `manual_demo_20260927T111841_207979`;
  API was deliberately stopped. Individual checks return observations; the final
  report task fails the DAG and sends a DOWN notification.
- [Health recovery](health_recovery.png): run `manual_demo_20260927T112028_161214`;
  API restored, all services ready and RECOVERED notification delivered.
- [Alertmanager firing](alertmanager_firing.png): actual APIDown and
  DataDriftDetected alerts routed to the Telegram receiver. APIDown cleared after
  recovery, as asserted in [outage record](../../docs/evidence/outage_recovered.json).
- [Prometheus targets](prometheus_targets.png): four targets UP, including Alertmanager.

![Actual operational alerts](alertmanager_firing.png)

## Data and metrics

- [Evidently drift](evidently_drift.png): shifted 142-row window, 13 drifted features.
- [Grafana traffic](grafana_traffic.png): actual historical traffic/latency and
  predicted cultivar class rates; selected range 11:14-11:23 UTC.
- [Grafana drift](grafana_drift.png): feature drift count/share/history.

## Telegram conversation

- [Training and drift](telegram-training-and-drift.png): initial version 3 promotion,
  NO DRIFT, DRIFT DETECTED with child run ID, and version 4 PROMOTED with `Trigger: drift`.
  Parent run `manual_demo_20260927T111552_605130` and the child run match the workflow
  screenshots and JSON evidence above.
- [Alerts and recovery](telegram-alerts-and-recovery.png): candidate REJECTED,
  DataDriftDetected FIRING, HEALTH DOWN (`manual_demo_20260927T111841_207979`),
  APIDown FIRING, HEALTH RECOVERED (`manual_demo_20260927T112028_161214`) and
  APIDown RESOLVED. The rejected candidate left Production unchanged.

![Telegram training and drift notifications](telegram-training-and-drift.png)

![Telegram rejection, operational alerts and recovery](telegram-alerts-and-recovery.png)

The owner supplied these actual conversation screenshots. Run IDs were checked
against the recorded demo; no bot token or private chat ID is visible. Times in
Telegram are UTC+7, while run IDs use UTC.
Delivery is also corroborated by Airflow task responses and Alertmanager counters
(three Telegram sends, zero failures at the verification checkpoint).

Training reused labeled Wine, so the drift alert correctly remained active after
retraining. No claim is made that the new model repaired the shifted distribution.
