# Verification: scheduled monitoring and guarded retraining

Verified on 2026-09-27 with Docker Desktop on macOS ARM64. This record covers the
scheduled monitoring and guarded-retraining version. The GitHub Actions workflow
provides Linux AMD64 checks; local results below do not imply a completed remote CI run.

## Automated checks

- Airflow image: **37 passed, 1 skipped** (FastAPI serving tests belong to API image).
- API image: **3 passed**; readiness, feature contract and failed-reload preservation.
- `airflow dags list-import-errors`: no import errors.
- `promtool check config`: valid configuration and **7 rules**.
- `amtool check-config`: valid private configuration, receiver and inhibition rule.
- Shell syntax validated for demo wrappers; Docker Compose configuration validated.
- Actual Grafana queries checked against historical Prometheus series; empty 5xx
  series render zero observed error rate only while the target is UP. Prediction
  distribution now shows three cultivar rates, not a quantile of categorical labels.

Tests cover window freshness/newness, durable deduplication, UTC daily quotas,
cooldown, kill switch, recovery after interrupted dispatch, paused training,
health transitions/reminders, malformed health responses, notification failures,
private optional Telegram rendering and the existing model/data quality gates.

## Real pipeline demonstration

The [JSON record](evidence/demo_report.json) contains actual DAG IDs, run IDs, task
states and returned XCom results, not expected or mocked output.

- Bootstrap `manual_demo_20260927T111525_482908`: PROMOTED version 3, Telegram delivered.
- Baseline `manual_demo_20260927T111545_342258`: 142 successful captured requests,
  NO DRIFT, no training child.
- Shifted data `manual_demo_20260927T111552_605130`: another 142 successful requests,
  drift detected and automatic training requested.
- Child `drift__9316d9ed3be0a7c583c885fcf891df3abb6f384eab4945fd54b0c3b89f7d7a17`:
  passed gate, PROMOTED version 4, API reloaded, Telegram delivered.
- Repeat `manual_demo_20260927T111611_537074`: `unchanged_window`, training skipped,
  repeated notification suppressed.
- Weak candidate `manual_demo_20260927T111618_493877`: REJECTED, API remained version 4.
- After rebuilding/restarting Airflow with the reviewed fixes, repeat run
  `manual_demo_20260927T112551_041659` again returned `unchanged_window`; policy survived restart.

The initial live demo preceded the dispatch-recovery/paused-target fixes. Those
edge cases were then reproduced as failing regression tests, fixed, and included
in the final passing suite. They are not claimed as live crash-injection tests.

## Real service outage and recovery

- API was intentionally stopped by `scripts/demo.sh --outage-only`.
- [Failure record](evidence/outage_down.json): health DAG failed, API listed DOWN,
  Telegram delivered; APIDown FIRING in Prometheus and present in Alertmanager.
- [Recovery record](evidence/outage_recovered.json): script restarted API, model
  reloaded, health DAG succeeded, RECOVERED delivered; APIDown cleared in both systems.
- Alertmanager metrics: `alertmanager_notifications_total{integration="telegram"}`
  was **3**; all Telegram failure counters were **0**. This covers sustained drift,
  API-down and API-resolved notifications during the demonstration.
- Final readiness check: API model version 4, reference loaded, 284 captured rows,
  Grafana database OK and all four Prometheus targets UP.

## Evidence and interpretation

[14 current screenshots](../screenshots/automation/README.md) document the real
interfaces, including two owner-supplied Telegram conversation captures. These
show the training/drift workflow, rejected candidate, health DOWN/RECOVERED and
Alertmanager FIRING/RESOLVED messages; their run IDs match the JSON evidence.
Airflow UTC timestamps and browser-local UTC+7 timestamps differ by seven hours.

The baseline deliberately matches the training distribution to make this a
repeatable system demonstration. The small reused holdout and synthetic drift
are not a production evaluation. Automatic retraining reuses Wine labels, does
not consume unlabeled inference features, and does not guarantee drift recovery.

Dependency deprecation warnings remain in the pinned teaching stack (MLflow
registry stages, FastAPI/Pydantic). They do not fail these tests; migrating APIs
would require a separate compatibility upgrade. No cloud deployment, load-test
capacity claim, or multi-host execution guarantee is made.
