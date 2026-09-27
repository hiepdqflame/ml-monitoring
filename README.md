# Wine MLOps: Automated Monitoring and Guarded Retraining

[![Docker verification](https://github.com/hiepdqflame/ml-monitoring/actions/workflows/ci.yml/badge.svg)](https://github.com/hiepdqflame/ml-monitoring/actions/workflows/ci.yml)

**Author:** Do Quang Hiep - 25MS13293

**Repository:** https://github.com/hiepdqflame/ml-monitoring

A reproducible Docker teaching project: Airflow orchestrates a validated data
pipeline, MLflow tracks and versions models, FastAPI serves predictions, Evidently
measures feature drift, and Prometheus/Alertmanager/Grafana monitor operations.
Airflow and Alertmanager deliver optional Telegram notifications.

The dataset is **sklearn Wine cultivar classification**: 178 labeled rows,
13 features, three classes. `wine_quality_model` is a historical registry name;
the prediction is a cultivar class, not a wine quality score.

[Current evidence](screenshots/automation/README.md) ·
[Verification record](docs/automation-verification.md) · [Demo runbook](docs/demo-runbook.md)

## Architecture

```mermaid
flowchart TD
    Data[Labeled Wine data] --> Train[Airflow training DAG]
    Train --> Gate{Accuracy + macro F1 + champion comparison}
    Gate -->|Pass| Registry[MLflow Production + MinIO artifacts]
    Gate -->|Reject| Keep[Keep current model]
    Registry --> API[FastAPI serving]
    Train --> Ref[Training features as reference]
    API --> Capture[Captured inference features]
    Ref --> Evidently[Evidently reports and drift metrics]
    Capture --> Evidently
    Drift[Hourly Airflow drift DAG] --> Evidently
    Drift --> Guard{New window + cooldown + daily quota}
    Guard -->|Drift and allowed| Train
    Health[15-minute health DAG] --> API
    Health --> Registry
    Health --> Evidently
    API --> Prom[Prometheus]
    Evidently --> Prom
    Prom --> Grafana[Grafana dashboards]
    Prom --> AM[Alertmanager: group, inhibit, resolve]
    AM --> Telegram[Telegram]
    Health --> Telegram
    Train --> Telegram
    Drift --> Telegram
```

## Quick start

Requirements: Git, Docker Desktop/Engine with Compose v2 and internet access for
initial image/dependency downloads. Allocate about 6-8 GB RAM to Docker. Tested
locally on macOS ARM64; CI exercises Docker builds on Linux AMD64.

```bash
git clone https://github.com/hiepdqflame/ml-monitoring.git
cd ml-monitoring
cp .env.example .env       # Fresh clone only; preserve an existing configured .env.
docker compose up -d --build
docker compose ps
bash scripts/test_dashboard.sh
bash scripts/demo.sh --expect-auto-retrain
```

First build takes several minutes. `minio-init`, `airflow-init` and
`alertmanager-init` are one-shot jobs: `Exited (0)` is expected. The prediction API
is alive before a model exists but not ready for predictions; the demo first
trains and deploys a model. A scheduled health check can report that initial
not-ready state correctly. Telegram is disabled by default; no bot is required.

All published ports bind to localhost and coexist with the other tutorials:

- Airflow: http://localhost:18080 - `admin / admin`
- MLflow: http://localhost:15000
- API Swagger: http://localhost:18010/docs
- Evidently: http://localhost:18011/reports
- Grafana: http://localhost:13001 - `admin / admin`
- Prometheus: http://localhost:19091
- Alertmanager: http://localhost:19093
- MinIO console: http://localhost:19001 - `minio / minio123`

Change host ports with the documented variables in `.env.example`. Internal
container URLs use Compose service names. These credentials are local teaching
defaults; the stack is not configured as a public production deployment.

## Live demonstration screenshots

Captured from the running Docker stack on **2026-09-27**. The screenshots below
document the automated version; exact run IDs and outcomes are available in the
[evidence index](screenshots/automation/README.md) and
[JSON run record](docs/evidence/demo_report.json).

### Scheduled Airflow and automatic retraining

Three enabled DAGs: health checks every 15 minutes, hourly drift analysis, and
training triggered manually or by the drift DAG.

![Airflow DAG list showing the three workflows and their schedules](screenshots/automation/airflow_schedules.png)

The drift run took the `trigger_training` branch after passing the retraining
policy checks. Its child training run promoted model version 4.

![Successful drift analysis triggering the training DAG](screenshots/automation/drift_triggers_training.png)

<details>
<summary>View the child training run and repeated-window protection</summary>

The child completed validation, training, evaluation, promotion, reference
publication and notification.

![Successful automatically triggered training pipeline](screenshots/automation/automatic_training.png)

Checking the same window again skipped training. The recorded reason was
`unchanged_window`, and the repeated notification was suppressed.

![Repeated data window taking the no-retrain branch](screenshots/automation/repeated_window_skipped.png)

</details>

### MLflow model lifecycle

Version 4 is in Production; earlier versions are Archived. The deliberately weak
candidate was rejected before registration and did not replace the serving model.

![MLflow registry with version 4 in Production](screenshots/automation/mlflow_production_v4.png)

### Operational alerts and recovery

Alertmanager received real `APIDown` and `DataDriftDetected` alerts and routed
them to Telegram. The API was intentionally stopped for the outage test and
restored afterward; `APIDown` then resolved.

![Alertmanager showing API-down and sustained-drift alerts](screenshots/automation/alertmanager_firing.png)

<details>
<summary>View the failed health check, recovery and healthy scrape targets</summary>

The final health-report task failed the DAG when the API was down, even though
the DOWN notification was delivered successfully.

![Airflow health check correctly failing during the API outage](screenshots/automation/health_failure.png)

After the API restarted, the health DAG succeeded and sent RECOVERED.

![Successful service health check after API recovery](screenshots/automation/health_recovery.png)

All four Prometheus targets were UP at the final verification checkpoint.

![Prometheus targets for API, Evidently, Alertmanager and Prometheus all UP](screenshots/automation/prometheus_targets.png)

</details>

### Feature drift and serving metrics

Evidently detected drift in all 13 features of the shifted 142-row window.
Retraining reused the labeled Wine dataset, so this is evidence of the automated
lifecycle, not proof that the model repaired the shifted distribution.

![Evidently report showing 13 of 13 drifted features](screenshots/automation/evidently_drift.png)

Grafana displays actual request rates, latency, observed server errors and
predicted cultivar class rates during the demo window (11:14-11:23 UTC).

![Grafana serving dashboard with historical traffic and class prediction rates](screenshots/automation/grafana_traffic.png)

<details>
<summary>View the detailed drift dashboard</summary>

![Grafana drift dashboard with feature counts, drift share and analysis history](screenshots/automation/grafana_drift.png)

</details>

<details>
<summary>Historical Telegram screenshot from the initial manual demo</summary>

The owner supplied this real bot-conversation screenshot for the earlier manual
version. It shows training and drift messages from that version. Delivery for the
new automated demo is documented separately in the
[verification record](docs/automation-verification.md); this is not a new
Telegram screenshot.

![Historical Telegram conversation showing training and drift notifications](screenshots/telegram-alerts-01.png)

</details>

## Airflow behavior

- **`service_health_check`** runs every 15 minutes (`*/15 * * * *`, UTC). It checks
  API model readiness, MLflow and Evidently. DOWN and RECOVERED transitions send
  Telegram; unchanged failures remind at most hourly. An unhealthy service makes
  the DAG fail even if its notification succeeds.
- **`wine_drift_check`** runs hourly (`@hourly`, UTC). It requires reference data,
  at least 30 samples, at least 30 new observations since the last analysis, and
  a latest observation no older than two hours. Missing, stale or unchanged data
  produces an explained no-op, not a fake analysis or a retrain loop.
- **`wine_mlflow_pipeline`** has no independent schedule. Run it manually or let
  the drift DAG trigger it after the persistent policy checks pass.

All DAGs disable catchup and allow one active run each. UI Trigger and the CLI
remain available; you do not need to wait an hour to demonstrate the workflow.

```bash
docker compose exec airflow-scheduler python scripts/trigger_and_wait.py wine_mlflow_pipeline
docker compose exec airflow-scheduler python scripts/trigger_and_wait.py wine_drift_check
docker compose exec airflow-scheduler python scripts/trigger_and_wait.py service_health_check
```

The CLI enables the requested DAG before triggering. For manual-only mode, set
`MONITOR_SCHEDULES_ENABLED=false` and `AUTO_RETRAIN_ENABLED=false` in `.env`, then:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

## Retraining controls and model lifecycle

1. Ingest and validate schema, finite values, labels and duplicate rows; record a
   SHA-256 dataset fingerprint and a reproducible stratified 142/36 split.
2. Train RandomForest on training rows only; log parameters, model signature,
   dataset artifacts, metrics and Airflow/drift provenance to MLflow.
3. Require accuracy and macro F1 >= 0.90 and no regression against the current
   Production model on the same holdout. Rejection is a valid green DAG outcome;
   it keeps the model and reference unchanged.
4. Promote and reload only after passing. A reload failure attempts to restore
   the previous Production version. Publish only training features as reference.
5. Report PROMOTED / REJECTED / FAILED to Telegram and preserve upstream failures.

Automatic attempts have a **6-hour cooldown**, **3 attempts per UTC day** and a
feature-window fingerprint that prevents repeating the same request. SQLite
transactions persist the policy in `pipeline_data`; deterministic Airflow child
run IDs make retries idempotent. Pending analysis/dispatch can be resumed after
interruption while the data is still eligible. A paused/missing training DAG is
reported as suppressed instead of claiming a successful queued deployment.

`AUTO_RETRAIN_ENABLED=false` is the automatic-retrain kill switch. Cooldown/quota
are configurable through `RETRAIN_COOLDOWN_SECONDS` and `RETRAIN_DAILY_LIMIT`;
manual training remains possible. Failures consume an automatic attempt to bound
repeated work. Inspect failed child tasks before clearing/retrying them.

**What retraining means here:** inference features have no ground-truth labels.
Automatic training deliberately reuses the labeled built-in Wine dataset to
illustrate orchestration, registration and deployment. It does not learn from
unlabeled traffic and does not prove that drift is resolved. Real adaptation needs
new verified labels and an independent evaluation set. Perfect scores on this
small reused holdout are not a production-accuracy claim.

## Telegram and operational alerts

Create a bot through Telegram's BotFather, then open your bot and send `/start`.
Configure credentials privately from the project root:

```bash
python3 scripts/configure_telegram.py
# Rebuild if code changed, then regenerate private Alertmanager config.
docker compose up -d --build --force-recreate alertmanager-init airflow-webserver airflow-scheduler
docker compose up -d --force-recreate alertmanager
```

The helper prompts for token privately, discovers your chat and sends a test.
It writes only local `.env`. Never paste the token into README, screenshots or Git.
Alertmanager's token and generated config live in a private Docker volume; they
are regenerated at initialization and never printed by the renderer.

Airflow reports workflow decisions and service-readiness transitions. Alertmanager
reports sustained metric alerts, groups notifications, inhibits secondary API
alerts while APIDown is active, repeats after four hours and sends resolutions.
APIDown/EvidentlyServiceDown require one minute; DataDriftDetected requires two.
A health transition and a sustained Prometheus alert describe different checks.
AlertmanagerDown is visible in Prometheus; Alertmanager cannot deliver an alert
about itself while it is unavailable.

Telegram delivery failures fail the explicit Airflow notification task, so check
upstream deployment status before retrying. Messages are not exactly-once.
Localhost links in messages are reachable on the Docker host, not a remote phone.

## Tests and CI

```bash
docker compose config --quiet
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose exec alertmanager amtool check-config /etc/alertmanager/private/alertmanager.yml
```

The Airflow suite skips the separate FastAPI serving module. Run it separately:

```bash
docker run --rm --entrypoint sh -e PYTHONPATH=/workspace \
  -v "$PWD:/workspace:ro" -w /workspace ml-monitoring-api \
  -c 'pip install --quiet pytest==8.3.5 httpx==0.25.2 && python -m pytest tests/test_serving.py -q -p no:cacheprovider'
```

GitHub Actions has three jobs: **pipeline**, **serving**, **monitoring-config**.
They do not need Telegram secrets. Live demo assertions verify the real containers,
registry, quality gate and child DAG; unit tests exercise failure and guard cases.

## Persistence and limits

Models/artifacts, Airflow metadata/logs, reference data, reports, alert state and
policy state use Docker volumes. Captured inference rows are an in-memory window
of at most 10,000 rows and reset when Evidently restarts. Prometheus keeps seven
days of metrics. Reports and pipeline artifacts have no automatic retention job.
The SQLite policy assumes this single-host LocalExecutor deployment; a multi-host
production system needs a shared transactional store and deployment locking.

```bash
docker compose stop     # Stop this project; keep data.
docker compose start   # Resume existing containers.
```
