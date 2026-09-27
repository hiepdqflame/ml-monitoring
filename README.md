# ML Monitoring: Manual Airflow, MLflow and Telegram

Repository: https://github.com/hiepdqflame/ml-monitoring

Live-demo evidence: [screenshots and explanations](screenshots/README.md).

A Docker-based teaching project using the **sklearn Wine cultivar dataset**
(178 labeled rows, 13 features, three classes). The historical registry name
`wine_quality_model` is retained, but this is classification of cultivar, not a
wine quality score.

## What runs

- Airflow 2.10.5 with LocalExecutor and its own PostgreSQL metadata database.
- MLflow 2.17.2, PostgreSQL backend, private MinIO artifact bucket.
- FastAPI serving the model in the MLflow `Production` stage.
- Evidently generating HTML feature-drift/data-quality reports.
- Prometheus and Grafana collecting and visualizing API/model/drift metrics.
- Telegram notifications from explicit Airflow tasks.

**Both DAGs are manual-only** (`schedule=None`). There is no automatic retraining,
cron, daily execution, or background Telegram message loop.

## Start

Start Docker Desktop. From this directory:

```bash
# Only on a fresh clone without a local .env:
# cp .env.example .env

docker compose up -d --build
docker compose ps
```

First build downloads ML dependencies and can take several minutes. `minio-init`
and `airflow-init` are one-shot initialization services: `Exited (0)` is normal.
The API starts before a model exists; `/health` reports `model_loaded: false`
until the first successful training DAG. Container health checks liveness.

Default local URLs (chosen to coexist with Tutorial 04 and ci-cd-demo):

- Airflow: http://localhost:18080 (admin / admin for local demo).
- MLflow: http://localhost:15000.
- Prediction API: http://localhost:18010/docs.
- Evidently: http://localhost:18011/docs and http://localhost:18011/reports.
- Grafana: http://localhost:13001 (admin / admin).
- Prometheus: http://localhost:19091.
- MinIO console: http://localhost:19001 (minio / minio123).

Host ports are controlled by `MONITOR_*_PORT` and `AIRFLOW_PORT` in `.env.example`.
Internal container URLs always use Compose service names and container ports.
Existing project data volumes are retained; PostgreSQL is not published to host.

## 1. Run the training pipeline

Open Airflow, select **wine_mlflow_pipeline**, and press **Trigger DAG**.
Accept defaults for the first run. The graph is:

```text
ingest -> validate -> split -> train -> evaluate -> quality_gate
                                            |-> promote_and_reload -> publish_reference --|
                                            |-> keep_current_model -----------------------|-> notify_result
```

Or trigger from the terminal:

```bash
docker compose exec airflow-scheduler airflow dags trigger wine_mlflow_pipeline
```

The pipeline:

1. Snapshots the labeled built-in Wine dataset and validates schema, values,
   classes and duplicate rows. Logs a SHA-256 data fingerprint.
2. Creates a reproducible stratified 142/36 train/holdout split.
3. Fits RandomForest using only training data and logs model, parameters and
   dataset artifacts in MLflow. Large files stay on a volume, not in Airflow XCom.
4. Evaluates accuracy and macro F1. Defaults require both >= 0.90. If Production
   exists, it also scores that model on the same holdout and rejects regressions.
5. Only a passing candidate is registered/promoted and loaded by the serving API.
   If reload fails, the task attempts to restore the previous Production version.
6. Publishes the **training features only** as Evidently reference data.
7. Reports PROMOTED / REJECTED / FAILED, metrics, version, DAG run and MLflow URL
   to Telegram. The notification task preserves upstream failure status.

To demonstrate rejection, trigger with `n_estimators=10`, `max_depth=1`,
`min_accuracy=1.0` and `min_f1=1.0` (a deliberately weak candidate).
A rejected candidate keeps the existing Production model. The DAG can be green
because rejection is a valid gate outcome; read `notify_result` and MLflow tags.
The fixed holdout supports a demo, not an unbiased benchmark after repeated tuning.

## 2. Generate traffic and check drift

After successful training, generate 100 normal requests inside Docker:

```bash
docker compose exec -w /opt/airflow/simulations airflow-scheduler \
  python run_simulation.py -n 100 -r 10 -s normal
```

The simulator uses the same ordered 13 Wine features as training and captures
successful requests/predictions to Evidently. The API itself does not auto-capture.
Trigger **wine_drift_check** in Airflow, or:

```bash
docker compose exec airflow-scheduler airflow dags trigger wine_drift_check
```

The DAG analyzes the most recent `window_size` rows (default 100, minimum 30),
compares them to the reference, writes an HTML report, updates Prometheus, and
notifies Telegram. `drift_threshold` is the fraction of features that must drift
(default 0.30); it is not the per-feature statistical test p-value threshold.

Then repeat with deliberately shifted features:

```bash
docker compose exec -w /opt/airflow/simulations airflow-scheduler \
  python run_simulation.py -n 100 -r 10 -s severe_drift
docker compose exec airflow-scheduler airflow dags trigger wine_drift_check
```

Open Evidently `/reports`, Grafana, and Prometheus `/alerts`. After each scenario,
wait for the drift DAG to finish before generating the next scenario so the recent
window represents that scenario. Alerts have their own `for:` waiting periods;
Airflow sends the analysis result immediately after the manual run.

Drift does not prove accuracy loss. Captured samples have no ground-truth labels
and are never automatically used for training. Running the training DAG again on
this same built-in dataset demonstrates orchestration, not adaptation to new drift.

## 3. Connect Telegram privately

1. Open the official **@BotFather** account on Telegram and send `/newbot`.
2. Choose a display name and a unique username ending in `bot`.
3. Keep the bot token locally. Open your new bot and send `/start`.
4. In a local terminal, run:

   ```bash
   python3 scripts/configure_telegram.py
   ```

5. Enter the token at the hidden prompt. Press Enter for Chat ID to find your
   recent `/start`, or enter a group/chat ID explicitly. If several chats are
   found, choose the intended chat. For a group, add the bot and send a command
   in that group first. The script sends one connection test.
6. Recreate Airflow processes to load the saved environment:

   ```bash
   docker compose up -d --no-deps --force-recreate airflow-webserver airflow-scheduler
   ```

Credentials are stored only in local `.env`, ignored by Git and excluded from
Docker build context. Token-bearing request URLs are never included in notification
exceptions. Do not share `docker compose config` output: it expands environment
secrets. Use `docker compose config --quiet` to validate configuration.

Without configuration, Telegram is explicitly `disabled`; DAG logs do not claim
message delivery. With Telegram enabled, delivery errors fail the final task.
An already deployed model is not undone because a notification failed. Check the
upstream deployment tasks before retrying. Telegram cannot guarantee exactly-once
messages; manually rerunning a notification task can send a duplicate.

Links containing localhost open on the Docker host, not a remote phone. The
message still contains run IDs, version and metrics for remote reading.

## Verify and troubleshoot

```bash
docker compose config --quiet
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests -q
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs --tail 100 airflow-scheduler api evidently
```

- A training failure stays failed even if its notification was sent successfully.
- No reference: run the training DAG through `publish_reference` first.
- No recent data: generate at least 30 successful captured predictions.
- Notification disabled: configure Telegram, recreate both Airflow processes, rerun.
- Wrong feature count: use the provided updated simulator, not Wine Quality's
  unrelated 11-feature schema.
- Runtime requests are held in Evidently memory; restarting Evidently clears the
  captured window. Reference data and generated reports persist in named volumes.
- Airflow task files and logs persist. This local demo has no retention job;
  production operation would need lifecycle policies, stronger authentication,
  secrets management, and a labeled-data feedback loop.

The Airflow test suite skips the API test module because FastAPI runs in its own
image. To run those three serving tests without modifying the application image:

```bash
docker run --rm --entrypoint sh -e PYTHONPATH=/workspace \
  -v "$PWD:/workspace:ro" -w /workspace ml-monitoring-api \
  -c 'pip install --quiet pytest==8.3.5 httpx==0.25.2 && python -m pytest tests/test_serving.py -q -p no:cacheprovider'
```

For an automated manual trigger that waits for the actual final state:

```bash
docker compose exec airflow-scheduler python scripts/trigger_and_wait.py wine_mlflow_pipeline
```

Stop this project without deleting model/data volumes:

```bash
docker compose down
```

Airflow source: `airflow_dags/wine_pipelines.py`. Pipeline implementation:
`pipeline/`. Current verification evidence is recorded in `docs/verification.md`.
