# Verification evidence

Checks performed on 2026-09-27 using Docker Desktop on macOS ARM64.

## Completed

- Docker Compose configuration validation passed without printing credentials.
- Airflow image passed `pip check`; all service images built successfully.
- Pipeline and DAG suite in the running Airflow container: **24 passed**, one
  serving test module skipped because FastAPI lives in the separate API image.
- Serving suite in a disposable API container: **3 passed** (503 before loading,
  feature-contract rejection, preservation of current model on reload failure).
- Both DAGs import without errors; both are manual-only, catchup disabled, and
  limited to one active run each.
- Failure-path run `manual_demo_20260927T085627_039000`: drift analysis without a
  reference failed, and the final notification task preserved the failed DAG state.
- Training run `manual_demo_20260927T085655_839967`: successful promotion, reload,
  reference publication and finalization. Model version **1**, MLflow run
  `8f3687fac30e40ffacbf3b6aaa2b4047`, holdout accuracy **1.0**, macro F1 **1.0**.
- Rejection run `manual_demo_20260927T085855_311207`: shallow 10-tree candidate
  failed the strict quality gate; promotion/reference tasks skipped and Production
  remained version 1. Final business status was **REJECTED**.
- Reference dataset: **142 training rows, 13 features**, no holdout labels/features.
- Normal simulation: **100/100 successful requests**, captured by Evidently.
- Baseline drift run `manual_demo_20260927T090417_086738`: **NO DRIFT**, HTML report
  `drift_report_20260927_090421.html` created in the persistent reports volume.

- Drift simulation: **100/100 successful requests** using shifted feature values.
- Drift run `manual_demo_20260927T090610_088292`: **DRIFT DETECTED**, 13/13 features,
  drifted-feature share 1.0; report `drift_report_20260927_090612.html` generated.
- Prometheus: model API, Evidently and Prometheus targets all **UP**; drift status
  1, drifted-feature count 13 and 13 per-feature series confirmed via query API.
- Grafana: both ML Model Monitoring and Evidently Data Drift Monitoring dashboards
  provisioned, returned by its API and visually checked in the browser.
- Prometheus `DataDriftDetected` reached **FIRING** and was captured in the UI.
- The [15 evidence screenshots](../screenshots/README.md) include Airflow run
  graphs, MLflow Production/Archived stages, Prometheus, Grafana, Evidently and
  the user's actual Telegram conversation. Their scope and run IDs are documented.

## Interpretation

Perfect holdout scores on this small built-in teaching dataset are not evidence of
production performance. The split is fixed and can be repeatedly inspected during
the demo. Drift analysis compares features; it does not measure live accuracy or
supply ground-truth training labels.

## Telegram live verification

Bot credentials were configured locally through the private setup script, followed
by rebuilding and recreating the Airflow webserver and scheduler. Credentials are
not included in this document.

- Training DAG run `manual_demo_20260927T091751_785097`: **success**, business outcome
  **PROMOTED**, Telegram **delivered**. Production advanced to model version **2**;
  API health confirmed version 2 loaded. MLflow run
  `a1d4aa9ae61e493d9bab8f3559428cd2`, accuracy **1.0**, macro F1 **1.0**.
- Drift DAG run `manual_demo_20260927T091829_501469`: **success**, drift detected,
  Telegram **delivered**. The analysis used the captured drift-demo data.
- Delivery status is based on successful Telegram Bot API responses received by
  the actual Airflow notification tasks, not a mocked test or standalone script.
- Latest Airflow image: **24 tests passed**, one serving module skipped; the
  separate API suite's **3 passed** result is recorded above.
- Both DAGs still have `schedule=None` and import without errors. Notifications
  are sent when a user manually triggers a DAG; no automatic runs were enabled.

Earlier verification runs intentionally used Telegram disabled before credentials
were configured. Live checks now cover training success and detected drift; rejected
and failed pipeline branches were tested earlier with delivery disabled.
