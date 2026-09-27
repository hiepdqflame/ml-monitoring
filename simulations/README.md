# Wine monitoring simulations

The simulator uses the same 13 ordered features from `sklearn.datasets.load_wine`
as the Airflow training pipeline. Normal rows are sampled from the real built-in
dataset; drift scenarios deliberately shift feature values. Production captures
have no target labels and are never used automatically as training data.

First run `wine_mlflow_pipeline` successfully to load the model and publish the
142-row training reference. Run commands from the project root:

```bash
# Healthy baseline, 100 predictions and Evidently captures
docker compose exec -w /opt/airflow/simulations airflow-scheduler python run_simulation.py -n 100 -r 10 -s normal

# Manually analyze and send the result through Airflow/Telegram
docker compose exec airflow-scheduler python scripts/trigger_and_wait.py wine_drift_check

# Shifted inputs, then run the drift DAG again
docker compose exec -w /opt/airflow/simulations airflow-scheduler python run_simulation.py -n 100 -r 10 -s severe_drift
```

Wait for each analysis to finish before starting the next scenario. The default
analysis window contains the most recent 100 captures. `--analyze` runs Evidently
directly; use the Airflow DAG when Telegram notification and DAG history are needed.

Scenarios: `normal`, `slight_drift`, `moderate_drift`, `severe_drift`, `sudden_shift`.
Optional traffic patterns: `steady`, `burst`, `gradual`. The predefined
`scenarios.py` normal-day exercise is long-running; use the short commands above
for a classroom demo.

Inside Docker, endpoints come from `API_URL` and `EVIDENTLY_URL`. For optional
host execution, `config.yaml` uses ports 18010 and 18011. See the root README for
the complete setup, screenshots and Telegram configuration.
