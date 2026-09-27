"""Manual-only training and drift demos. No automatic production-data retraining."""
import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule


def finish_training():
    from pipeline.notifications import send_telegram
    from pipeline.quality import pipeline_status
    context = get_current_context()
    ti = context['ti']
    result = ti.xcom_pull(task_ids='evaluate') or {}
    deployed = ti.xcom_pull(task_ids='publish_reference') or {}
    instances = context['dag_run'].get_task_instances()
    status = pipeline_status([i.state for i in instances if i.task_id != ti.task_id], bool(deployed))
    failed = [i.task_id for i in instances if i.state in ('failed', 'upstream_failed')]
    run_id = result.get('mlflow_run_id', '')
    text = (f"ML monitoring | {status}\nDAG: {context['dag'].dag_id}\nRun: {context['run_id']}\n"
            f"Model version: {deployed.get('model_version', 'unchanged')}\n"
            f"Metrics: {result.get('candidate', {})}\nGate: {result.get('gate', {})}\n"
            f"Failed tasks: {', '.join(failed) or 'none'}\n"
            f"MLflow: {os.getenv('MLFLOW_PUBLIC_URL', 'http://localhost:15000')}\nMLflow run: {run_id}")
    delivery = send_telegram(text)
    logging.info('Telegram notification status: %s', delivery)
    if status == 'FAILED':
        raise AirflowException('Pipeline failed; see upstream task logs. Notification status: ' + delivery)
    return {'pipeline_status': status, 'telegram': delivery, 'model_version': deployed.get('model_version')}


with DAG(
    'wine_mlflow_pipeline', description='Manual data validation, training, gated promotion and Telegram',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'), schedule=None, catchup=False, max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20), default_args={'retries': 1, 'retry_delay': timedelta(seconds=20)},
    params={'n_estimators': Param(100, type='integer', minimum=10, maximum=500),
            'max_depth': Param(10, type='integer', minimum=1, maximum=30),
            'min_accuracy': Param(.9, type='number', minimum=0, maximum=1),
            'min_f1': Param(.9, type='number', minimum=0, maximum=1)},
    tags=['manual', 'mlflow', 'telegram'],
) as training_dag:
    @task
    def ingest():
        from pipeline.tasks import ingest as run
        return run(get_current_context()['run_id'])

    @task
    def validate(data):
        from pipeline.tasks import validate as run
        return run(data)

    @task
    def split(data):
        from pipeline.tasks import split as run
        return run(data)

    @task(retries=0)
    def train(data):
        from pipeline.tasks import train as run
        return run(data, get_current_context()['params'])

    @task
    def evaluate(data):
        from pipeline.tasks import evaluate as run
        return run(data, get_current_context()['params'])

    @task.branch
    def quality_gate(result):
        return 'promote_and_reload' if result['gate']['passed'] else 'keep_current_model'

    @task
    def promote_and_reload(result):
        from pipeline.tasks import promote
        return promote(result)

    @task
    def publish_reference(result):
        from pipeline.tasks import publish_reference as run
        return run(result)

    result = evaluate(train(split(validate(ingest()))))
    branch = quality_gate(result)
    promoted = promote_and_reload(result)
    published = publish_reference(promoted)
    rejected = EmptyOperator(task_id='keep_current_model')
    branch >> [promoted, rejected]
    final = task(task_id='notify_result', trigger_rule=TriggerRule.ALL_DONE, retries=0)(finish_training)()
    [published, rejected] >> final


with DAG(
    'wine_drift_check', description='Manually analyze captured data and notify Telegram; never retrains',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'), schedule=None, catchup=False, max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    params={'window_size': Param(100, type='integer', minimum=30, maximum=10000),
            'drift_threshold': Param(.3, type='number', minimum=.01, maximum=1)},
    tags=['manual', 'drift', 'telegram'],
) as drift_dag:
    @task
    def analyze():
        from pipeline.tasks import analyze_drift
        return analyze_drift(get_current_context()['params'])

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def notify_drift():
        from pipeline.notifications import send_telegram
        context = get_current_context()
        result = context['ti'].xcom_pull(task_ids='analyze')
        success = any(t.task_id == 'analyze' and t.state == 'success' for t in context['dag_run'].get_task_instances())
        if success and result:
            status = 'DRIFT DETECTED' if result['drift_detected'] else 'NO DRIFT'
            text = (f"ML monitoring | {status}\nRun: {context['run_id']}\n"
                    f"Drifted features: {result['drifted_count']}/{result['total_features']}\n"
                    f"Samples: {result['current_samples']}\n"
                    f"Report: {os.getenv('EVIDENTLY_PUBLIC_URL', 'http://localhost:18011')}{result['report_url']}\n"
                    'Retraining requires a manual training DAG run; investigate and obtain labeled data first.')
        else:
            text = f"ML monitoring | DRIFT CHECK FAILED\nRun: {context['run_id']}\nCheck reference data, captured samples and Airflow logs."
        delivery = send_telegram(text)
        logging.info('Telegram notification status: %s', delivery)
        if not success:
            raise AirflowException('Drift analysis failed; notification status: ' + delivery)
        return {'telegram': delivery, 'drift_detected': result['drift_detected']}

    analyze() >> notify_drift()
