"""Readiness checks every 15 minutes; alert on transitions and hourly reminders."""
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context


with DAG(
    'service_health_check', description='Readiness of API, MLflow and Evidently; down/recovery notifications',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    schedule='*/15 * * * *' if os.getenv('MONITOR_SCHEDULES_ENABLED', 'true').lower() == 'true' else None,
    catchup=False, max_active_runs=1, dagrun_timeout=timedelta(minutes=3),
    tags=['scheduled', 'health', 'telegram'],
) as dag:
    @task(retries=0)
    def check(name, url):
        from pipeline.health import check_service
        return check_service(name, url)

    @task(retries=0)
    def report(results):
        from pipeline.health import report_health
        result = report_health(results, get_current_context()['run_id'])
        get_current_context()['ti'].xcom_push(key='health_result', value=result)
        if not result['healthy']:
            raise AirflowException('Services not ready: ' + ', '.join(result['down']))
        return result

    report([check.override(task_id='check_' + name)(name, os.getenv(env, default))
            for name, env, default in [('api', 'API_URL', 'http://api:8000'),
                                       ('mlflow', 'MLFLOW_TRACKING_URI', 'http://mlflow:5000'),
                                       ('evidently', 'EVIDENTLY_URL', 'http://evidently:8001')]])
