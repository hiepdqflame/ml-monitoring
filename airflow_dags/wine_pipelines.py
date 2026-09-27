"""Scheduled monitoring and guarded retraining of the labeled Wine demo dataset."""
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

SCHEDULES_ENABLED = os.getenv('MONITOR_SCHEDULES_ENABLED', 'true').lower() == 'true'


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
            f"MLflow: {os.getenv('MLFLOW_PUBLIC_URL', 'http://localhost:15000')}\nMLflow run: {run_id}\n"
            f"Trigger: {result.get('provenance', {}).get('trigger_source', 'manual')}\n"
            'Training data: labeled built-in Wine (lifecycle demo, not drift remediation).')
    delivery = send_telegram(text)
    logging.info('Telegram notification status: %s', delivery)
    if status == 'FAILED':
        raise AirflowException('Pipeline failed; see upstream task logs. Notification status: ' + delivery)
    return {'pipeline_status': status, 'telegram': delivery, 'model_version': deployed.get('model_version')}


with DAG(
    'wine_mlflow_pipeline', description='Manual or drift-triggered training with quality gate and rollback',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'), schedule=None, catchup=False, max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20), default_args={'retries': 1, 'retry_delay': timedelta(seconds=20)},
    params={'n_estimators': Param(100, type='integer', minimum=10, maximum=500),
            'max_depth': Param(10, type='integer', minimum=1, maximum=30),
            'min_accuracy': Param(.9, type='number', minimum=0, maximum=1),
            'min_f1': Param(.9, type='number', minimum=0, maximum=1)},
    tags=['training', 'quality-gate', 'mlflow', 'telegram'],
) as training_dag:
    @task
    def ingest():
        from pipeline.tasks import ingest as run
        context = get_current_context()
        conf = context['dag_run'].conf or {}
        provenance = {key: str(conf.get(key, ''))[:250] for key in
                      ('trigger_source', 'source_drift_run', 'source_window')}
        provenance['trigger_source'] = provenance['trigger_source'] or 'manual'
        return run(context['run_id'], provenance)

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
    'wine_drift_check', description='Hourly drift analysis with deduplication, cooldown and retrain quota',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    schedule='@hourly' if SCHEDULES_ENABLED else None, catchup=False, max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    params={'window_size': Param(100, type='integer', minimum=30, maximum=10000),
            'drift_threshold': Param(.3, type='number', minimum=.01, maximum=1)},
    tags=['scheduled', 'drift', 'telegram'],
) as drift_dag:
    @task
    def analyze():
        from pipeline.tasks import analyze_drift
        return analyze_drift(get_current_context()['params'])

    @task.branch(retries=1, retry_delay=timedelta(seconds=20))
    def decide_retrain(result):
        from airflow.models import DagModel, DagRun
        from airflow.utils.session import create_session
        from pipeline.automation import AutomationStore
        context = get_current_context()
        with create_session() as session:
            target = session.query(DagModel).filter(DagModel.dag_id == 'wine_mlflow_pipeline').first()
            available = target is not None and not target.is_paused and target.is_active
            busy = session.query(DagRun).filter(DagRun.dag_id == 'wine_mlflow_pipeline',
                                               DagRun.state.in_(['queued', 'running'])).first() is not None
        store = AutomationStore()
        decision = store.reserve_retrain(
            result, context['run_id'], training_busy=busy, training_available=available,
            enabled=os.getenv('AUTO_RETRAIN_ENABLED', 'true').lower() == 'true',
            cooldown=max(0, int(os.getenv('RETRAIN_COOLDOWN_SECONDS', '21600'))),
            daily_limit=max(1, int(os.getenv('RETRAIN_DAILY_LIMIT', '3'))),
        )
        store.record_decision(result.get('window_id'), decision)
        context['ti'].xcom_push(key='decision', value=decision)
        logging.info('Retrain decision: %s', decision)
        return 'trigger_training' if decision['trigger'] else 'no_retrain'

    trigger = TriggerDagRunOperator(
        task_id='trigger_training', trigger_dag_id='wine_mlflow_pipeline',
        trigger_run_id="{{ ti.xcom_pull(task_ids='decide_retrain', key='decision')['run_id'] }}",
        conf={'trigger_source': 'drift', 'source_drift_run': '{{ run_id }}',
              'source_window': "{{ ti.xcom_pull(task_ids='decide_retrain', key='decision')['window_id'] }}"},
        wait_for_completion=False, skip_when_already_exists=True,
        retries=1, retry_delay=timedelta(seconds=20),
    )
    no_retrain = EmptyOperator(task_id='no_retrain')

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def notify_drift():
        from pipeline.notifications import send_telegram
        context = get_current_context()
        result = context['ti'].xcom_pull(task_ids='analyze') or {}
        decision = context['ti'].xcom_pull(task_ids='decide_retrain', key='decision') or {}
        instances = context['dag_run'].get_task_instances()
        failed = [t.task_id for t in instances if t.task_id != 'notify_drift' and t.state in ('failed', 'upstream_failed')]
        success = not failed and bool(result)
        if success and decision.get('trigger'):
            from pipeline.automation import AutomationStore
            AutomationStore().complete_dispatch(decision['window_id'])
        if success and result.get('status') == 'skipped':
            return {'status': 'skipped', 'reason': result['reason'], 'telegram': 'suppressed', 'retrain': decision}
        if success and result:
            status = 'DRIFT DETECTED' if result['drift_detected'] else 'NO DRIFT'
            text = (f"ML monitoring | {status}\nRun: {context['run_id']}\n"
                    f"Drifted features: {result['drifted_count']}/{result['total_features']}\n"
                    f"Samples: {result['current_samples']}\n"
                    f"Report: {os.getenv('EVIDENTLY_PUBLIC_URL', 'http://localhost:18011')}{result['report_url']}\n"
                    f"Retrain decision: {decision.get('reason')}\nChild run: {decision.get('run_id', 'none')}\n"
                    'Auto training reuses labeled Wine for demonstration; it does not learn unlabeled drift data.')
        else:
            text = f"ML monitoring | DRIFT CHECK FAILED\nRun: {context['run_id']}\nFailed tasks: {failed}"
        delivery = send_telegram(text)
        logging.info('Telegram notification status: %s', delivery)
        if not success:
            raise AirflowException('Drift analysis failed; notification status: ' + delivery)
        return {'status': 'analyzed', 'telegram': delivery, 'drift_detected': result['drift_detected'],
                'retrain': decision, 'report_url': result['report_url']}

    branch = decide_retrain(analyze())
    branch >> [trigger, no_retrain]
    [trigger, no_retrain] >> notify_drift()
