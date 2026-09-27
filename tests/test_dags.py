from airflow.models import DagBag


def test_schedules_and_promotion_have_safety_gates():
    bag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)
    assert not bag.import_errors, bag.import_errors
    training = bag.dags['wine_mlflow_pipeline']
    drift = bag.dags['wine_drift_check']
    health = bag.dags['service_health_check']
    assert training.schedule_interval is None
    assert drift.schedule_interval == '@hourly'
    assert health.schedule_interval == '*/15 * * * *'
    for dag in [training, drift, health]:
        assert dag.catchup is False
        assert dag.max_active_runs == 1
    assert 'quality_gate' in training.get_task('promote_and_reload').upstream_task_ids
    assert {t.task_id for t in training.leaves} == {'notify_result'}
    assert training.get_task('notify_result').trigger_rule == 'all_done'
    assert 'decide_retrain' in drift.get_task('trigger_training').upstream_task_ids
    assert drift.get_task('trigger_training').trigger_dag_id == training.dag_id
    assert drift.get_task('trigger_training').skip_when_already_exists is True
    assert {t.task_id for t in drift.leaves} == {'notify_drift'}
    assert drift.get_task('notify_drift').trigger_rule == 'all_done'
