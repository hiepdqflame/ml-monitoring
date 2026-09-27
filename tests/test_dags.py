from airflow.models import DagBag


def test_dags_are_manual_and_promotion_has_a_gate():
    bag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)
    assert not bag.import_errors, bag.import_errors
    training = bag.dags['wine_mlflow_pipeline']
    drift = bag.dags['wine_drift_check']
    for dag in [training, drift]:
        assert dag.schedule_interval is None
        assert dag.catchup is False
        assert dag.max_active_runs == 1
    assert 'quality_gate' in training.get_task('promote_and_reload').upstream_task_ids
    assert {t.task_id for t in training.leaves} == {'notify_result'}
    assert training.get_task('notify_result').trigger_rule == 'all_done'
    assert 'analyze' in drift.get_task('notify_drift').upstream_task_ids
