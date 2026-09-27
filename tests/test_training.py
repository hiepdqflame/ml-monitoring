import importlib

import pytest


def test_real_training_logs_model_and_gate_rejects_impossible_score(tmp_path, monkeypatch):
    pytest.importorskip('mlflow')
    try:
        tasks = importlib.import_module('pipeline.tasks')
    except ModuleNotFoundError:
        pytest.fail('Training pipeline not implemented')
    monkeypatch.setenv('MLFLOW_TRACKING_URI', 'sqlite:///' + str(tmp_path / 'mlflow.db'))
    monkeypatch.setenv('PIPELINE_DATA_DIR', str(tmp_path / 'runs'))
    monkeypatch.setenv('MODEL_NAME', 'test_wine')
    monkeypatch.chdir(tmp_path)
    run = tasks.ingest('test-run')
    tasks.validate(run)
    tasks.split(run)
    trained = tasks.train(run, {'n_estimators': 10, 'max_depth': 1})
    result = tasks.evaluate(trained, {'min_accuracy': 1.0, 'min_f1': 1.0})
    assert result['gate']['passed'] is False
    assert 0.85 <= result['candidate']['accuracy'] < 1.0
    import mlflow
    logged = mlflow.tracking.MlflowClient().get_run(trained['mlflow_run_id'])
    assert logged.data.metrics['accuracy'] == result['candidate']['accuracy']
    assert logged.data.tags['data_sha256']
    model = mlflow.pyfunc.load_model('runs:/' + trained['mlflow_run_id'] + '/model')
    from pipeline.data import FEATURE_NAMES, load_dataset
    assert len(model.predict(load_dataset()[FEATURE_NAMES].iloc[:2])) == 2
