"""Data and model lifecycle tasks; large artifacts stay outside Airflow XCom."""
import hashlib
import json
import os
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import pandas as pd
import requests
from mlflow.exceptions import MlflowException
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score

from pipeline.data import FEATURE_NAMES, load_dataset, split_dataset, validate_dataset
from pipeline.quality import quality_gate


def setup_mlflow():
    mlflow.set_tracking_uri(os.environ['MLFLOW_TRACKING_URI'])
    mlflow.set_experiment('wine_manual_pipeline')
    return mlflow.tracking.MlflowClient()


def production_version(client):
    try:
        versions = client.get_latest_versions(os.getenv('MODEL_NAME', 'wine_quality_model'), stages=['Production'])
    except MlflowException as error:
        if error.error_code == 'RESOURCE_DOES_NOT_EXIST':
            return None
        raise
    return versions[0].version if versions else None


def ingest(run_id, provenance=None):
    directory = Path(os.getenv('PIPELINE_DATA_DIR', '/opt/airflow/pipeline_data')) / hashlib.sha256(run_id.encode()).hexdigest()[:24]
    directory.mkdir(parents=True, exist_ok=True)
    load_dataset().to_csv(directory / 'raw.csv', index=False)
    return {'directory': str(directory), 'airflow_run_id': run_id, 'provenance': provenance or {}}


def validate(run):
    directory = Path(run['directory'])
    result = validate_dataset(pd.read_csv(directory / 'raw.csv'))
    (directory / 'validation.json').write_text(json.dumps(result, indent=2))
    return {**run, **result}


def split(run):
    directory = Path(run['directory'])
    train_frame, test_frame = split_dataset(pd.read_csv(directory / 'raw.csv'))
    train_frame.to_csv(directory / 'train.csv', index=False)
    test_frame.to_csv(directory / 'test.csv', index=False)
    return run


def train(run, params):
    setup_mlflow()
    directory = Path(run['directory'])
    frame = pd.read_csv(directory / 'train.csv')
    validation = json.loads((directory / 'validation.json').read_text())
    settings = {'n_estimators': int(params.get('n_estimators', 100)),
                'max_depth': int(params.get('max_depth', 10)), 'random_state': 42}
    model = RandomForestClassifier(**settings)
    model.fit(frame[FEATURE_NAMES], frame.target)
    with mlflow.start_run(run_name=run['airflow_run_id']) as active:
        mlflow.log_params({**settings, 'train_rows': len(frame), 'features': len(FEATURE_NAMES)})
        mlflow.set_tags({'airflow_run_id': run['airflow_run_id'], 'data_sha256': validation['sha256'],
                         'dataset': 'sklearn.datasets.load_wine', 'purpose': 'lifecycle-demo',
                         'trigger_source': run.get('provenance', {}).get('trigger_source', 'manual'),
                         'source_drift_run': run.get('provenance', {}).get('source_drift_run', ''),
                         'source_window': run.get('provenance', {}).get('source_window', ''),
                         'training_data_policy': 'built-in labeled Wine; not unlabeled inference data'})
        mlflow.sklearn.log_model(model, 'model', input_example=frame[FEATURE_NAMES].iloc[:3],
                                signature=mlflow.models.infer_signature(frame[FEATURE_NAMES], model.predict(frame[FEATURE_NAMES])),
                                pip_requirements=['scikit-learn==1.3.2', 'numpy==1.24.3',
                                                  'pandas==2.1.4', 'cloudpickle==2.2.1'])
        mlflow.log_artifact(str(directory / 'validation.json'))
        mlflow.log_artifact(str(directory / 'train.csv'), 'data')
        mlflow.log_artifact(str(directory / 'test.csv'), 'data')
        run_id = active.info.run_id
    joblib.dump(model, directory / 'candidate.joblib')
    return {**run, 'mlflow_run_id': run_id}


def scores(model, frame):
    predicted = model.predict(frame[FEATURE_NAMES])
    return {'accuracy': float(accuracy_score(frame.target, predicted)),
            'f1_macro': float(f1_score(frame.target, predicted, average='macro'))}


def evaluate(run, params):
    client = setup_mlflow()
    directory = Path(run['directory'])
    frame = pd.read_csv(directory / 'test.csv')
    candidate = scores(joblib.load(directory / 'candidate.joblib'), frame)
    previous = production_version(client)
    champion = None
    if previous:
        model = mlflow.pyfunc.load_model(f"models:/{os.getenv('MODEL_NAME', 'wine_quality_model')}/{previous}")
        champion = scores(model, frame)
    gate = quality_gate(candidate, champion, float(params.get('min_accuracy', .9)), float(params.get('min_f1', .9)))
    result = {**run, 'candidate': candidate, 'champion': champion, 'previous_version': previous, 'gate': gate}
    (directory / 'evaluation.json').write_text(json.dumps(result, indent=2))
    with mlflow.start_run(run_id=run['mlflow_run_id']):
        mlflow.log_metrics(candidate)
        mlflow.set_tag('quality_gate', 'passed' if gate['passed'] else 'rejected')
        mlflow.log_artifact(str(directory / 'evaluation.json'))
    return result


def promote(result):
    if not result['gate']['passed']:
        raise ValueError('Cannot promote a model that failed the quality gate')
    client = setup_mlflow()
    name = os.getenv('MODEL_NAME', 'wine_quality_model')
    existing = [v for v in client.search_model_versions(f"name='{name}'") if v.run_id == result['mlflow_run_id']]
    version = existing[0].version if existing else mlflow.register_model('runs:/' + result['mlflow_run_id'] + '/model', name).version
    client.transition_model_version_stage(name, version, 'Production', archive_existing_versions=True)
    try:
        response = requests.post(os.getenv('API_URL', 'http://api:8000') + '/model/reload', timeout=120)
        response.raise_for_status()
        if str(response.json()['model_version']) != str(version):
            raise RuntimeError('Serving API loaded an unexpected version')
    except Exception:
        client.transition_model_version_stage(name, version, 'Archived')
        if result['previous_version']:
            client.transition_model_version_stage(name, result['previous_version'], 'Production', archive_existing_versions=True)
            recovery = requests.post(os.getenv('API_URL', 'http://api:8000') + '/model/reload', timeout=120)
            recovery.raise_for_status()
        raise
    client.set_model_version_tag(name, version, 'airflow_run_id', result['airflow_run_id'])
    return {**result, 'model_version': str(version)}


def publish_reference(result):
    frame = pd.read_csv(Path(result['directory']) / 'train.csv')[FEATURE_NAMES]
    response = requests.post(os.getenv('EVIDENTLY_URL', 'http://evidently:8001') + '/reference', json={
        'data': frame.to_dict('records'), 'feature_names': FEATURE_NAMES,
        'description': f"Wine training split only; model version {result['model_version']}; run {result['mlflow_run_id']}",
    }, timeout=60)
    response.raise_for_status()
    return result


def analyze_drift(params):
    from pipeline.automation import AutomationStore
    base = os.getenv('EVIDENTLY_URL', 'http://evidently:8001')
    store = AutomationStore()
    status = requests.get(base + '/window-status', params={'window_size': int(params.get('window_size', 100))}, timeout=15)
    status.raise_for_status()
    reason = store.analysis_reason(status.json())
    if reason in ('ready', 'unchanged_window', 'insufficient_new_samples'):
        pending = store.recover_analysis(status.json())
        if pending:
            return pending
    if reason != 'ready':
        return {'status': 'skipped', 'reason': reason, **status.json()}
    response = requests.post(base + '/analyze', json={
        'window_size': int(params.get('window_size', 100)), 'threshold': float(params.get('drift_threshold', .3)),
    }, timeout=120)
    response.raise_for_status()
    result = response.json()
    store.record_analysis(result)
    return result
