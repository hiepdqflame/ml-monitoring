from unittest.mock import patch

import pytest

pytest.importorskip('fastapi', reason='Serving tests run separately in the API image')
from fastapi.testclient import TestClient
from api.main import app, model_manager


def test_missing_model_preserves_503(monkeypatch):
    monkeypatch.setattr(model_manager, 'model', None)
    response = TestClient(app).post('/predict', json={'features': [1.0] * 13})
    assert response.status_code == 503


def test_wrong_feature_count_and_order_are_rejected(monkeypatch):
    monkeypatch.setattr(model_manager, 'model', object())
    client = TestClient(app)
    assert client.post('/predict', json={'features': [1.0] * 11}).status_code == 422
    assert client.post('/predict', json={'features': [1.0] * 13, 'feature_names': ['wrong'] * 13}).status_code == 422


def test_failed_reload_preserves_current_model(monkeypatch):
    current = object()
    monkeypatch.setattr(model_manager, 'model', current)
    monkeypatch.setattr(model_manager, 'model_version', '7')
    with patch('api.main.mlflow.tracking.MlflowClient', side_effect=RuntimeError('registry unavailable')):
        assert model_manager.load_model() is False
    assert model_manager.model is current
    assert model_manager.model_version == '7'
