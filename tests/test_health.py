from unittest.mock import Mock, patch

import pytest

from pipeline.automation import AutomationStore
from pipeline.health import check_service, report_health
from pipeline.notifications import TelegramError


def test_api_http_200_without_model_is_not_ready():
    response = Mock(status_code=200)
    response.json.return_value = {'status': 'healthy', 'model_loaded': False}
    with patch('pipeline.health.requests.get', return_value=response):
        assert check_service('api', 'http://api:8000')['healthy'] is False


def test_malformed_health_body_is_reported_as_down():
    response = Mock(status_code=200)
    response.json.return_value = []
    with patch('pipeline.health.requests.get', return_value=response):
        assert check_service('api', 'http://api:8000')['healthy'] is False


def test_failed_notification_does_not_consume_health_transition(tmp_path, monkeypatch):
    monkeypatch.setenv('PIPELINE_DATA_DIR', str(tmp_path))
    result = [{'service': 'api', 'healthy': False, 'detail': 'Unreachable'}]
    with patch('pipeline.health.send_telegram', side_effect=TelegramError('unreachable')):
        with pytest.raises(TelegramError):
            report_health(result, 'demo')
    assert AutomationStore().health_notification(['api'])['status'] == 'DOWN'
    with patch('pipeline.health.send_telegram', return_value='delivered'):
        assert report_health(result, 'demo')['healthy'] is False
    assert AutomationStore().health_notification(['api']) is None
