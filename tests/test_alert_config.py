import importlib
import json

import pytest


def renderer():
    try:
        return importlib.import_module('scripts.render_alertmanager').render
    except ImportError:
        pytest.fail('Optional Telegram Alertmanager configuration is not implemented')


def test_alertmanager_works_without_telegram(tmp_path):
    renderer()(tmp_path, {})
    config = json.loads((tmp_path / 'alertmanager.yml').read_text())
    assert config['route']['receiver'] == 'local'
    assert config['receivers'] == [{'name': 'local'}]


def test_telegram_token_is_only_in_private_token_file(tmp_path):
    renderer()(tmp_path, {'TELEGRAM_ENABLED': 'true', 'TELEGRAM_BOT_TOKEN': '123:test-only',
                          'TELEGRAM_CHAT_ID': '-100123'})
    text = (tmp_path / 'alertmanager.yml').read_text()
    assert 'test-only' not in text
    receiver = json.loads(text)['receivers'][0]['telegram_configs'][0]
    assert receiver['chat_id'] == -100123 and receiver['send_resolved'] is True
    assert (tmp_path / 'telegram-token').read_text() == '123:test-only'
    assert (tmp_path / 'telegram-token').stat().st_mode & 0o777 == 0o600


def test_invalid_enabled_credentials_fail_before_start(tmp_path):
    with pytest.raises(ValueError, match='credentials'):
        renderer()(tmp_path, {'TELEGRAM_ENABLED': 'true', 'TELEGRAM_CHAT_ID': 'not-a-number'})
