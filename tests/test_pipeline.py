import importlib
import io
import json
from urllib.error import HTTPError
from unittest.mock import patch

import numpy as np
import pytest


def module(name):
    try:
        return importlib.import_module('pipeline.' + name)
    except ModuleNotFoundError:
        pytest.fail('Pipeline component not implemented: ' + name)


def test_wine_split_preserves_schema_and_excludes_holdout_from_training():
    data = module('data')
    frame = data.load_dataset()
    train, test = data.split_dataset(frame)
    assert frame.shape == (178, 14)
    assert len(train) == 142 and len(test) == 36
    assert set(train.index).isdisjoint(test.index)
    assert set(train['target']) == {0, 1, 2}
    assert set(test['target']) == {0, 1, 2}
    assert list(frame.columns[:3]) == ['alcohol', 'malic_acid', 'ash']


@pytest.mark.parametrize('bad', ['missing_feature', 'nan', 'invalid_label', 'duplicate'])
def test_invalid_data_is_rejected_before_training(bad):
    data = module('data')
    frame = data.load_dataset()
    if bad == 'missing_feature':
        frame = frame.drop(columns=['alcohol'])
    elif bad == 'nan':
        frame.loc[0, 'alcohol'] = np.nan
    elif bad == 'invalid_label':
        frame.loc[0, 'target'] = 99
    else:
        frame.loc[1] = frame.loc[0]
    with pytest.raises(ValueError):
        data.validate_dataset(frame)


@pytest.mark.parametrize('candidate,champion,expected', [
    ({'accuracy': .95, 'f1_macro': .94}, None, True),
    ({'accuracy': .89, 'f1_macro': .95}, None, False),
    ({'accuracy': .95, 'f1_macro': .89}, None, False),
    ({'accuracy': .95, 'f1_macro': .94}, {'accuracy': .96, 'f1_macro': .94}, False),
    ({'accuracy': .95, 'f1_macro': .94}, {'accuracy': .95, 'f1_macro': .95}, False),
    ({'accuracy': .9, 'f1_macro': .9}, {'accuracy': .9, 'f1_macro': .9}, True),
    ({'accuracy': float('nan'), 'f1_macro': 1}, None, False),
])
def test_gate_rejects_low_quality_nonfinite_and_regressions(candidate, champion, expected):
    assert module('quality').quality_gate(candidate, champion)['passed'] is expected


def test_failure_status_is_not_hidden_by_successful_notification():
    quality = module('quality')
    assert quality.pipeline_status(['success', 'failed', 'upstream_failed']) == 'FAILED'
    assert quality.pipeline_status(['success', 'skipped'], promoted=False) == 'REJECTED'
    assert quality.pipeline_status(['success', 'skipped'], promoted=True) == 'PROMOTED'


def test_telegram_disabled_does_not_send():
    with patch('urllib.request.urlopen', side_effect=AssertionError('Unexpected network')):
        assert module('notifications').send_telegram('demo', {}) == 'disabled'


def test_telegram_enabled_requires_both_credentials():
    notify = module('notifications')
    with pytest.raises(notify.TelegramError):
        notify.send_telegram('demo', {'TELEGRAM_ENABLED': 'true'})


def test_telegram_validates_response_and_sends_plain_text():
    notify = module('notifications')
    env = {'TELEGRAM_ENABLED': 'true', 'TELEGRAM_BOT_TOKEN': '123:secret', 'TELEGRAM_CHAT_ID': '456'}
    class Reply:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return b'{"ok":true,"result":{"message_id":7}}'
    with patch('urllib.request.urlopen', return_value=Reply()) as http:
        assert notify.send_telegram('Model <v1> passed', env) == 'delivered'
        payload = json.loads(http.call_args.args[0].data)
        assert payload['chat_id'] == '456'
        assert payload['text'] == 'Model <v1> passed'
        assert 'parse_mode' not in payload
    with patch('urllib.request.urlopen', side_effect=OSError('https://api.telegram.org/bot123:secret')):
        with pytest.raises(notify.TelegramError) as exc:
            notify.send_telegram('demo', env)
        assert 'secret' not in str(exc.value)


def test_telegram_api_rejection_is_not_delivery():
    notify = module('notifications')
    class Reply:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return b'{"ok":false,"description":"bad token"}'
    with patch('urllib.request.urlopen', return_value=Reply()):
        with pytest.raises(notify.TelegramError):
            notify.send_telegram('demo', {'TELEGRAM_ENABLED': 'true', 'TELEGRAM_BOT_TOKEN': 'x', 'TELEGRAM_CHAT_ID': '1'})


@pytest.mark.parametrize('code,description,hint', [
    (400, 'Bad Request: chat not found', '/start'),
    (403, 'Forbidden: bot was blocked by the user', 'Unblock'),
    (403, "Forbidden: bots can't send messages to bots", 'personal'),
    (409, "Conflict: can't use getUpdates method while webhook is active", 'webhook'),
])
def test_telegram_http_errors_show_reason_and_action_without_token(code, description, hint):
    notify = module('notifications')
    token = '123:private-token'
    body = json.dumps({'ok': False, 'error_code': code, 'description': description + ' ' + token}).encode()
    error = HTTPError('https://api.telegram.org/bot' + token, code, 'Rejected', {}, io.BytesIO(body))
    with patch('urllib.request.urlopen', side_effect=error):
        with pytest.raises(notify.TelegramError) as exc:
            notify.telegram_request('sendMessage', token, {'chat_id': '456', 'text': 'demo'})
    message = str(exc.value)
    assert str(code) in message and description in message and hint in message
    assert token not in message


def test_bot_id_is_not_accepted_as_destination():
    from scripts import configure_telegram
    with patch.object(configure_telegram.getpass, 'getpass', return_value='123:fake'), \
         patch.object(configure_telegram, 'telegram_request', return_value={'id': 123, 'username': 'demo_bot'}), \
         patch('builtins.input', return_value='123'), \
         patch.object(configure_telegram, 'send_telegram', side_effect=AssertionError('Must not send to bot itself')):
        with pytest.raises(ValueError, match='bot ID'):
            configure_telegram.main()
