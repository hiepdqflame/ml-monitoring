"""Write optional Telegram configuration to a private Docker volume, never stdout."""
import json
import os
from pathlib import Path


def render(directory, env):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    receiver = {'name': 'local'}
    token = ''
    if env.get('TELEGRAM_ENABLED', 'false').lower() == 'true':
        token = env.get('TELEGRAM_BOT_TOKEN', '').strip()
        try:
            chat = int(env.get('TELEGRAM_CHAT_ID', ''))
        except ValueError:
            raise ValueError('Valid Telegram credentials are required when enabled') from None
        if not token or not chat:
            raise ValueError('Valid Telegram credentials are required when enabled')
        receiver = {'name': 'telegram', 'telegram_configs': [{
            'bot_token_file': '/etc/alertmanager/private/telegram-token', 'chat_id': chat,
            'send_resolved': True, 'parse_mode': 'HTML',
            'message': '{{ range .Alerts }}[{{ .Status | toUpper }}] {{ .Labels.alertname }}\n{{ .Annotations.summary }}\n{{ end }}',
        }]}
    config = {
        'global': {'resolve_timeout': '5m'},
        'route': {'receiver': receiver['name'], 'group_by': ['alertname', 'component'],
                  'group_wait': '15s', 'group_interval': '1m', 'repeat_interval': '4h'},
        'inhibit_rules': [{'source_matchers': ['alertname="APIDown"'],
                           'target_matchers': ['component="api"', 'alertname!="APIDown"'],
                           'equal': ['component']}],
        'receivers': [receiver],
    }
    for name, content in [('telegram-token', token), ('alertmanager.yml', json.dumps(config, indent=2))]:
        path = directory / name
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as output:
            output.write(content)
        path.chmod(0o600)
        if os.geteuid() == 0:
            os.chown(path, 65534, 65534)
    return receiver['name']


if __name__ == '__main__':
    receiver = render('/etc/alertmanager/private', os.environ)
    print('Alertmanager private configuration ready; receiver:', receiver)
