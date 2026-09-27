"""Telegram notifications without token-bearing exceptions or HTML parsing."""
import json
import os
from urllib.error import HTTPError
from urllib.parse import quote
import urllib.request


class TelegramError(RuntimeError):
    pass


def _api_error(method, token, body, status='unknown'):
    description = str(body.get('description', 'Request rejected'))
    for secret in (token, quote(token, safe='')):
        if secret:
            description = description.replace(secret, '[REDACTED]')
    description = ' '.join(description.split())[:500]
    reason = description.lower()
    hint = ''
    if 'chat not found' in reason:
        hint = 'Open your bot, send /start, then press Enter at the Chat ID prompt to discover your chat.'
    elif 'blocked' in reason:
        hint = 'Unblock the bot in Telegram and send /start again.'
    elif "can't send messages to bots" in reason:
        hint = 'Use your personal chat or group ID, not the bot ID.'
    elif 'webhook' in reason:
        hint = 'This bot has an active webhook; enter the chat ID directly or use a dedicated demo bot.'
    return TelegramError(f"Telegram {method} failed ({body.get('error_code', status)}): {description}. {hint}".strip())


def telegram_request(method, token, payload):
    request = urllib.request.Request(
        'https://api.telegram.org/bot' + token + '/' + method,
        data=json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json'}, method='POST',
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = json.loads(response.read())
    except HTTPError as error:
        try:
            body = json.loads(error.read(8192))
            if not isinstance(body, dict):
                body = {}
        except Exception:
            body = {}
        raise _api_error(method, token, body, error.code) from None
    except Exception:
        # HTTP exceptions include the URL, which embeds the private bot token.
        raise TelegramError('Telegram connection or response failed; check connectivity and try again.') from None
    if not isinstance(body, dict):
        raise TelegramError('Telegram returned an invalid response.')
    if body.get('ok') is not True:
        raise _api_error(method, token, body)
    return body.get('result')


def send_telegram(text, environ=None):
    env = os.environ if environ is None else environ
    if env.get('TELEGRAM_ENABLED', 'false').lower() != 'true':
        return 'disabled'
    token = env.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat = env.get('TELEGRAM_CHAT_ID', '').strip()
    if not token or not chat:
        raise TelegramError('Telegram is enabled but bot token or chat ID is missing.')
    telegram_request('sendMessage', token, {'chat_id': chat, 'text': text[:3900],
                                           'disable_web_page_preview': True})
    return 'delivered'
