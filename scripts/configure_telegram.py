"""Run locally: enter the bot token privately, discover chat, send a test, save .env."""
import getpass
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.notifications import TelegramError, send_telegram, telegram_request


def main():
    token = getpass.getpass('Telegram bot token (hidden): ').strip()
    if not re.fullmatch(r'\d+:[A-Za-z0-9_-]+', token):
        raise ValueError('Invalid bot token format')
    bot = telegram_request('getMe', token, {})
    print('Connected to bot @' + bot['username'])
    print('For personal messages: open this bot in Telegram and send /start first.')
    chat = input('Chat ID (Enter to find your /start message): ').strip()
    if not chat:
        updates = telegram_request('getUpdates', token, {'timeout': 0})
        chats = {}
        for item in updates:
            message = item.get('message', item.get('channel_post', {}))
            info = message.get('chat', {})
            if 'id' in info:
                chats[str(info['id'])] = info.get('title', info.get('first_name', 'Chat'))
        if not chats:
            raise ValueError('Open your bot, send /start, then run this script again; or enter the chat ID directly.')
        for ident, name in chats.items():
            print(ident + ': ' + name)
        chat = next(iter(chats)) if len(chats) == 1 else input('Choose chat ID: ').strip()
    if not re.fullmatch(r'-?\d+', chat):
        raise ValueError('Chat ID must be an integer')
    if chat == str(bot['id']):
        raise ValueError('You entered the bot ID. Send /start to your bot, run this script again, and press Enter at Chat ID to find your personal chat.')
    settings = {'TELEGRAM_ENABLED': 'true', 'TELEGRAM_BOT_TOKEN': token, 'TELEGRAM_CHAT_ID': chat}
    send_telegram('ML monitoring: Telegram connection verified. Manual Airflow pipeline notifications are ready.', settings)
    path = ROOT / '.env'
    lines = path.read_text().splitlines() if path.exists() else []
    lines = [line for line in lines if line.split('=', 1)[0].strip() not in settings]
    lines.extend(key + '=' + value for key, value in settings.items())
    path.write_text('\n'.join(lines) + '\n')
    os.chmod(path, 0o600)
    print('Test delivered. Saved credentials to local .env (not printed).')
    print('Apply settings: docker compose up -d --no-deps --force-recreate airflow-webserver airflow-scheduler')


if __name__ == '__main__':
    try:
        main()
    except (TelegramError, ValueError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
