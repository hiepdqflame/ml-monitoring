"""Service readiness checks and state-change notifications."""
import requests

from pipeline.automation import AutomationStore
from pipeline.notifications import send_telegram


def check_service(name, url):
    try:
        response = requests.get(url.rstrip('/') + '/health', timeout=10)
        healthy = response.status_code == 200
        detail = 'HTTP ' + str(response.status_code)
        if name != 'mlflow' and healthy:
            body = response.json()
            if not isinstance(body, dict):
                raise ValueError('Health body must be an object')
            healthy = body.get('status') == 'healthy'
            if name == 'api':
                healthy = healthy and body.get('model_loaded') is True
            detail += ', ready=' + str(healthy)
        return {'service': name, 'healthy': healthy, 'detail': detail}
    except (requests.RequestException, ValueError):
        return {'service': name, 'healthy': False, 'detail': 'Unreachable or invalid health response'}


def report_health(results, run_id):
    down = [r['service'] for r in results if not r['healthy']]
    store = AutomationStore()
    notification = store.health_notification(down)
    delivery = 'suppressed'
    if notification:
        text = (f"ML monitoring | HEALTH {notification['status']}\nRun: {run_id}\n" +
                '\n'.join(f"{r['service']}: {r['detail']}" for r in results))
        delivery = send_telegram(text)
        # A failed delivery leaves the transition pending for the next check.
        store.record_health(down)
    return {'healthy': not down, 'down': down, 'telegram': delivery, 'services': results}
