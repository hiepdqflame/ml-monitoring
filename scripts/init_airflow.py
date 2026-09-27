"""Idempotent local Airflow metadata initialization."""
import os
import subprocess

subprocess.run(['airflow', 'db', 'migrate'], check=True)
from airflow.www.app import create_app

app = create_app()
with app.app_context():
    security = app.appbuilder.sm
    username = os.environ.get('AIRFLOW_ADMIN_USER', 'admin')
    if not security.find_user(username=username):
        user = security.add_user(
            username=username, first_name='Demo', last_name='Admin',
            email='admin@example.invalid', role=security.find_role('Admin'),
            password=os.environ['AIRFLOW_ADMIN_PASSWORD'],
        )
        if not user:
            raise RuntimeError('Could not create Airflow administrator')
print('Airflow database and administrator ready')
