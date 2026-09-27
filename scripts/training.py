"""Compatibility entry point: submit the manually triggered, quality-gated DAG."""
from pathlib import Path
import subprocess

if __name__ == '__main__':
    subprocess.run(['docker', 'compose', 'exec', '-T', 'airflow-scheduler',
                    'airflow', 'dags', 'trigger', 'wine_mlflow_pipeline'],
                   cwd=Path(__file__).resolve().parents[1], check=True)
