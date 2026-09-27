FROM apache/airflow:2.10.5-python3.10
USER airflow
RUN pip install --no-cache-dir apache-airflow==2.10.5 mlflow==2.17.2 \
    scikit-learn==1.3.2 numpy==1.24.3 pandas==2.1.4 boto3==1.36.3 \
    pytest==8.3.5 colorama==0.4.6 cloudpickle==2.2.1 && pip check
RUN pip install --no-cache-dir tqdm==4.66.1 && pip check
COPY --chown=airflow:root pipeline /opt/airflow/pipeline
COPY --chown=airflow:root airflow_dags /opt/airflow/dags
COPY --chown=airflow:root scripts /opt/airflow/scripts
COPY --chown=airflow:root tests /opt/airflow/tests
COPY --chown=airflow:root simulations /opt/airflow/simulations
COPY --chown=airflow:root api /opt/airflow/api
COPY --chown=airflow:root evidently/window_metadata.py /opt/airflow/evidently/window_metadata.py
RUN mkdir -p /opt/airflow/pipeline_data
ENV PYTHONPATH=/opt/airflow
