FROM apache/airflow:2.10.5-python3.11
COPY --chown=airflow:root requirements.txt /tmp/project-requirements.txt
COPY --chown=airflow:root airflow/requirements-airflow.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
ENV PYTHONPATH=/opt/airflow/project
