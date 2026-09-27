FROM apache/airflow:slim-3.3.2-python3.12
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
COPY --chown=airflow:root docker/constraints-3.3.2-python3.12.txt /tmp/constraints.txt
RUN python -m pip install --no-cache-dir "apache-airflow==3.3.2" \
    "apache-airflow-providers-standard==1.19.0" "psycopg2-binary==2.9.13" \
    "asyncpg==0.31.0" "statsd==4.0.1" \
    -r /tmp/requirements.txt --constraint /tmp/constraints.txt \
    && python -m pip check
COPY --chown=airflow:root pipeline /opt/airflow/pipeline
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root sql /opt/airflow/sql
ENV PYTHONPATH=/opt/airflow
