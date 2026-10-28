ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-postgres \
    clickhouse-connect \
    faker

# dbt живёт в отдельном venv: его зависимости конфликтуют с constraints Airflow.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.10.23" "dbt-postgres==1.10.2" \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt
USER airflow
