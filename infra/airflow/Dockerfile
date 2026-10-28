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

# Пакеты dbt запекаются в образ вне bind-mount репозитория.
RUN mkdir -p /opt/dbt-deps \
    && printf '%s\n' 'name: dbt_deps' 'version: "1.0.0"' 'config-version: 2' \
        'packages-install-path: /opt/dbt-packages' > /opt/dbt-deps/dbt_project.yml
COPY --from=dbt packages.yml /opt/dbt-deps/packages.yml
RUN dbt deps --project-dir /opt/dbt-deps \
    && chmod -R a+rX /opt/dbt-packages
ENV DBT_PACKAGES_INSTALL_PATH=/opt/dbt-packages
USER airflow
