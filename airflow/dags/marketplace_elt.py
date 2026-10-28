"""ELT маркетплейса: OLTP -> Cloudberry raw -> dbt.
Core / марты / ML / загрузка в ClickHouse — добавятся после моделирования."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from pipelines.oltp_to_raw import TABLES

DBT = "cd /opt/project/dbt && dbt"


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["marketplace", "elt"],
)
def marketplace_elt():
    @task
    def extract_load(table: str) -> int:
        from pipelines.oltp_to_raw import load_table

        return load_table(table)

    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build")

    extract_load.expand(table=list(TABLES)) >> dbt_build


marketplace_elt()
