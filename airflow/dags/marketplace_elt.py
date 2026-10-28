"""ELT маркетплейса: source → DWH raw → dbt, один батч на все таблицы.
ODS / DDS / марты / ML / загрузка в ClickHouse — добавятся после моделирования."""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task

from pipelines.source_to_raw import TABLES

DBT = "cd /opt/project/dbt && dbt"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["marketplace", "elt"],
)
def marketplace_elt():
    @task
    def open_batch() -> dict:
        return {
            "batch_id": get_current_context()["run_id"],
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    @task
    def extract_load(table: str, batch: dict) -> dict:
        from pipelines.source_to_raw import load_table

        return load_table(
            table, batch["batch_id"], datetime.fromisoformat(batch["extracted_at"])
        )

    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build")

    batch = open_batch()
    extract_load.partial(batch=batch).expand(table=list(TABLES)) >> dbt_build


marketplace_elt()
