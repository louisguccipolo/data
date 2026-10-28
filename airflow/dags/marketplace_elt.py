"""ELT маркетплейса: source → DWH raw → dbt staging / ODS / DDS / mart → ClickHouse.

Один батч на все таблицы; dbt build подхватывает seeds и весь граф моделей.
После тестов публикуем витрины и refresh_status. ML и Superset — следующие этапы.
"""

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

    @task
    def publish_clickhouse(batch: dict) -> dict:
        from pipelines.mart_to_clickhouse import publish

        return publish(batch["batch_id"])

    batch = open_batch()
    extract_load.partial(batch=batch).expand(table=list(TABLES)) >> dbt_build
    dbt_build >> publish_clickhouse(batch)


marketplace_elt()
