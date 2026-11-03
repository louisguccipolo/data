"""ELT маркетплейса: source → DWH raw → dbt staging / ODS / DDS / mart → ClickHouse.

Один батч на все таблицы; dbt build подхватывает seeds и весь граф моделей.
Время истории — logical_date; для ручного запуска без неё — run_after.
Повтор задачи сохраняет дату батча. Прошлые даты требуют отдельного replay.
После тестов публикуем витрины и refresh_status. ML и Superset — следующие этапы.
"""

from datetime import datetime, timedelta

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
        import os
        import psycopg2
        from contextlib import closing
        from airflow.exceptions import AirflowFailException
        from pipelines.batch_time import airflow_batch, check_batch_order

        try:
            batch = airflow_batch(get_current_context())
            with closing(psycopg2.connect(os.environ["DWH_DSN"])) as conn:
                conn.set_session(readonly=True)
                check_batch_order(conn, batch)
        except ValueError as exc:
            raise AirflowFailException(str(exc)) from exc
        return batch

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
