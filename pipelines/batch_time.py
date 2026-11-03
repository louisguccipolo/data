"""Stable Airflow batch time and ordering of the incremental DWH history."""

from datetime import timezone

from psycopg2 import sql

from pipelines.source_to_raw import TABLES


def airflow_batch(context):
    """Use explicit/scheduled logical_date, or the manual run's fixed run_after."""
    run = context["dag_run"]
    timestamp = run.logical_date if run.logical_date is not None else run.run_after
    if timestamp is None or timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Дата батча Airflow должна содержать часовой пояс")
    return {
        "batch_id": context["run_id"],
        "extracted_at": timestamp.astimezone(timezone.utc).isoformat(),
    }


def check_batch_order(conn, batch):
    """Reject backdated/colliding batches before any extract_load task writes raw.

    ODS incremental models use load_dts > watermark. An earlier date would be
    silently ignored; a different batch at the same date is ambiguous. A retry
    of the same batch at the same timestamp is allowed.
    """
    relations = list(TABLES) + [f"{name}__keys" for name, keys in TABLES.items() if keys]
    with conn.cursor() as cur:
        for name in relations:
            cur.execute("SELECT to_regclass(%s)", (f"raw.{name}",))
            if cur.fetchone()[0] is None:
                continue
            cur.execute(
                sql.SQL(
                    "SELECT _batch_id, _extracted_at FROM {} "
                    "WHERE _extracted_at >= %s::timestamptz "
                    "AND (_extracted_at > %s::timestamptz OR _batch_id <> %s) LIMIT 1"
                ).format(sql.Identifier("raw", name)),
                (batch["extracted_at"], batch["extracted_at"], batch["batch_id"]),
            )
            conflict = cur.fetchone()
            if conflict:
                raise ValueError(
                    f"Дата батча {batch['extracted_at']} нарушает порядок истории: "
                    f"raw.{name} уже содержит батч {conflict[0]} от {conflict[1]}. "
                    "Выберите более позднюю logical_date; исторический replay "
                    "требует отдельной пересборки из соответствующих снимков."
                )
