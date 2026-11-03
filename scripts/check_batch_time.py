"""Check batch timestamps/order in a disposable database; live DWH is untouched."""

from contextlib import closing
from datetime import datetime, timezone
import os
from types import SimpleNamespace
import uuid

import psycopg2
from psycopg2 import sql

from pipelines.batch_time import airflow_batch, check_batch_order


def batch(logical_date, run_after, run_id="test"):
    return airflow_batch({
        "run_id": run_id,
        "dag_run": SimpleNamespace(logical_date=logical_date, run_after=run_after),
    })


def check():
    logical = datetime.fromisoformat("2026-11-02T16:00:00+03:00")
    after = datetime.fromisoformat("2026-11-02T14:00:00+00:00")
    expected = "2026-11-02T13:00:00+00:00"
    assert batch(logical, after)["extracted_at"] == expected
    assert batch(None, after)["extracted_at"] == after.isoformat()
    assert batch(logical, after) == batch(logical, after)  # retry is stable
    try:
        batch(logical.replace(tzinfo=None), after)
    except ValueError:
        pass
    else:
        raise AssertionError("Naive timestamp accepted")

    params = psycopg2.extensions.parse_dsn(os.environ["DWH_DSN"])
    dbname = "batch_time_check_" + uuid.uuid4().hex[:12]
    with closing(psycopg2.connect(**dict(params, dbname="postgres"))) as admin:
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        try:
            with closing(psycopg2.connect(**dict(params, dbname=dbname))) as conn:
                with conn, conn.cursor() as cur:
                    cur.execute("CREATE SCHEMA raw")
                    for name in ("orders", "sellers__keys"):
                        cur.execute(sql.SQL(
                            "CREATE TABLE raw.{} (_batch_id text, _extracted_at timestamptz)"
                        ).format(sql.Identifier(name)))
                # Empty raw and absent tables are allowed.
                current = batch(logical, after)
                check_batch_order(conn, current)
                for name in ("orders", "sellers__keys"):
                    with conn, conn.cursor() as cur:
                        cur.execute(sql.SQL("TRUNCATE raw.orders, raw.sellers__keys"))
                        cur.execute(sql.SQL("INSERT INTO raw.{} VALUES (%s, %s)").format(
                            sql.Identifier(name)), (current["batch_id"], expected))
                    check_batch_order(conn, current)  # same batch/date retry
                    check_batch_order(conn, batch(after, after, "newer"))
                    for candidate in (
                        batch(logical, after, "collision"),
                        batch(datetime(2026, 11, 1, tzinfo=timezone.utc), after),
                    ):
                        try:
                            check_batch_order(conn, candidate)
                        except ValueError:
                            pass
                        else:
                            raise AssertionError(f"Out-of-order batch accepted: {name}")
        finally:
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(dbname)))
    print("PASS: logical/manual/UTC/retry dates; empty raw; newer/retry batches; "
          "backdate/collision rejection for rows and key snapshots; temporary DB removed")


if __name__ == "__main__":
    check()
