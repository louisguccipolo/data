"""Публикация одиннадцати витрин Postgres mart → ClickHouse mart и refresh_status.

Запуск: from pipelines.mart_to_clickhouse import publish; publish('manual').
Подключения — CLICKHOUSE_HOST/USER/PASSWORD и DWH_DSN. Строки переносит сам
ClickHouse через postgresql(dwh, …): пароль Postgres хранится в named collection,
Python читает только контрольные агрегаты. DDL применяется при каждом запуске.
Сначала собираем весь комплект в __stg, проверяем схему, зерно, строки и точную
выручку; только затем переключаем таблицы и последним — статус успеха.
Одиннадцать EXCHANGE — не одна атомарная транзакция: между переключениями возможно
окно рассогласования; ошибка во время EXCHANGE может оставить частичный комплект.
Одновременные ручные публикации недопустимы; DAG ограничен max_active_runs=1.
"""

from contextlib import closing
import os
from pathlib import Path
import re

import clickhouse_connect
import psycopg2
from psycopg2 import sql


# витрина -> ключ зерна (для проверки уникальности)
MARTS = {
    "monthly_sales": "month",
    "seller_sales": "seller_id",
    "order_summary": "summary_key",
    "customer_summary": "summary_key",
    "category_sales": "category_key",
    "product_price_history": ("product_id", "sale_date", "price"),
    "order_status_funnel": "order_status",
    "payments_by_type": "payment_type",
    "payments_by_state_year": ("customer_state", "purchase_year"),
    "delivery_by_state": "customer_state",
    "review_delivery": "review_score",
}


def _ch():
    # На свежем томе mart может ещё не существовать — сначала применяем DDL.
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="default",
    )


def _columns(ch, table):
    return ch.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = 'mart' AND table = {t:String} ORDER BY position",
        parameters={"t": table},
    ).result_rows


def _ensure_schema(ch):
    ddl = (Path(__file__).resolve().parents[1] / "infra/clickhouse/mart.sql").read_text(encoding="utf-8")
    # DDL — единственный источник ожидаемых имён, типов и порядка колонок.
    # Определение каждой колонки в mart.sql занимает одну строку.
    expected = {}
    for table, body in re.findall(
        r"CREATE TABLE IF NOT EXISTS mart\.(\w+)\s*\((.*?)\)\s*ENGINE",
        ddl, re.DOTALL,
    ):
        expected[table] = [
            tuple(line.strip().rstrip(",").split(maxsplit=1))
            for line in body.strip().splitlines()
        ]
    if set(expected) != {*MARTS, "refresh_status"}:
        raise RuntimeError("mart.sql: ожидаются все витрины MARTS и refresh_status")
    for statement in ddl.split(";"):
        if statement.strip():
            ch.command(statement)
    for table in expected:
        ch.command(f"CREATE TABLE IF NOT EXISTS mart.{table}__stg AS mart.{table}")
    columns = {}
    for table, wanted in expected.items():
        for name in (table, f"{table}__stg"):
            actual = _columns(ch, name)
            if actual != wanted:
                raise RuntimeError(
                    f"mart.{name}: схема не совпадает с mart.sql; "
                    f"ожидалось {wanted}, получено {actual}. "
                    "Публикация отменена; нужна ручная миграция схемы."
                )
            if name == table:
                columns[table] = actual
    return columns


def _source_totals(cur, table, has_revenue):
    aggregate = sql.SQL(", sum(revenue_amount)") if has_revenue else sql.SQL("")
    cur.execute(
        sql.SQL("SELECT count(*){} FROM {}").format(
            aggregate, sql.Identifier("mart", table)
        )
    )
    return cur.fetchone()


def publish(batch_id: str) -> dict:
    """Собрать, проверить и опубликовать комплект; вернуть строки по витринам."""
    if not isinstance(batch_id, str) or not batch_id:
        raise ValueError("batch_id должен быть непустой строкой")
    rows = {}
    revenues = {}
    with closing(_ch()) as ch:
        columns = _ensure_schema(ch)
        with closing(psycopg2.connect(os.environ["DWH_DSN"])) as source:
            source.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with source, source.cursor() as cur:
                for table, key in MARTS.items():
                    schema = dict(columns[table])
                    col_list = ", ".join(f"`{name}`" for name in schema)
                    keys = (key,) if isinstance(key, str) else key
                    # uniqExact пропускает Nullable-аргументы с NULL. Tuple
                    # сохраняет допустимую NULL-группу из контракта блока Б.
                    key_list = ", ".join(
                        f"tuple(`{name}`)" if schema[name].startswith("Nullable(")
                        else f"`{name}`" for name in keys
                    )
                    null_check = " OR ".join(
                        f"isNull(`{name}`)" for name in keys
                        if not schema[name].startswith("Nullable(")
                    ) or "0"
                    ch.command(f"TRUNCATE TABLE mart.{table}__stg")
                    ch.command(
                        f"INSERT INTO mart.{table}__stg ({col_list}) "
                        f"SELECT {col_list} FROM postgresql(dwh, schema = 'mart', table = '{table}') "
                        "SETTINGS insert_null_as_default = 0"
                    )
                    has_revenue = "revenue_amount" in schema
                    aggregate = ", sum(revenue_amount)" if has_revenue else ""
                    actual = ch.query(
                        f"SELECT count(), uniqExact({key_list}), countIf({null_check})"
                        f"{aggregate} FROM mart.{table}__stg"
                    ).result_rows[0]
                    expected = _source_totals(cur, table, has_revenue)
                    count, unique, nulls = actual[:3]
                    if count == 0 or count != expected[0]:
                        raise RuntimeError(
                            f"mart.{table}: в ClickHouse {count} строк, "
                            f"в Postgres {expected[0]}; требуется равенство и > 0"
                        )
                    if count != unique or nulls:
                        raise RuntimeError(
                            f"mart.{table}: ключ {key}: строк={count}, "
                            f"уникальных={unique}, NULL={nulls}"
                        )
                    if has_revenue:
                        if actual[3] != expected[1]:
                            raise RuntimeError(
                                f"mart.{table}: выручка ClickHouse {actual[3]} "
                                f"не равна Postgres {expected[1]}"
                            )
                        revenues[table] = actual[3]
                    rows[table] = count
                    print(f"mart.{table}: строк={count}; проверки пройдены", flush=True)

        for table in ("order_summary", "customer_summary"):
            if rows[table] != 1:
                raise RuntimeError(f"mart.{table}: требуется ровно 1 строка, получено {rows[table]}")
        if len({revenues[table] for table in (
            "monthly_sales", "seller_sales", "category_sales", "order_summary"
        )}) != 1:
            raise RuntimeError(f"Выручка витрин не сходится: {revenues}")

        for table in MARTS:
            ch.command(f"EXCHANGE TABLES mart.{table} AND mart.{table}__stg")

        source_rows = sum(rows.values())
        ch.command("TRUNCATE TABLE mart.refresh_status__stg")
        ch.command(
            "INSERT INTO mart.refresh_status__stg "
            "(batch_id, published_at, source_rows, checks_passed) "
            "SELECT {batch:String}, now64(3, 'UTC'), {rows:UInt64}, 1",
            parameters={"batch": batch_id, "rows": source_rows},
        )
        ch.command("EXCHANGE TABLES mart.refresh_status AND mart.refresh_status__stg")
    return {"batch_id": batch_id, "rows": rows, "source_rows": source_rows}
