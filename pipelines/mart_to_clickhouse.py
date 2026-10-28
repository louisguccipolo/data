"""Cloudberry mart.* -> ClickHouse mp.* через table function postgresql().

Схема: TRUNCATE mp.<t>__stg -> INSERT SELECT из Cloudberry -> сверка count -> EXCHANGE TABLES.
Дашборды никогда не видят полупустую таблицу. Колонки берутся из DDL целевой таблицы в ClickHouse,
так что витрина в Cloudberry может содержать лишние колонки — уедут только нужные.
Креды к Cloudberry лежат в named collection `cloudberry_dwh` (в query_log пароля не будет).
"""

import os

import clickhouse_connect
import psycopg2

MARTS: list[str] = []  # заполнить после моделирования витрин


def _ch():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="mp",
    )


def load_mart(table: str) -> int:
    ch = _ch()
    cols = [
        r[0]
        for r in ch.query(
            "SELECT name FROM system.columns WHERE database = 'mp' AND table = {t:String} ORDER BY position",
            parameters={"t": table},
        ).result_rows
    ]
    if not cols:
        raise RuntimeError(f"mp.{table} не найдена в ClickHouse — проверь infra/clickhouse/init")
    col_list = ", ".join(f"`{c}`" for c in cols)

    ch.command(f"TRUNCATE TABLE mp.{table}__stg")
    ch.command(
        f"INSERT INTO mp.{table}__stg ({col_list}) "
        f"SELECT {col_list} FROM postgresql(dwh, schema = 'mart', table = '{table}')"
    )

    loaded = ch.command(f"SELECT count() FROM mp.{table}__stg")
    with psycopg2.connect(os.environ["DWH_DSN"]) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM mart.{table}")
        expected = cur.fetchone()[0]
    if loaded != expected:
        raise RuntimeError(f"mp.{table}: в ClickHouse {loaded} строк, в Cloudberry {expected}")

    ch.command(f"EXCHANGE TABLES mp.{table} AND mp.{table}__stg")
    print(f"mp.{table}: {loaded} rows swapped in")
    return loaded
