"""EL: OLTP Postgres -> Cloudberry raw.* (append-only, все версии строк).

Инкремент по updated_at: берём строки с updated_at >= max(updated_at) в raw.
Граница включительная — дубли на границе схлопнет дедуп в stg.
Новые колонки в источнике автоматически добавляются в raw (schema drift).
"""

import os
import tempfile

import psycopg2
from psycopg2 import sql

# таблица -> первичный ключ (нужен stg для дедупа)
TABLES = {
    "cities": "city_id",
    "categories": "category_id",
    "warehouses": "warehouse_id",
    "users": "user_id",
    "sellers": "seller_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "reviews": "review_id",
}

RAW_STORAGE = "appendoptimized=true, orientation=column, compresstype=zstd, compresslevel=1"


def _source_columns(cur, table):
    cur.execute(
        """SELECT a.attname, format_type(a.atttypid, a.atttypmod)
           FROM pg_attribute a
           WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
           ORDER BY a.attnum""",
        (f"public.{table}",),
    )
    return cur.fetchall()


def _ensure_raw_table(cur, table, columns):
    cols = sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(n), sql.SQL(t)) for n, t in columns)
    cur.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS raw.{} ({}, _loaded_at timestamptz NOT NULL DEFAULT now()) "
            "WITH (" + RAW_STORAGE + ") DISTRIBUTED RANDOMLY"
        ).format(sql.Identifier(table), cols)
    )
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = %s",
        (table,),
    )
    existing = {r[0] for r in cur.fetchall()}
    for name, typ in columns:
        if name not in existing:
            cur.execute(
                sql.SQL("ALTER TABLE raw.{} ADD COLUMN {} {}").format(
                    sql.Identifier(table), sql.Identifier(name), sql.SQL(typ)
                )
            )


def load_table(table: str) -> int:
    src = psycopg2.connect(os.environ["OLTP_DSN"])
    dwh = psycopg2.connect(os.environ["DWH_DSN"])
    try:
        with src.cursor() as s, dwh.cursor() as d:
            columns = _source_columns(s, table)
            _ensure_raw_table(d, table, columns)
            d.execute(sql.SQL("SELECT max(updated_at) FROM raw.{}").format(sql.Identifier(table)))
            watermark = d.fetchone()[0]

            col_list = sql.SQL(", ").join(sql.Identifier(n) for n, _ in columns)
            query = sql.SQL("SELECT {} FROM public.{}").format(col_list, sql.Identifier(table))
            if watermark is not None:
                query += sql.SQL(" WHERE updated_at >= {}").format(sql.Literal(watermark))

            # COPY-стрим через временный файл: в памяти до 64 МБ, дальше на диск
            with tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024) as buf:
                s.copy_expert(sql.SQL("COPY ({}) TO STDOUT").format(query).as_string(s), buf)
                buf.seek(0)
                d.copy_expert(
                    sql.SQL("COPY raw.{} ({}) FROM STDIN").format(sql.Identifier(table), col_list).as_string(d),
                    buf,
                )
                rows = d.rowcount
        dwh.commit()
        print(f"raw.{table}: +{rows} rows (watermark={watermark})")
        return rows
    finally:
        src.close()
        dwh.close()
