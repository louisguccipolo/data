"""EL: source.raw → DWH raw, все версии строк без преобразования данных.

Запуск: python -m pipelines.source_to_raw [--tables t1,t2] [--batch-id ID].
Подключения — SOURCE_DSN и DWH_DSN; EL_LOOKBACK_SECONDS задаёт перекрытие (300 с).
Берём _loaded_at >= watermark - lookback: now() отражает начало транзакции,
поэтому поздний commit может принести строки ниже водяного знака. Повторы окна
отбрасываем по _src_loaded_at и md5 содержимого, различая NULL и пустую строку.
Полные снимки ключей нужны rts_* / eff_*: по инкременту не видно исчезновения
строки (спека §4.2). Всё содержимое храним как text, типы приводит staging.
На таблицу — один снимок REPEATABLE READ READ ONLY и одна транзакция DWH.
"""

import argparse
from contextlib import closing
from datetime import datetime, timedelta, timezone
import os
import sys
import tempfile

import psycopg2
from psycopg2 import sql


# таблица -> колонки полного снимка ключей (None — снимок не нужен)
TABLES = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": None,
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "customers": None,
    "product_category_translation": None,
}

METADATA = {
    "_src_loaded_at": "timestamptz",
    "_batch_id": "text NOT NULL",
    "_extracted_at": "timestamptz NOT NULL",
    "_op": "text",
    "_lsn": "text",
    "_commit_ts": "timestamptz",
}


def _columns(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = %s ORDER BY ordinal_position",
        (table,),
    )
    return [row[0] for row in cur.fetchall()]


def _names(columns):
    return sql.SQL(", ").join(sql.Identifier(name) for name in columns)


def _text_columns(columns):
    return sql.SQL(", ").join(
        sql.SQL("{} text").format(sql.Identifier(name)) for name in columns
    )


def _ensure_raw_table(cur, table, source_columns):
    relation = sql.Identifier("raw", table)
    definitions = [_text_columns(source_columns)]
    definitions.extend(
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(typ))
        for name, typ in METADATA.items()
    )
    cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            relation, sql.SQL(", ").join(definitions)
        )
    )
    existing = _columns(cur, table)
    added = [name for name in source_columns if name not in existing]
    for name in added:
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN {} text").format(
                relation, sql.Identifier(name)
            )
        )
    data_columns = [name for name in existing if name not in METADATA] + added
    removed = [name for name in data_columns if name not in source_columns]
    if removed:
        print(
            f"Предупреждение: raw.{table}: колонок нет в источнике: "
            f"{', '.join(removed)}; у новых строк будет NULL",
            file=sys.stderr,
        )
    cur.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
            sql.Identifier(f"{table}__src_loaded_at_idx"),
            relation,
            sql.Identifier("_src_loaded_at"),
        )
    )
    return data_columns, added


def _copy(source_cur, dwh_cur, query, stage, columns):
    # CSV различает NULL (пустое поле) и '' (поле в кавычках).
    # До 64 МБ держим в памяти, затем временный файл переносится на диск.
    with tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024) as stream:
        source_cur.copy_expert(
            sql.SQL("COPY ({}) TO STDOUT WITH (FORMAT csv, NULL '')").format(
                query
            ).as_string(source_cur),
            stream,
        )
        stream.seek(0)
        dwh_cur.copy_expert(
            sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')").format(
                sql.Identifier(stage), _names(columns)
            ).as_string(dwh_cur),
            stream,
        )


def _row_hash(alias, columns):
    # Текст записи экранирует разделители и различает NULL и ''.
    return sql.SQL("md5(ROW({})::text)").format(
        sql.SQL(", ").join(sql.Identifier(alias, name) for name in columns)
    )


def _insert_data(cur, table, data_columns, cutoff, batch_id, extracted_at):
    overlap_filter = sql.SQL("")
    params = []
    if cutoff is not None:
        overlap_filter = sql.SQL("WHERE {} >= %s").format(
            sql.Identifier("existing", "_src_loaded_at")
        )
        params.append(cutoff)
    params.extend((batch_id, extracted_at))
    columns = data_columns + ["_src_loaded_at"]
    # Хешируем окно один раз: планировщик может выполнить антисоединение по хешу.
    cur.execute(
        sql.SQL(
            "WITH {overlap} AS MATERIALIZED ("
            "SELECT {existing_ts}, {existing_hash} AS {row_hash} "
            "FROM {relation} AS {existing} {overlap_filter}) "
            "INSERT INTO {relation} ({columns}, {batch_col}, {extracted_col}) "
            "SELECT {stage_columns}, %s, %s FROM {stage} AS {incoming} "
            "WHERE NOT EXISTS (SELECT 1 FROM {overlap} AS {seen} "
            "WHERE {seen_ts} IS NOT DISTINCT FROM {incoming_ts} "
            "AND {seen_hash} = {incoming_hash})"
        ).format(
            overlap=sql.Identifier("overlap"),
            existing_ts=sql.Identifier("existing", "_src_loaded_at"),
            existing_hash=_row_hash("existing", data_columns),
            row_hash=sql.Identifier("row_hash"),
            relation=sql.Identifier("raw", table),
            existing=sql.Identifier("existing"),
            overlap_filter=overlap_filter,
            columns=_names(columns),
            batch_col=sql.Identifier("_batch_id"),
            extracted_col=sql.Identifier("_extracted_at"),
            stage_columns=sql.SQL(", ").join(
                sql.Identifier("incoming", name) for name in columns
            ),
            stage=sql.Identifier("el_stage"),
            incoming=sql.Identifier("incoming"),
            seen=sql.Identifier("seen"),
            seen_ts=sql.Identifier("seen", "_src_loaded_at"),
            incoming_ts=sql.Identifier("incoming", "_src_loaded_at"),
            seen_hash=sql.Identifier("seen", "row_hash"),
            incoming_hash=_row_hash("incoming", data_columns),
        ),
        params,
    )
    return cur.rowcount


def _load_keys(source_cur, dwh_cur, table, key_columns, batch_id, extracted_at):
    relation = sql.Identifier("raw", f"{table}__keys")
    batch_col = sql.Identifier("_batch_id")
    extracted_col = sql.Identifier("_extracted_at")
    dwh_cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({}, {} text NOT NULL, "
                "{} timestamptz NOT NULL)").format(
            relation, _text_columns(key_columns), batch_col, extracted_col
        )
    )
    dwh_cur.execute(
        sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
            sql.Identifier(f"{table}__keys__batch_id_idx"), relation, batch_col
        )
    )
    dwh_cur.execute(
        sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
            sql.Identifier("el_keys_stage"), _text_columns(key_columns)
        )
    )
    query = sql.SQL("SELECT DISTINCT {} FROM {}").format(
        sql.SQL(", ").join(
            sql.SQL("{}::text").format(sql.Identifier(name)) for name in key_columns
        ),
        sql.Identifier("raw", table),
    )
    _copy(source_cur, dwh_cur, query, "el_keys_stage", key_columns)
    dwh_cur.execute(
        sql.SQL("DELETE FROM {} WHERE {} = %s").format(relation, batch_col),
        (batch_id,),
    )
    dwh_cur.execute(
        sql.SQL("INSERT INTO {} ({}, {}, {}) SELECT {}, %s, %s FROM {}").format(
            relation, _names(key_columns), batch_col, extracted_col,
            _names(key_columns), sql.Identifier("el_keys_stage"),
        ),
        (batch_id, extracted_at),
    )
    return dwh_cur.rowcount


def load_table(table: str, batch_id: str, extracted_at: datetime) -> dict:
    """Загрузить таблицу и полный снимок её ключей одним батчем."""
    if table not in TABLES:
        raise ValueError(f"Неизвестная таблица: {table}")
    if not batch_id:
        raise ValueError("batch_id не должен быть пустым")
    if extracted_at.tzinfo is None or extracted_at.utcoffset() is None:
        raise ValueError("extracted_at должен содержать часовой пояс")
    extracted_at = extracted_at.astimezone(timezone.utc)
    lookback = int(os.environ.get("EL_LOOKBACK_SECONDS", "300"))
    if lookback < 0:
        raise ValueError("EL_LOOKBACK_SECONDS должен быть неотрицательным")
    dsns = {}
    for name in ("SOURCE_DSN", "DWH_DSN"):
        dsns[name] = os.environ.get(name)
        if not dsns[name]:
            raise ValueError(f"Не задана переменная {name}")

    with closing(psycopg2.connect(dsns["SOURCE_DSN"])) as source:
        source.set_client_encoding("UTF8")
        source.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with closing(psycopg2.connect(dsns["DWH_DSN"])) as dwh:
            dwh.set_client_encoding("UTF8")
            with dwh, source:
                with source.cursor() as s, dwh.cursor() as d:
                    for cur in (s, d):
                        cur.execute(
                            "SELECT set_config('TimeZone', 'UTC', true), "
                            "set_config('DateStyle', 'ISO, YMD', true)"
                        )
                    source_columns = _columns(s, table)
                    if not source_columns:
                        raise ValueError(f"В источнике нет таблицы raw.{table}")
                    required = {"_loaded_at", "_source_file", *(TABLES[table] or [])}
                    missing = required - set(source_columns)
                    if missing:
                        raise ValueError(
                            f"raw.{table}: в источнике нет колонок {', '.join(sorted(missing))}"
                        )
                    collisions = set(source_columns) & set(METADATA)
                    if collisions:
                        raise ValueError(
                            f"raw.{table}: имена заняты метаданными EL: "
                            f"{', '.join(sorted(collisions))}"
                        )
                    source_columns.remove("_loaded_at")
                    data_columns, added = _ensure_raw_table(d, table, source_columns)
                    d.execute(
                        sql.SQL("SELECT max({}) FROM {}").format(
                            sql.Identifier("_src_loaded_at"), sql.Identifier("raw", table)
                        )
                    )
                    watermark = d.fetchone()[0]
                    cutoff = watermark - timedelta(seconds=lookback) if watermark else None
                    d.execute(
                        sql.SQL("CREATE TEMP TABLE {} ({}, {} timestamptz) "
                                "ON COMMIT DROP").format(
                            sql.Identifier("el_stage"), _text_columns(data_columns),
                            sql.Identifier("_src_loaded_at"),
                        )
                    )
                    query = sql.SQL("SELECT {}, {} FROM {}").format(
                        sql.SQL(", ").join(
                            sql.SQL("{}::text").format(sql.Identifier(name))
                            for name in source_columns
                        ),
                        sql.Identifier("_loaded_at"), sql.Identifier("raw", table),
                    )
                    if cutoff is not None:
                        query += sql.SQL(" WHERE {} >= {}").format(
                            sql.Identifier("_loaded_at"), sql.Literal(cutoff)
                        )
                    _copy(s, d, query, "el_stage", source_columns + ["_src_loaded_at"])
                    d.execute(
                        sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("el_stage"))
                    )
                    selected = d.fetchone()[0]
                    inserted = _insert_data(d, table, data_columns, cutoff, batch_id, extracted_at)
                    keys = None
                    if TABLES[table] is not None:
                        keys = _load_keys(s, d, table, TABLES[table], batch_id, extracted_at)
    return {
        "table": table,
        "watermark": watermark.isoformat() if watermark else None,
        "selected": selected,
        "inserted": inserted,
        "skipped_overlap": selected - inserted,
        "keys": keys,
        "added_columns": added,
    }


def new_batch() -> dict:
    """Создать общий идентификатор и время ручного батча в UTC."""
    extracted_at = datetime.now(timezone.utc).isoformat()
    return {"batch_id": f"manual__{extracted_at}", "extracted_at": extracted_at}


def main():
    parser = argparse.ArgumentParser(description="Загрузить source.raw в DWH raw")
    parser.add_argument("--tables", help="таблицы через запятую; по умолчанию все")
    parser.add_argument("--batch-id", help="идентификатор батча для повторного запуска")
    args = parser.parse_args()
    try:
        tables = list(TABLES) if args.tables is None else list(dict.fromkeys(
            name.strip() for name in args.tables.split(",")
        ))
        if any(name not in TABLES for name in tables):
            raise ValueError(f"Допустимые таблицы: {', '.join(TABLES)}")
        batch = new_batch()
        if args.batch_id is not None:
            batch["batch_id"] = args.batch_id
        extracted_at = datetime.fromisoformat(batch["extracted_at"])
        print(f"Батч: {batch['batch_id']}; extracted_at={batch['extracted_at']}")
        print("Таблица                         | Выбрано | Вставлено | Повторы | Ключи | Водяной знак | Новые колонки")
        for table in tables:
            try:
                result = load_table(table, batch["batch_id"], extracted_at)
            except Exception as exc:
                raise RuntimeError(f"raw.{table}: {exc}") from exc
            print(
                f"{table:<31} | {result['selected']:>7} | {result['inserted']:>9} | "
                f"{result['skipped_overlap']:>7} | {str(result['keys']):>5} | "
                f"{result['watermark']} | {', '.join(result['added_columns']) or '—'}"
            )
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
