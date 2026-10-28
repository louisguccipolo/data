"""Загрузка CSV Olist в source.raw, как у организаторов.

Запуск: python scripts/load_raw.py [--dsn DSN] [--data-dir DIR] [--recreate] [--dry-run].
TRUNCATE + COPY сохраняет исходные значения, включая разницу между NULL и ''.
При перезаливке у всех строк новый _loaded_at: EL заберёт таблицу целиком.
Модель переживает это за счёт hashdiff сателлитов (спека, §4.2).
Проверяем sha256: Kaggle отличается от GitHub, контрольные цифры расходятся.
"""

import argparse
import csv
import hashlib
import os
from pathlib import Path
import re
import sys
import time


REPO_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DSN = "host=localhost port=15432 dbname=marketplace user=hack password=hack"

# файл -> таблица -> колонки в порядке CSV
MAPPING = (
    (
        "olist_orders_dataset.csv",
        "orders",
        (
            "order_id", "customer_id", "order_status", "order_purchase_timestamp",
            "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
        ),
    ),
    (
        "olist_order_items_dataset.csv",
        "order_items",
        (
            "order_id", "order_item_id", "product_id", "seller_id",
            "shipping_limit_date", "price", "freight_value",
        ),
    ),
    (
        "olist_order_payments_dataset.csv",
        "order_payments",
        (
            "order_id", "payment_sequential", "payment_type",
            "payment_installments", "payment_value",
        ),
    ),
    (
        "olist_order_reviews_dataset.csv",
        "order_reviews",
        (
            "review_id", "order_id", "review_score", "review_comment_title",
            "review_comment_message", "review_creation_date", "review_answer_timestamp",
        ),
    ),
    (
        "olist_products_dataset.csv",
        "products",
        (
            "product_id", "product_category_name", "product_name_lenght",
            "product_description_lenght", "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm",
        ),
    ),
    (
        "olist_sellers_dataset.csv",
        "sellers",
        ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
    ),
    (
        "olist_customers_dataset.csv",
        "customers",
        (
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state",
        ),
    ),
    (
        "product_category_name_translation.csv",
        "product_category_translation",
        ("product_category_name", "product_category_name_english"),
    ),
)


def verify_checksums(data_dir):
    checksums = {}
    source = "манифестом"
    manifest = REPO_DIR / "scripts/olist.sha256"
    for line in manifest.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("#"):
            match = re.fullmatch(r"#\s*(olist/work-at-olist-data)@([0-9a-f]{40})", line)
            if match:
                source = f"{match[1]}@{match[2][:7]}"
            continue
        if not line:
            continue
        fields = line.split()
        if len(fields) != 2 or not re.fullmatch(r"[0-9a-f]{64}", fields[0]):
            raise ValueError(f"Неверная строка в {manifest}: {line}")
        expected, filename = fields
        if filename in checksums:
            raise ValueError(f"Повтор файла в {manifest}: {filename}")
        checksums[filename] = expected

    filenames = {name for name, _, _ in MAPPING}
    if set(checksums) != filenames:
        raise ValueError(
            "Манифест и MAPPING не совпадают: "
            f"нет в манифесте {sorted(filenames - set(checksums))}; "
            f"лишние в манифесте {sorted(set(checksums) - filenames)}"
        )

    missing = [name for name, _, _ in MAPPING if not (data_dir / name).is_file()]
    if missing:
        raise ValueError(
            f"Нет файлов в {data_dir}: {', '.join(missing)}. Выполните make data."
        )

    mismatches = []
    for filename, expected in checksums.items():
        checksum = hashlib.sha256()
        with (data_dir / filename).open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                checksum.update(block)
        actual = checksum.hexdigest()
        if actual != expected:
            mismatches.append(f"{filename}: ожидаем {expected}, фактически {actual}")
    if mismatches:
        raise ValueError(
            "в data/ не GitHub-версия Olist (вероятно, Kaggle). Выполните make data\n"
            + "\n".join(mismatches)
        )
    return f"sha256: {len(checksums)}/{len(checksums)} совпадают с {source}"


def inspect_files(data_dir):
    checksum_summary = verify_checksums(data_dir)
    counts = {}
    for filename, _, columns in MAPPING:
        with (data_dir / filename).open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream)
            header = next(reader, [])
            if header != list(columns):
                raise ValueError(
                    f"Неверный заголовок {filename}: в файле {header}; ожидаем {list(columns)}"
                )
            counts[filename] = sum(1 for _ in reader)
    return counts, checksum_summary


def load_raw(dsn, data_dir, counts, recreate):
    # dry-run не требует драйвера и подключения к БД
    import psycopg2
    from psycopg2 import sql

    conn = psycopg2.connect(dsn)
    loaded = {}
    try:
        conn.set_client_encoding("UTF8")
        with conn:
            with conn.cursor() as cur:
                for schema in ("raw", "core", "mart"):
                    cur.execute(
                        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
                    )
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'raw' AND table_type = 'BASE TABLE'"
                )
                existing = {row[0] for row in cur.fetchall()}
                if recreate or any(table not in existing for _, table, _ in MAPPING):
                    cur.execute((REPO_DIR / "source/init/02_raw_ddl.sql").read_text(encoding="utf-8"))

                for filename, table, columns in MAPPING:
                    relation = sql.Identifier("raw", table)
                    cur.execute(sql.SQL("TRUNCATE {}").format(relation))
                    query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
                        relation, sql.SQL(", ").join(sql.Identifier(name) for name in columns)
                    )
                    # Файл идёт прямо в COPY: пустое поле и кавычки имеют разный смысл
                    with (data_dir / filename).open(encoding="utf-8-sig", newline="") as stream:
                        cur.copy_expert(query.as_string(cur), stream)
                    cur.execute(
                        sql.SQL("UPDATE {} SET {} = %s").format(
                            relation, sql.Identifier("_source_file")
                        ),
                        (filename,),
                    )
                    cur.execute(sql.SQL("SELECT count(*) FROM {}").format(relation))
                    actual = cur.fetchone()[0]
                    expected = counts[filename]
                    if actual != expected:
                        raise ValueError(
                            f"raw.{table}: в таблице {actual} строк, в CSV {expected}"
                        )
                    loaded[table] = actual

        # Статистика обновляется после commit загрузки
        try:
            with conn:
                with conn.cursor() as cur:
                    for _, table, _ in MAPPING:
                        cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier("raw", table)))
        except Exception as exc:
            raise RuntimeError(f"Данные загружены, но ANALYZE не выполнен: {exc}") from exc
    finally:
        conn.close()
    return loaded


def main():
    parser = argparse.ArgumentParser(description="Загрузить CSV Olist в source.raw")
    parser.add_argument("--dsn", default=os.environ.get("SOURCE_DSN", DEFAULT_DSN))
    parser.add_argument("--data-dir", type=Path, default=os.environ.get("DATA_DIR", REPO_DIR / "data"))
    parser.add_argument("--recreate", action="store_true", help="пересоздать все восемь таблиц raw")
    parser.add_argument("--dry-run", action="store_true", help="проверить файлы без БД")
    args = parser.parse_args()
    started = time.perf_counter()
    try:
        counts, checksum_summary = inspect_files(args.data_dir)
        if args.dry_run:
            rows = counts
            label = "Файл"
        else:
            loaded = load_raw(args.dsn, args.data_dir, counts, args.recreate)
            rows = {f"raw.{table}": count for table, count in loaded.items()}
            label = "Таблица"
        print(checksum_summary)
        print(f"{label:<40} | Записей")
        for name, count in rows.items():
            print(f"{name:<40} | {count}")
        print(f"Итого: {sum(rows.values())}")
        print(f"Время: {time.perf_counter() - started:.2f} с")
    except Exception as exc:
        print(f"Ошибка: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
