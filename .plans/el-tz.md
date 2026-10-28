# ТЗ: EL source → DWH `raw` (Airflow)

Контекст: `.plans/marketplace-dwh-spec.md`, §4.2 (EL), §6 (служебные колонки, `rts_*` / `eff_*`), §12 (DAG), §19 (остатки старой архитектуры).
Стиль кода — как в `scripts/load_raw.py`: русский docstring модуля (что делает, как запускать, почему так), `psycopg2.sql.Identifier` для имён, только stdlib + psycopg2.

## Что есть сейчас

- `source` (Postgres 16): схема `raw`, 8 таблиц Olist (`orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `sellers`, `customers`, `product_category_translation`). Все колонки `text`, плюс `_source_file text` и `_loaded_at timestamptz DEFAULT now()`. Нет PK и `updated_at`. Заливает `scripts/load_raw.py` (TRUNCATE + COPY в одной транзакции): у всех строк одной заливки **один и тот же** `_loaded_at`.
- DWH (Postgres 16): схема `raw` создана `dwh/init.sql`, таблиц нет.
- Контейнер `airflow`: env `SOURCE_DSN`, `DWH_DSN`, `PYTHONPATH=/opt/project`, репозиторий в `/opt/project`, dbt в `/opt/project/dbt`.
- `pipelines/oltp_to_raw.py` и `airflow/dags/marketplace_elt.py` — под старую архитектуру (Cloudberry, `OLTP_DSN`, `public.*`, водяной знак `updated_at`, старый список таблиц). Не работают.

## Что сделать

### 1. `pipelines/source_to_raw.py` (новый; `pipelines/oltp_to_raw.py` удалить)

**Таблицы — данные, а не код:**
```python
# таблица -> колонки снимка ключей (None — снимок не нужен)
TABLES = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": None,          # пропажу платежа ловит multi-active сателлит
    "order_reviews": ["review_id", "order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "customers": None,
    "product_category_translation": None,
}
```

**DWH `raw.<t>`** (создаётся, если нет; `CREATE TABLE IF NOT EXISTS`, обычный heap Postgres, без Cloudberry-синтаксиса):
- все колонки данных источника (всё, кроме `_loaded_at`) — **всегда `text`**, независимо от типа в источнике;
- `_source_file text`;
- `_src_loaded_at timestamptz` — это `_loaded_at` источника (переименован: своё `_loaded_at` в DWH не заводим);
- `_batch_id text NOT NULL`, `_extracted_at timestamptz NOT NULL`;
- под будущий CDC, nullable, сейчас всегда NULL: `_op text`, `_lsn text`, `_commit_ts timestamptz`.
- индекс по `_src_loaded_at` (для водяного знака и окна перекрытия).

**Schema drift:** колонка есть в источнике, нет в DWH → `ALTER TABLE raw.<t> ADD COLUMN <col> text`. Колонка пропала из источника → в DWH остаётся, у новых строк NULL, печатаем предупреждение. Таблицы в источнике нет → понятная ошибка.

**Водяной знак с перекрытием:**
- `wm = max(_src_loaded_at)` в DWH `raw.<t>`; NULL → забираем всё.
- Из источника: `_loaded_at >= wm - lookback`, `lookback` = env `EL_LOOKBACK_SECONDS`, по умолчанию 300. Не строгое `>`: `now()` — время начала транзакции, транзакция, закоммиченная после нашего чтения, принесла бы строки ниже водяного знака.
- **Повторы перекрытия не вставлять.** У источника организаторов все строки одной заливки имеют один `_loaded_at`, поэтому при `>=` каждый прогон заново выбирает всю таблицу. Правило: строка из выборки вставляется, если в DWH `raw.<t>` нет строки с тем же `_src_loaded_at` и тем же содержимым (все колонки данных + `_source_file`; сравнение по `md5` от строки, NULL и `''` должны различаться). Сравниваем только с окном `_src_loaded_at >= wm - lookback`, не со всей таблицей. Перезаливка источника (новый `_loaded_at`) вставляется целиком — это ожидаемо, дальше её схлопывает `sat` (спека §4.2, §6).
- Механика: выборка из источника — `COPY (SELECT …) TO STDOUT` потоком через `tempfile.SpooledTemporaryFile` (как в старом коде) → `COPY` во временную таблицу DWH → `INSERT INTO raw.<t> SELECT … FROM stage WHERE NOT EXISTS (…)`. `COPY` в формате CSV, чтобы NULL и `''` не склеились.

**Снимок ключей** (только для таблиц, где в `TABLES` список колонок):
- `raw.<t>__keys`: колонки ключа (`text`), `_batch_id text NOT NULL`, `_extracted_at timestamptz NOT NULL`; индекс по `_batch_id`.
- `SELECT DISTINCT <ключи> FROM raw.<t>` в источнике — **все** ключи таблицы, не только из выборки. Строки, где все колонки ключа NULL, тоже пишем (нулевой ключ решает staging).
- Идемпотентно по батчу: в той же транзакции `DELETE FROM raw.<t>__keys WHERE _batch_id = %s`, затем вставка.

**Транзакции и согласованность:**
- Источник: одна транзакция `REPEATABLE READ READ ONLY` на таблицу — выборка данных и снимок ключей из одного снимка.
- DWH: вся работа по таблице (DDL, drift, вставка данных, снимок ключей) — одна транзакция; при любой ошибке rollback, `raw` не меняется.
- Повторный запуск с тем же `batch_id` ничего не удваивает (данные — правилом перекрытия, ключи — DELETE по батчу).

**Интерфейс:**
- `load_table(table: str, batch_id: str, extracted_at: datetime) -> dict` — возвращает статистику: `table`, `watermark`, `selected` (строк выбрано из источника), `inserted`, `skipped_overlap`, `keys` (строк снимка или None), `added_columns`.
- `new_batch() -> dict` — `{"batch_id": "<manual__ISO-время>", "extracted_at": "<ISO UTC>"}` для ручного запуска.
- CLI: `python -m pipelines.source_to_raw [--tables t1,t2] [--batch-id ID]` — один батч на все выбранные таблицы (один `_extracted_at`), печать таблицы статистики, exit 0 / ≠0 с понятным сообщением.
- DSN: env `SOURCE_DSN`, `DWH_DSN` (без значений по умолчанию с паролями).
- Docstring модуля на русском: что делает, как запускать, почему `>=` и правило повторов, почему снимок ключей (спека §4.2: по инкременту не видно исчезновения строки; кормит `rts_*` / `eff_*`), почему всё `text`.

### 2. `airflow/dags/marketplace_elt.py`

- Расписание `@daily` (спека §12), `catchup=False`, `max_active_runs=1`, ретраи как сейчас.
- Первая задача `open_batch` возвращает `{"batch_id": run_id DAG-рана, "extracted_at": <UTC now, ISO>}` — одно значение `_extracted_at` на весь батч (спека §6: `load_dts` = `_extracted_at` батча).
- `extract_load` — dynamic task mapping по `TABLES` (`.partial(batch=…).expand(table=…)`), вызывает `load_table`.
- Затем `dbt_build` (`BashOperator`, `cd /opt/project/dbt && dbt build`), как сейчас.
- Docstring обновить: source → DWH raw → dbt, без Cloudberry.

### 3. Убрать остатки старой архитектуры, мешающие сборке (спека §19)

- Удалить: `pipelines/oltp_to_raw.py`, каталог `oltp/`, DAG `airflow/dags/oltp_simulator.py`, все файлы в `dbt/models/staging/` (старые модели и yml под старые таблицы), макросы `dbt/macros/cloudberry_create_table_as.sql` и `dbt/macros/latest_version.sql`. Макрос `pii_hash` из `latest_version.sql` сохранить в `dbt/macros/pii_hash.sql` (нужен для PII-стратегии, спека §15); `age_band` не нужен.
- `dbt/dbt_project.yml`: убрать комментарии про Cloudberry и конфиг `staging: +schema: stg` (staging появится отдельной задачей); `vars.pii_salt` оставить.
- `Makefile`: убрать цель `tick` (и из `.PHONY`); цель `run` оставить. Добавить цель `el`: `$(DC) exec airflow python -m pipelines.source_to_raw` — «один батч EL вручную». Комментарий `psql-dwh` («gpadmin») поправить на «владелец DWH».
- Новый `dbt/models/sources.yml`: source `raw` (схема `raw`) — 8 таблиц + 5 таблиц `__keys`; `loaded_at_field: _extracted_at`; freshness `warn_after 26h`, `error_after 50h` (DAG суточный). Описания таблиц — коротко, по-русски.

## Не трогать

`scripts/`, `source/`, `dwh/`, `compose.yaml`, `infra/`, `pipelines/mart_to_clickhouse.py`, `dbt/macros/sat.sql`, `rts.sql`, `eff_sat.sql`, `generate_schema_name.sql`, `.plans/`. Ничего не коммитить.

## Критерии приёмки (проверяет Claude на живом стенде)

1. `make el` на пустом DWH: в `raw.*` ровно столько строк, сколько в `source.raw.*` (orders 99 441, order_items 112 650, order_payments 103 886, order_reviews 99 224, products 32 951, sellers 3 095, customers 99 441, product_category_translation 71); содержимое совпадает с источником (`EXCEPT` в обе стороны по колонкам данных, включая NULL и `''`).
2. `__keys`: число строк = `count(DISTINCT ключ)` в источнике; у всех строк батча один `_batch_id` и один `_extracted_at`, одинаковый для всех таблиц батча.
3. Повторный `make el` без изменений источника: в данных `inserted = 0`, `skipped_overlap` = вся выборка; в `__keys` добавился новый батч.
4. Повторный запуск с тем же `--batch-id`: ничего не удвоилось ни в данных, ни в ключах.
5. `make seed` (перезаливка источника, новый `_loaded_at`) → `make el`: таблицы забраны целиком, строк в `raw` стало вдвое больше.
6. Schema drift: колонка, добавленная в `source.raw.orders`, после `make el` появилась в DWH как `text`, у старых строк NULL. Удалённая колонка остаётся в DWH, предупреждение в выводе.
7. Ошибка посреди загрузки (например, нет таблицы в источнике) → ненулевой код, `raw` этой таблицы не изменился.
8. DAG `marketplace_elt` парсится Airflow без ошибок (`airflow dags list-import-errors` пуст), ручной запуск проходит `open_batch` → 8 × `extract_load` → `dbt_build`.
9. `dbt parse` и `dbt build` проходят (моделей нет, sources валидны).
