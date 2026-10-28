Расширено владельцем 28.10: публикуются все 11 витрин mart

# ТЗ: публикация DWH `mart` → ClickHouse `mart` (Airflow)

Контекст: `.plans/marketplace-dwh-spec.md` §8, §12, §13, §19; контракт витрин и типов — `.plans/superset-guide.md` §2–3 (источник `core` там = наш `dds`/`mart`); приёмка mart — `.plans/mart-implementation.md`.
Решение принято (вариант A): ClickHouse сам тянет данные из Postgres через `postgresql(dwh, …)` (named collection), Python только оркестрирует и сверяет. Live-движки (`ENGINE = PostgreSQL`, `MaterializedPostgreSQL`) и перекачку строк через Python не использовать.

## Что есть сейчас

- DWH (Postgres 16): схема `mart`, 5 таблиц dbt (`table`): `monthly_sales` (24), `seller_sales` (2970), `order_summary` (1), `customer_summary` (1), `category_sales` (74). Типы: деньги `numeric`, количества `bigint`, флаги `boolean`, `revenue_share` `double precision`, месяц `date`, остальное `text`. Все колонки в `information_schema` nullable. Роль `ch_reader` имеет SELECT на `mart`. `dbt test --select path:models/mart` — 22/22 PASS.
- ClickHouse 26.8 (`compose.yaml`, сервис `clickhouse`): `CLICKHOUSE_DB: mp`, база `mp` пустая (таблиц нет). Пользователь `etl` (полный доступ, `named_collection_control`), пользователь `bi` (`readonly=1`, `GRANT SELECT ON mp.*`) из `infra/clickhouse/init/02_bi_user.sh`. Named collection `dwh` → Postgres DWH под `ch_reader` (`infra/clickhouse/config.d/named_collections.xml`).
- `pipelines/mart_to_clickhouse.py` — заготовка под старую архитектуру: база `mp`, «Cloudberry» в docstring/сообщениях, `MARTS = []`, по одной таблице, без `refresh_status`, без DDL.
- `airflow/dags/marketplace_elt.py`: `open_batch` → `extract_load` (mapped) → `dbt_build`. Публикации нет. В контейнере `airflow` уже есть `clickhouse-connect`, `psycopg2`, env `CLICKHOUSE_HOST/USER/PASSWORD`, `DWH_DSN`, `PYTHONPATH=/opt/project`.
- `Makefile`, цель `ch`: `-d mp`.

## Что сделать

### 1. База `mart` в ClickHouse вместо `mp`

- `compose.yaml`: `CLICKHOUSE_DB: mart`. В комментариях/коде/Makefile не должно остаться `mp` как имени базы ClickHouse (`grep -rnw mp` по compose, Makefile, infra, pipelines, airflow — пусто; имя образа `mp-airflow` не трогать).
- `infra/clickhouse/init/02_bi_user.sh`: пользователь **`bi_reader`** (`readonly = 1`), `GRANT SELECT ON mart.* TO bi_reader`; старого `bi` больше нет. Пароль — прежний `CLICKHOUSE_BI_PASSWORD`.
- DDL витрин — **один файл** `infra/clickhouse/mart.sql` (только `CREATE DATABASE IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS`, идемпотентно). Его применяет пайплайн в начале каждого запуска; init-скрипты ClickHouse его не дублируют. Так схема создаётся и на свежем, и на уже инициализированном томе.

### 2. Таблицы ClickHouse (`infra/clickhouse/mart.sql`)

Пять витрин + `refresh_status`, состав и порядок колонок — ровно как в Postgres `mart` и `superset-guide.md` §3. Engine `MergeTree`, `ORDER BY` — ключ из гайда (`month`, `seller_id`, `summary_key`, `summary_key`, `category_key`; для `refresh_status` — `batch_id`).

Типы (гайд §2):
- деньги — `Decimal(18,2)`;
- количества — `UInt64`;
- `month` — `Date`;
- идентификаторы и тексты — `String`;
- флаги `is_unknown_category`, `is_translation_missing` — `UInt8`;
- `revenue_share` — `Float64`;
- **`Nullable(String)`** только там, где NULL допустим контрактом mart: `category_name_original`, `category_name_english`, `seller_city`, `seller_state` (Unknown-продавец). Остальные колонки не Nullable.

`refresh_status` (гайд §3.6): `batch_id String`, `published_at DateTime64(3, 'UTC')`, `source_rows UInt64`, `checks_passed UInt8`. Ровно одна строка — последняя успешная публикация.

Промежуточные `__stg`-таблицы создаёт пайплайн как `CREATE TABLE IF NOT EXISTS mart.<t>__stg AS mart.<t>` (не дублировать DDL). Если колонки существующей `mart.<t>` (имена/типы/порядок, `system.columns`) не совпадают с ожидаемыми из `mart.sql` — пайплайн падает с понятным сообщением, ничего не публикуя (миграции схемы делаем вручную).

### 3. `pipelines/mart_to_clickhouse.py` — переписать

Стиль — как `pipelines/source_to_raw.py`: русский docstring модуля (что делает, как запускать, почему так), без лишних абстракций. Список витрин — данные:

```python
# витрина -> ключ зерна (для проверки уникальности)
MARTS = {
    "monthly_sales": "month",
    "seller_sales": "seller_id",
    "order_summary": "summary_key",
    "customer_summary": "summary_key",
    "category_sales": "category_key",
}
```

Публичная функция `publish(batch_id: str) -> dict` (вызывается из DAG). Порядок — **сначала собрать и проверить весь комплект, потом переключить**:

1. Применить `infra/clickhouse/mart.sql`, создать `__stg`, проверить схему (п. 2).
2. Для каждой витрины: `TRUNCATE mart.<t>__stg` → `INSERT INTO mart.<t>__stg (<cols>) SELECT <cols> FROM postgresql(dwh, schema = 'mart', table = '<t>') SETTINGS insert_null_as_default = 0` — NULL в не-Nullable колонке должен ронять загрузку, а не превращаться молча в 0/''. Колонки — из `system.columns` целевой таблицы.
3. Проверки на `__stg` (любая неудача → исключение, опубликованные таблицы и `refresh_status` не тронуты):
   - `count()` в CH = `count(*)` в Postgres `mart.<t>` (через `DWH_DSN`), и > 0;
   - ключ уникален: `count() = uniqExact(<key>)`, ключ без NULL;
   - для витрин с `revenue_amount`: `sum(revenue_amount)` в CH точно (Decimal, без float) равна сумме в Postgres;
   - инварианты комплекта: `order_summary` и `customer_summary` — ровно 1 строка; `sum(revenue_amount)` равна между `monthly_sales`, `seller_sales`, `category_sales` и `order_summary`.
   Контрольные числа Olist (24 месяца, 74 категории, 13 221 498,11 …) в пайплайн **не** хардкодить — это регрессии датасета, их ловит dbt (`warn`).
4. Только после успеха всех проверок: `EXCHANGE TABLES mart.<t> AND mart.<t>__stg` по каждой витрине. В docstring честно написать: пять EXCHANGE — не одна атомарная транзакция, окно рассогласования между ними возможно.
5. Последним — `refresh_status` тем же способом (`__stg` → одна строка → `EXCHANGE`): `batch_id` = переданный, `published_at = now64(3, 'UTC')`, `source_rows` = сумма строк пяти витрин, `checks_passed = 1`.
6. Не делать `TRUNCATE`/`DROP` опубликованных таблиц. После EXCHANGE в `__stg` остаётся предыдущая версия — это нормально, следующий запуск её трункейтит.
7. Вернуть dict: `batch_id`, строки по витринам, `source_rows`. Логировать через `print` по одной строке на витрину (как в `source_to_raw.py`).

Креды: ClickHouse — `CLICKHOUSE_HOST/USER/PASSWORD` из env, Postgres — `DWH_DSN`. Пароль Postgres в SQL к ClickHouse не передаётся (только named collection `dwh`).

### 4. DAG `airflow/dags/marketplace_elt.py`

Задача `publish_clickhouse` (`@task`) после `dbt_build`, вызывает `publish(batch["batch_id"])`. Если `dbt_build` упал — публикация не запускается (по умолчанию `all_success`). Обновить docstring DAG (Марты и ClickHouse теперь в цепочке; ML и Superset — следующие этапы).

### 5. Прочее

- `Makefile`: цель `ch` → `-d mart`; добавить цель `publish` (ручная публикация: `docker compose exec airflow python -c "from pipelines.mart_to_clickhouse import publish; print(publish('manual'))"`) с комментарием `##` как у соседних целей.
- `.plans/marketplace-dwh-spec.md` §19: отметить пункт ClickHouse выполненным (`[x]`, кратко что сделано); пункт про Superset не трогать. В §19 «Под старую архитектуру» убрать `mart_to_clickhouse.py` и ссылки на `mp`, если они исправлены.
- Отчёт о приёмке — `.plans/clickhouse-publish-implementation.md` в стиле `.plans/mart-implementation.md`: что сделано, решения, команды и **фактический вывод** проверок, ограничения, вопросы к владельцу.

## Живой стенд

- Текущий том `clickhouse-data` содержит только пустую базу `mp` и пользователей `etl`/`bi`. Перед пересозданием **проверь**, что в `mp` и `default` нет таблиц (`system.tables`). Если пусто — разрешено `docker compose stop clickhouse && docker compose rm -f clickhouse && docker volume rm <project>_clickhouse-data && docker compose up -d clickhouse` (init-скрипты отработают заново, создадут `mart` и `bi_reader`). Если таблицы есть — остановиться и написать в «Вопросы к владельцу».
- После изменения compose — `docker compose up -d` (airflow при необходимости пересоздастся сам; образ пересобирать не нужно — новых зависимостей нет).
- dbt запускать только полным `dbt build` внутри контейнера: `docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build'` (частичный selector ломает DDS views, см. `mart-implementation.md`).

## Приёмка (всё — с фактическим выводом в отчёте)

1. Свежая инициализация: после пересоздания тома в ClickHouse есть база `mart`, пользователь `bi_reader`, нет базы `mp` и пользователя `bi`.
2. Ручная публикация `publish('manual-1')`, затем `publish('manual-2')`: количества строк в CH = Postgres, данные не удвоились, в `refresh_status` одна строка с `manual-2`.
3. Контроль из гайда §3 и §5 в ClickHouse **под `bi_reader`**: `SELECT count(), sum(revenue_amount) FROM mart.monthly_sales` → `24`, `13221498.11`; ноябрь 2016 с нулями; `order_summary` (96 478 / 13 221 498,11 / 2 202 965,54 / 15 424 463,65 / 110 197); `customer_summary` (96 096 / 2 997 / 93 099 / 99 441); лидер продавцов `4869f7a5dfa277a7dca6462dcf3b52b2` 226 987,93; 74 категории, лидер `beleza_saude` 1 233 131,72, сумма долей 1 ± 1e-9; три строки `is_unknown_category or is_translation_missing`. Сверка выручки четырёх витрин.
4. Типы в CH (`system.columns`) соответствуют п. 2.
5. `bi_reader` не может: `INSERT` в `mart.*`, `SELECT` из `postgresql(dwh, …)`, читать другие базы кроме `mart` (кроме системных, если CH их не закрывает — зафиксировать фактическое поведение).
6. Отказоустойчивость: смоделировать падение проверки **без правки рабочего кода** (например, в отдельном `python -c` подменить функцию получения ожидаемых значений из Postgres через monkeypatch) → исключение, опубликованные `mart.*` и `refresh_status` не изменились (сравнить подписи/`batch_id` до и после).
7. Полный прогон DAG: `airflow dags unpause` + `airflow dags trigger marketplace_elt`, дождаться `success` всех задач, включая `publish_clickhouse`; `refresh_status.batch_id` = `run_id` этого запуска. Второй прогон DAG — данные не удвоились.
8. `grep` из п. 1 ТЗ пуст; в `pipelines/` нет слова Cloudberry.

## Ограничения

- Не менять staging / ODS / DDS / mart-модели dbt, тесты dbt, `dwh/init.sql`, EL, ML (`marts/`, `notebooks/`, `ml-docs/`).
- Superset в этом ТЗ **не** делаем (сервис, подключение, датасеты — отдельная задача).
- Никаких коммитов и git-операций, кроме чтения.
- Развилки дизайна не решать молча — в раздел «Вопросы к владельцу» отчёта.
