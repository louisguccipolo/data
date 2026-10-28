# ТЗ: шлифовка витрин блока Б под дашборды — 28.10.2026

Закрывает два вопроса к владельцу из `.plans/clickhouse-publish-implementation.md`.
Цель — витрины без «технических» типов и NULL в ключах, чтобы Superset показывал их без ухищрений.
Superset в стенд не добавляем: аналитик подключает его локально.

## Решения владельца

1. **Масштаб чисел — разумный, фиксированный.**
   - Суммы и средние в днях: `delivery_by_state.delivery_days_sum` → `numeric(18,2)`;
     `avg_delivery_days` в `delivery_by_state` и `review_delivery` → `numeric(10,2)`.
   - `avg_installments` в `payments_by_type` и `payments_by_state_year` → `numeric(10,2)`.
   - Доли (`late_share`, `order_share`, `revenue_share`) остаются `double precision` / `Float64`
     (Superset форматирует их как проценты); значения не округлять.
   - Округление — `round(…, 2)` до каста, чтобы сумма `delivery_days_sum` не теряла точность больше копейки дня.
2. **Ключи зерна — NOT NULL.** `product_price_history.sale_date`, `payments_by_state_year.purchase_year`,
   `review_delivery.review_score`: в dbt добавить тест `not_null`; в ClickHouse — не Nullable,
   `SETTINGS allow_nullable_key = 1` убрать у этих таблиц. В текущих данных NULL там нет (проверено) —
   при появлении сборка должна падать громко, а не публиковать дыру. Логику витрин (LEFT JOIN и т.п.) не менять.
3. **Подписи без NULL.** `delivery_by_state.state_name` / `region` — `coalesce(…, 'Unknown')`, в CH `String`
   без Nullable. Остальные Nullable-подписи (`category_name_*`, `seller_city/state`) не трогать — для них уже есть
   `category_label` и т.п.
4. Средние/доли, которые по смыслу «нет измерений» (`avg_delivery_days`, `late_share`, `order_share`), остаются Nullable:
   NULL честнее нуля.

## Что поменять

- `dbt/models/mart/*.sql` и `_mart.yml` (тесты + описания типов).
- `infra/clickhouse/mart.sql` — типы по п. 1–3.
- Миграция ClickHouse: пайплайн падает на расхождении схемы, поэтому изменённые таблицы (и их `__stg`)
  нужно пересоздать. Добавить в `Makefile` цель `ch-reset-mart` (DROP изменённых таблиц + `__stg`, затем `publish`)
  или аналогичную разовую процедуру; описать её в отчёте.
- Проверки пайплайна (`pipelines/mart_to_clickhouse.py`) поправить, только если ломаются от смены типов.
  Сверка выручки остаётся точной; для `delivery_days_sum` допускается сравнение после округления до 2 знаков.

## Приёмка

1. `dbt build --select mart` в `docker compose exec -T airflow` — зелёный, новые `not_null` проходят.
2. В Postgres `information_schema.columns`: указанные колонки имеют `numeric(18,2)` / `numeric(10,2)`.
3. В ClickHouse `system.columns` (без `__stg`): нет `Decimal(38,…)`; ключи блока Б не Nullable;
   `allow_nullable_key` не встречается в `SHOW CREATE` таблиц блока Б.
4. Публикация + прогон DAG `marketplace_elt` — `success`; количества строк как в прошлой приёмке (95 877),
   выручка 13 221 498,11 во всех четырёх витринах.
5. Под `bi_reader`: `select * from mart.review_delivery order by review_score` и
   `select customer_state, state_name, avg_delivery_days from mart.delivery_by_state order by avg_delivery_days desc limit 5`
   — значения с 2 знаками, без NULL в подписях.
6. Отчёт — дописать раздел «Шлифовка блока Б» в `.plans/clickhouse-publish-implementation.md` с фактическим выводом.

Без коммитов. Дизайн модели (ods/dds, зерно витрин) не менять — развилки записать в «Вопросы к владельцу».
