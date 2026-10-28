# Приёмка публикации mart → ClickHouse — 28.10.2026

ТЗ — `.plans/clickhouse-publish-tz.md` (расширено владельцем 28.10 до всех 11 витрин). Реализация — коммит `e529b98`.

## Что реализовано

- База ClickHouse `mart` вместо `mp`: `compose.yaml` (`CLICKHOUSE_DB: mart`), `Makefile` (цель `ch` → `-d mart`, новая цель `publish`).
- Пользователь `bi_reader` (`readonly = 1`, `GRANT SELECT ON mart.*`) в `infra/clickhouse/init/02_bi_user.sh`; старого `bi` нет.
- DDL — один файл `infra/clickhouse/mart.sql`: 11 витрин блоков А и Б + `refresh_status`, `MergeTree`, только `IF NOT EXISTS`. Применяется пайплайном в начале каждого запуска.
- `pipelines/mart_to_clickhouse.py`, функция `publish(batch_id)`:
  1. применяет `mart.sql`, создаёт `<t>__stg AS mart.<t>`, сверяет колонки `system.columns` с DDL — расхождение роняет запуск до загрузки;
  2. `INSERT … SELECT FROM postgresql(dwh, schema = 'mart', …)` в `__stg` с `insert_null_as_default = 0`;
  3. проверяет весь комплект: count CH = Postgres и > 0, уникальность ключа зерна, точная сумма `revenue_amount`, по одной строке в `order_summary` / `customer_summary`, равенство выручки месяцев / продавцов / категорий / итога;
  4. только после этого — `EXCHANGE TABLES` по каждой витрине, `refresh_status` последним.
- DAG `marketplace_elt`: задача `publish_clickhouse` после `dbt_build` (правило `all_success`).

Ключи зерна блока Б: `product_price_history` — (`product_id`, `sale_date`, `price`), `order_status_funnel` — `order_status`, `payments_by_type` — `payment_type`, `payments_by_state_year` — (`customer_state`, `purchase_year`), `delivery_by_state` — `customer_state`, `review_delivery` — `review_score`.

## Принятые решения

- ClickHouse сам тянет данные через named collection `dwh` (вариант A); пароль Postgres в SQL не передаётся. Live-движки и перекачка через Python не используются.
- Опубликованные таблицы не трункейтятся и не удаляются; после `EXCHANGE` в `__stg` остаётся прошлая версия, следующий запуск её трункейтит.
- Контрольные числа Olist в пайплайн не зашиты — это регрессии датасета, их ловит dbt.

## Приёмка и фактический вывод

**1. Инициализация.** `system.databases`: `INFORMATION_SCHEMA`, `default`, `information_schema`, `mart`, `system` — `mp` нет. `system.users`: `etl`, `bi_reader` — `bi` нет.

**2. Повторная публикация.** `publish('check-1')`, затем `publish('check-2')` — по каждой витрине `проверки пройдены`:

| Витрина | Строк |
|---|---:|
| monthly_sales | 24 |
| seller_sales | 2 970 |
| order_summary | 1 |
| customer_summary | 1 |
| category_sales | 74 |
| product_price_history | 92 686 |
| order_status_funnel | 9 |
| payments_by_type | 5 |
| payments_by_state_year | 75 |
| delivery_by_state | 27 |
| review_delivery | 5 |

`source_rows = 95877` оба раза, удвоения нет. `refresh_status`: одна строка `check-2 | 2026-10-28 13:59:53.947 | 95877 | 1`.

**3. Контрольные цифры под `bi_reader`.**
- `monthly_sales`: `24 | 13221498.11`.
- Лидер продавцов: `4869f7a5dfa277a7dca6462dcf3b52b2 | 226987.93`.
- `category_sales`: `74 | sum(revenue_share) = 1.0000000000000002 | 3` строки Unknown / без перевода.
- Выручка четырёх витрин: `13221498.11 | 13221498.11 | 13221498.11 | 13221498.11`.

**4. Типы (`system.columns`, без `__stg`).** `UInt64` ×33, `String` ×12, `Decimal(18,2)` ×9, `Decimal(38,19)` ×1, `Float64` ×3, `UInt8` ×3, `Date` ×1, `DateTime64(3,'UTC')` ×1; Nullable: `String` ×6, `Float64` ×5, `Int32` ×2, `Date` ×1. Nullable в блоке А — только `category_name_original`, `category_name_english`, `seller_city`, `seller_state`, как в ТЗ.

**5. Права `bi_reader`.** Все три попытки — `ACCESS_DENIED` (Code 497):
- `INSERT` в `mart.payments_by_type`;
- `SELECT` из `postgresql(dwh, …)` — нужен `NAMED COLLECTION ON dwh`;
- `CREATE TABLE default.x`.

**6. Отказоустойчивость.** В отдельном `python -c` подменён `_source_totals`: для `review_delivery` ожидаемое число строк +1. Результат:

```
FAILED OK: mart.review_delivery: в ClickHouse 5 строк, в Postgres 6; требуется равенство и > 0
```

После сбоя `refresh_status.batch_id = check-2`, `mart.review_delivery` — 5 строк: опубликованный комплект не тронут.

**7. Прогон DAG.** `ch_publish_acceptance_1__20261028T1400` — `success`, `ch_publish_acceptance_2__20261028T1400` — `success`. После второго прогона `refresh_status.batch_id = ch_publish_acceptance_2__20261028T1400`, `source_rows = 95877`, `seller_sales` — 2 970 строк (не удвоилось).

**8. Остатки старой архитектуры.** `grep -rnw mp` по compose, Makefile, infra, pipelines, airflow — только `name: mp-dwh` (имя проекта compose, не база). `grep -i cloudberry pipelines` — пусто.

## Ограничения

- 11 `EXCHANGE` — не одна атомарная транзакция: между ними возможно короткое окно, когда часть витрин уже новая, часть ещё старая. `refresh_status` переключается последним.
- Изменение схемы существующей таблицы пайплайн не мигрирует — падает с сообщением; миграции делаются вручную.
- Superset не подключён — отдельный этап.

## Вопросы к владельцу

- В блоке Б Nullable и части ключей зерна: `product_price_history.sale_date`, `payments_by_state_year.purchase_year`, `review_delivery.review_score`. Сейчас уникальность ключа проверяется, но NULL в ключе допустим контрактом mart. Оставить так или требовать NOT NULL в dbt?
- `delivery_by_state.delivery_days_sum` в ClickHouse — `Decimal(38,19)`: сумма дней доставки, в Postgres `numeric` без масштаба. Оставить или сузить тип в `mart.sql`?
