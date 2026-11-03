# Приёмка публикации mart → ClickHouse — 28.10.2026

## Результат

Реализовано исходное ТЗ и расширение владельца: публикуются **все 11 витрин**
DWH `mart` (73 колонки), затем `refresh_status` (4 колонки). На стенде опубликовано
**95 877 строк**, все количества под `bi_reader` равны Postgres. Два последовательных
прогона `marketplace_elt` завершились `success` всех задач с первой попытки;
подписи данных после прогонов совпали. Последний успешный batch:
`ch_publish_acceptance_2__20261028T1400`.

Пункты приёмки 1–7 выполнены, включая исходные `manual-1` / `manual-2`, расширенный
`manual-3`, права BI и сохранность публикации при отказах. В пункте 8 есть явно
зафиксированное исключение буквального grep: `mp-dwh` — имя Compose-проекта;
`mp-airflow` — разрешённое ТЗ имя образа. Имени базы ClickHouse `mp` в проверяемом
коде больше нет. Ограничение точности `numeric` без typmod в `postgresql()` и
семантика Nullable-зерна блока Б разобраны в «Вопросах к владельцу».

Вывод ниже взят из `/tmp/ch-publish-acceptance/*.log`, ANSI-цвета удалены.
`[exit_code=...]` добавлен скриптом захвата после реального процесса.
Время в логах — UTC. SQL и проверочные скрипты приведены для воспроизведения.

## Что реализовано

- `infra/clickhouse/mart.sql`: единственный идемпотентный DDL, только CREATE DATABASE /
  CREATE TABLE IF NOT EXISTS. 11 MergeTree-витрин и статус; порядок колонок совпадает
  с Postgres и `_mart.yml`. Init не дублирует DDL.
- `pipelines/mart_to_clickhouse.py`: `publish(batch_id)` применяет DDL, создаёт `__stg`
  через AS целевой таблицы, сверяет имена/типы/порядок **всех** целевых и промежуточных
  таблиц до загрузки; читает строки через `postgresql(dwh, …)`, Python получает
  только агрегаты. Колонки INSERT берутся из `system.columns` целевых таблиц.
- Для всех 11 витрин проверяются непустота, count Postgres = ClickHouse, уникальность
  зерна. При наличии `revenue_amount` проверяется точная сумма Decimal; равенство
  выручки между витринами относится только к четырём витринам блока А. Обе summary
  должны содержать по одной строке. Контрольные числа Olist в пайплайне не зашиты.
- После всех проверок выполняются 11 EXCHANGE; статус переключается последним,
  содержит одну строку, UTC DateTime64(3), сумму строк 11 витрин и checks_passed=1.
  Опубликованные таблицы не TRUNCATE / DROP; предыдущие версии остаются в `__stg`.
- `airflow/dags/marketplace_elt.py`: `publish_clickhouse` после полного `dbt_build`,
  batch_id из `open_batch`, trigger_rule `all_success`, max_active_runs=1.
- `compose.yaml`: CLICKHOUSE_DB=mart. Init создаёт только `bi_reader` с readonly=1
  и SELECT ON mart.*. `Makefile`: `ch` открывает mart, добавлена цель `publish`.
- В ТЗ добавлена строка расширения владельцем; §19 и пункт ClickHouse §20 основной
  спецификации отмечены выполненными. Пункт Superset сохранён.

dbt-модели/тесты, DDS, EL, ML и `.context/` в рамках этой задачи не редактировались.
Существовавшие изменения других работ сохранены. После последнего указания владельца
git-команды не выполнялись; ранее использовались только чтение status/diff.

## Решения по блоку Б

| Витрина | Зерно / ORDER BY | Допустимые Nullable-колонки |
|---|---|---|
| product_price_history | product_id, sale_date, price | sale_date |
| order_status_funnel | order_status | order_share |
| payments_by_type | payment_type | нет |
| payments_by_state_year | customer_state, purchase_year | purchase_year |
| delivery_by_state | customer_state | state_name, region, avg_delivery_days, late_share |
| review_delivery | review_score | review_score, avg_delivery_days, late_share |

Деньги и цена — Decimal(18,2), счётчики — UInt64. Год и оценка сохраняют
целочисленный смысл PostgreSQL integer как Int32. Средние и доли блока Б — Float64,
с Nullable по контракту. `delivery_days_sum` — Decimal(38,19), тип, фактически
предоставляемый table function для исходного numeric без typmod; см. ограничение ниже.

Для Nullable-компонентов ключа включён `allow_nullable_key=1`. Уникальность
проверяется `uniqExact(col1, col2, ...)`; Nullable-аргумент обёрнут в `tuple(col)`:
иначе ClickHouse исключает NULL из uniqExact и отвергает разрешённую контрактом
NULL-группу. Обязательные компоненты ключа проверяются на отсутствие NULL.
Фактическая проверка `values` ниже подтверждает разницу. У блока А все прежние
ограничения на NULL сохранены.

### Фактические типы и NULL в Postgres

Команда: `docker compose exec -T dwh psql -X -U dwh -d dwh` со следующим SQL:

```sql
SELECT table_name, ordinal_position, column_name, data_type, numeric_precision, numeric_scale
FROM information_schema.columns WHERE table_schema='mart' ORDER BY table_name, ordinal_position;
SELECT format('SELECT %L AS model, count(*) AS rows, %s FROM mart.%I;', table_name,
    string_agg(format('count(*) FILTER (WHERE %I IS NULL) AS %I', column_name, column_name),
               ', ' ORDER BY ordinal_position), table_name)
FROM information_schema.columns WHERE table_schema='mart' GROUP BY table_name ORDER BY table_name \gexec
SELECT min(delivery_days_sum), max(delivery_days_sum), max(scale(delivery_days_sum)) AS max_scale,
       max(scale(avg_delivery_days)) AS avg_scale FROM mart.delivery_by_state;
SELECT max(scale(avg_installments)) AS max_scale FROM mart.payments_by_type;
SELECT * FROM mart.review_delivery ORDER BY review_score;
```

```text
$ docker compose exec -T dwh psql -X -U dwh -d dwh
       table_name       | ordinal_position |        column_name         |    data_type     | numeric_precision | numeric_scale 
------------------------+------------------+----------------------------+------------------+-------------------+---------------
 category_sales         |                1 | category_key               | text             |                   |              
 category_sales         |                2 | category_name_original     | text             |                   |              
 category_sales         |                3 | category_name_english      | text             |                   |              
 category_sales         |                4 | category_label             | text             |                   |              
 category_sales         |                5 | is_unknown_category        | boolean          |                   |              
 category_sales         |                6 | is_translation_missing     | boolean          |                   |              
 category_sales         |                7 | orders_count               | bigint           |                64 |             0
 category_sales         |                8 | unit_count                 | bigint           |                64 |             0
 category_sales         |                9 | revenue_amount             | numeric          |                18 |             2
 category_sales         |               10 | revenue_share              | double precision |                53 |              
 customer_summary       |                1 | summary_key                | text             |                   |              
 customer_summary       |                2 | customers_count            | bigint           |                64 |             0
 customer_summary       |                3 | repeat_customers_count     | bigint           |                64 |             0
 customer_summary       |                4 | one_order_customers_count  | bigint           |                64 |             0
 customer_summary       |                5 | orders_count               | bigint           |                64 |             0
 delivery_by_state      |                1 | customer_state             | text             |                   |              
 delivery_by_state      |                2 | state_name                 | text             |                   |              
 delivery_by_state      |                3 | region                     | text             |                   |              
 delivery_by_state      |                4 | orders_count               | bigint           |                64 |             0
 delivery_by_state      |                5 | delivered_measured_count   | bigint           |                64 |             0
 delivery_by_state      |                6 | delivery_days_sum          | numeric          |                   |              
 delivery_by_state      |                7 | avg_delivery_days          | numeric          |                   |              
 delivery_by_state      |                8 | late_count                 | bigint           |                64 |             0
 delivery_by_state      |                9 | late_share                 | double precision |                53 |              
 monthly_sales          |                1 | month                      | date             |                   |              
 monthly_sales          |                2 | orders_all_count           | bigint           |                64 |             0
 monthly_sales          |                3 | orders_delivered_count     | bigint           |                64 |             0
 monthly_sales          |                4 | revenue_amount             | numeric          |                18 |             2
 order_status_funnel    |                1 | order_status               | text             |                   |              
 order_status_funnel    |                2 | status_group               | text             |                   |              
 order_status_funnel    |                3 | orders_count               | bigint           |                64 |             0
 order_status_funnel    |                4 | order_share                | double precision |                53 |              
 order_status_funnel    |                5 | orders_without_items_count | bigint           |                64 |             0
 order_summary          |                1 | summary_key                | text             |                   |              
 order_summary          |                2 | orders_count               | bigint           |                64 |             0
 order_summary          |                3 | revenue_amount             | numeric          |                18 |             2
 order_summary          |                4 | freight_amount             | numeric          |                18 |             2
 order_summary          |                5 | order_value_amount         | numeric          |                18 |             2
 order_summary          |                6 | line_count                 | bigint           |                64 |             0
 order_summary          |                7 | unit_count                 | bigint           |                64 |             0
 payments_by_state_year |                1 | customer_state             | text             |                   |              
 payments_by_state_year |                2 | purchase_year              | integer          |                32 |             0
 payments_by_state_year |                3 | payments_count             | bigint           |                64 |             0
 payments_by_state_year |                4 | payment_amount             | numeric          |                18 |             2
 payments_by_state_year |                5 | installments_sum           | bigint           |                64 |             0
 payments_by_state_year |                6 | avg_installments           | numeric          |                   |              
 payments_by_state_year |                7 | zero_installments_count    | bigint           |                64 |             0
 payments_by_type       |                1 | payment_type               | text             |                   |              
 payments_by_type       |                2 | payments_count             | bigint           |                64 |             0
 payments_by_type       |                3 | payment_amount             | numeric          |                18 |             2
 payments_by_type       |                4 | installments_sum           | bigint           |                64 |             0
 payments_by_type       |                5 | avg_installments           | numeric          |                   |              
 payments_by_type       |                6 | zero_installments_count    | bigint           |                64 |             0
 product_price_history  |                1 | product_id                 | text             |                   |              
 product_price_history  |                2 | sale_date                  | date             |                   |              
 product_price_history  |                3 | price                      | numeric          |                18 |             2
 product_price_history  |                4 | unit_count                 | bigint           |                64 |             0
 product_price_history  |                5 | orders_count               | bigint           |                64 |             0
 product_price_history  |                6 | distinct_price_count       | bigint           |                64 |             0
 product_price_history  |                7 | product_unit_count         | bigint           |                64 |             0
 review_delivery        |                1 | review_score               | integer          |                32 |             0
 review_delivery        |                2 | orders_count               | bigint           |                64 |             0
 review_delivery        |                3 | delivered_measured_count   | bigint           |                64 |             0
 review_delivery        |                4 | avg_delivery_days          | numeric          |                   |              
 review_delivery        |                5 | late_count                 | bigint           |                64 |             0
 review_delivery        |                6 | late_share                 | double precision |                53 |              
 seller_sales           |                1 | seller_id                  | text             |                   |              
 seller_sales           |                2 | seller_city                | text             |                   |              
 seller_sales           |                3 | seller_state               | text             |                   |              
 seller_sales           |                4 | orders_count               | bigint           |                64 |             0
 seller_sales           |                5 | line_count                 | bigint           |                64 |             0
 seller_sales           |                6 | unit_count                 | bigint           |                64 |             0
 seller_sales           |                7 | revenue_amount             | numeric          |                18 |             2
(73 rows)

     model      | rows | category_key | category_name_original | category_name_english | category_label | is_unknown_category | is_translation_missing | orders_count | unit_count | revenue_amount | revenue_share 
----------------+------+--------------+------------------------+-----------------------+----------------+---------------------+------------------------+--------------+------------+----------------+---------------
 category_sales |   74 |            0 |                      1 |                     3 |              0 |                   0 |                      0 |            0 |          0 |              0 |             0
(1 row)

      model       | rows | summary_key | customers_count | repeat_customers_count | one_order_customers_count | orders_count 
------------------+------+-------------+-----------------+------------------------+---------------------------+--------------
 customer_summary |    1 |           0 |               0 |                      0 |                         0 |            0
(1 row)

       model       | rows | customer_state | state_name | region | orders_count | delivered_measured_count | delivery_days_sum | avg_delivery_days | late_count | late_share 
-------------------+------+----------------+------------+--------+--------------+--------------------------+-------------------+-------------------+------------+------------
 delivery_by_state |   27 |              0 |          0 |      0 |            0 |                        0 |                 0 |                 0 |          0 |          0
(1 row)

     model     | rows | month | orders_all_count | orders_delivered_count | revenue_amount 
---------------+------+-------+------------------+------------------------+----------------
 monthly_sales |   24 |     0 |                0 |                      0 |              0
(1 row)

        model        | rows | order_status | status_group | orders_count | order_share | orders_without_items_count 
---------------------+------+--------------+--------------+--------------+-------------+----------------------------
 order_status_funnel |    9 |            0 |            0 |            0 |           0 |                          0
(1 row)

     model     | rows | summary_key | orders_count | revenue_amount | freight_amount | order_value_amount | line_count | unit_count 
---------------+------+-------------+--------------+----------------+----------------+--------------------+------------+------------
 order_summary |    1 |           0 |            0 |              0 |              0 |                  0 |          0 |          0
(1 row)

         model          | rows | customer_state | purchase_year | payments_count | payment_amount | installments_sum | avg_installments | zero_installments_count 
------------------------+------+----------------+---------------+----------------+----------------+------------------+------------------+-------------------------
 payments_by_state_year |   75 |              0 |             0 |              0 |              0 |                0 |                0 |                       0
(1 row)

      model       | rows | payment_type | payments_count | payment_amount | installments_sum | avg_installments | zero_installments_count 
------------------+------+--------------+----------------+----------------+------------------+------------------+-------------------------
 payments_by_type |    5 |            0 |              0 |              0 |                0 |                0 |                       0
(1 row)

         model         | rows  | product_id | sale_date | price | unit_count | orders_count | distinct_price_count | product_unit_count 
-----------------------+-------+------------+-----------+-------+------------+--------------+----------------------+--------------------
 product_price_history | 92686 |          0 |         0 |     0 |          0 |            0 |                    0 |                  0
(1 row)

      model      | rows | review_score | orders_count | delivered_measured_count | avg_delivery_days | late_count | late_share 
-----------------+------+--------------+--------------+--------------------------+-------------------+------------+------------
 review_delivery |    5 |            0 |            0 |                        0 |                 0 |          0 |          0
(1 row)

    model     | rows | seller_id | seller_city | seller_state | orders_count | line_count | unit_count | revenue_amount 
--------------+------+-----------+-------------+--------------+--------------+------------+------------+----------------
 seller_sales | 2970 |         0 |           0 |            0 |            0 |          0 |          0 |              0
(1 row)

          min          |             max             | max_scale | avg_scale 
-----------------------+-----------------------------+-----------+-----------
 1204.8893865740740739 | 354783.56452546296296646668 |        20 |        20
(1 row)

 max_scale 
-----------
        20
(1 row)

 review_score | orders_count | delivered_measured_count |    avg_delivery_days    | late_count |      late_share      
--------------+--------------+--------------------------+-------------------------+------------+----------------------
            1 |        11366 |                     9353 | 21.32474074098823510765 |       3432 |  0.36694108842082757
            2 |         3129 |                     2919 |     16.6273869436197074 |        550 |   0.1884206920178143
            3 |         8133 |                     7916 | 14.25623451185362977985 |        694 |  0.08767054067710965
            4 |        19044 |                    18894 | 12.30595928883458985619 |        649 | 0.034349528950989736
            5 |        57001 |                    56742 | 10.68097187892860080141 |       1056 | 0.018610553029501955
(5 rows)


[exit_code=0]
```

## 1. Безопасная свежая инициализация

Проверка перед удалением: реальный mount контейнера и отсутствие любых
пользовательских таблиц; в `mp` и `default` по нулю таблиц. Только после этого
пересоздан `mp-dwh_clickhouse-data`. Остальные тома не удалялись и не пересоздавались.

SQL проверки пустоты:

```sql
SELECT 'mp' AS db_name, count() AS table_count FROM system.tables AS t WHERE t.database = 'mp'
UNION ALL SELECT 'default', count() FROM system.tables AS t WHERE t.database = 'default';
SELECT database, name FROM system.tables
WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY database, name;
```

```text
$ docker inspect mp-dwh-clickhouse-1 --format '{{range .Mounts}}{{if eq .Destination "/var/lib/clickhouse"}}{{.Type}} {{.Name}}{{end}}{{end}}'
volume mp-dwh_clickhouse-data

[exit_code=0]
```

```text
$ docker compose exec -T clickhouse bash -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery'
mp	0
default	0

[exit_code=0]
```

```text
$ docker compose stop clickhouse
 Container mp-dwh-clickhouse-1  Stopping
 Container mp-dwh-clickhouse-1  Stopped

[exit_code=0]
```

```text
$ docker compose rm -f clickhouse
Going to remove mp-dwh-clickhouse-1
 Container mp-dwh-clickhouse-1  Removing
 Container mp-dwh-clickhouse-1  Removed

[exit_code=0]
```

```text
$ docker volume rm mp-dwh_clickhouse-data
mp-dwh_clickhouse-data

[exit_code=0]
```

```text
$ docker compose up -d clickhouse
 Volume "mp-dwh_clickhouse-data"  Creating
 Volume "mp-dwh_clickhouse-data"  Created
 Container mp-dwh-clickhouse-1  Creating
 Container mp-dwh-clickhouse-1  Created
 Container mp-dwh-clickhouse-1  Starting
 Container mp-dwh-clickhouse-1  Started

[exit_code=0]
```

```text
$ docker compose up -d
 Container mp-dwh-airflow-db-1  Running
 Container mp-dwh-source-1  Running
 Container mp-dwh-clickhouse-1  Running
 Container mp-dwh-dwh-1  Running
 Container mp-dwh-airflow-1  Running
 Container mp-dwh-dwh-1  Waiting
 Container mp-dwh-airflow-db-1  Waiting
 Container mp-dwh-source-1  Waiting
 Container mp-dwh-dwh-1  Waiting
 Container mp-dwh-clickhouse-1  Waiting
 Container mp-dwh-airflow-db-1  Healthy
 Container mp-dwh-source-1  Healthy
 Container mp-dwh-dwh-1  Healthy
 Container mp-dwh-dwh-1  Healthy
 Container mp-dwh-dwh-init-1  Starting
 Container mp-dwh-dwh-init-1  Started
 Container mp-dwh-clickhouse-1  Healthy

[exit_code=0]
```

Свежая инициализация: SHOW DATABASES, SHOW USERS, SHOW GRANTS FOR bi_reader;
SELECT name, engine FROM system.databases WHERE name IN ('mart','mp').
Строка `readonly=0` в этом выводе относится к подключению **etl**; значение
`readonly=1` для bi_reader отдельно подтверждено контрольными запросами ниже.

```text
$ docker compose exec -T clickhouse bash -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery'
INFORMATION_SCHEMA
default
information_schema
mart
system
bi_reader
etl
GRANT SELECT ON mart.* TO bi_reader
readonly	0
mart	Atomic

[exit_code=0]
```

## 2. Полная сборка dbt

Единственный ручной запуск dbt в этой задаче — полный build в контейнере.
В каждом из двух DAG также выполнен полный build. Частичные selectors не запускались.
Три warn — прежние DDS: carrier_before_purchase=166, approved_missing=14,
delivered_without_date=8. Число узлов 488 учитывает завершённый блок Б.

```text
$ docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build'
[ниже — фактический конец вывода]
13:48:23  486 of 488 START test mart_block_b_shares ...................................... [RUN]
13:48:23  484 of 488 PASS not_null_product_price_history_product_id ...................... [PASS in 0.06s]
13:48:23  487 of 488 START test mart_review_reconciliation ............................... [RUN]
13:48:23  486 of 488 PASS mart_block_b_shares ............................................ [PASS in 0.06s]
13:48:23  488 of 488 START test unique_review_delivery_coalesce_review_score_0_ .......... [RUN]
13:48:23  488 of 488 PASS unique_review_delivery_coalesce_review_score_0_ ................ [PASS in 0.03s]
13:48:23  482 of 488 PASS mart_price_reconciliation ...................................... [PASS in 0.46s]
13:48:23  485 of 488 PASS unique_product_price_history_jsonb_build_array_product_id_sale_date_price_  [PASS in 0.22s]
13:48:23  487 of 488 PASS mart_review_reconciliation ..................................... [PASS in 0.19s]
13:48:23  
13:48:23  Finished running 34 incremental models, 3 seeds, 32 table models, 402 data tests, 17 view models in 0 hours 0 minutes and 59.53 seconds (59.53s).
13:48:23  
13:48:23  Completed with 3 warnings:
13:48:23  
13:48:23  Warning in test dds_warn_is_carrier_before_purchase (tests/dds_warn_is_carrier_before_purchase.sql)
13:48:23  Got 166 results, configured to warn if != 0
13:48:23  
13:48:23    compiled code at target/compiled/marketplace/tests/dds_warn_is_carrier_before_purchase.sql
13:48:23  
13:48:23  Warning in test dds_warn_is_approved_missing (tests/dds_warn_is_approved_missing.sql)
13:48:23  Got 14 results, configured to warn if != 0
13:48:23  
13:48:23    compiled code at target/compiled/marketplace/tests/dds_warn_is_approved_missing.sql
13:48:23  
13:48:23  Warning in test dds_warn_is_delivered_without_date (tests/dds_warn_is_delivered_without_date.sql)
13:48:23  Got 8 results, configured to warn if != 0
13:48:23  
13:48:23    compiled code at target/compiled/marketplace/tests/dds_warn_is_delivered_without_date.sql
13:48:23  
13:48:23  Done. PASS=485 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=488

[exit_code=0]
```

## 3. Ручные публикации и идемпотентность

На первом этапе, до расширения владельцем, выполнены `publish('manual-1')` и
`publish('manual-2')`. Для каждого набора посчитаны SHA-256 полных строк в порядке
ключа, затем количества сверены с Postgres; статус проверен на одну строку manual-2.
Ниже фактический вывод `docker compose exec -T airflow python -` с этим сценарием.

```text
$ docker compose exec -T airflow python -
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
{'batch_id': 'manual-1', 'rows': {'monthly_sales': 24, 'seller_sales': 2970, 'order_summary': 1, 'customer_summary': 1, 'category_sales': 74}, 'source_rows': 3070}
manual-1 signatures: {'monthly_sales': 'c2a6aa81e79fadbf0e616ca294be187c4345fc165fe30e8bb65e78351e6e0bfa', 'seller_sales': 'a22530977c68cfea9aee67b38b7fa5e7c1fa8d0c106598c0b77eec101f4d1a05', 'order_summary': 'e002cfa20177fd3fc766d05f24f4af921f35c769473c620cb4b843d0d123a32b', 'customer_summary': '6dda1cd32843a3caab1113dddd165721d81ae3d9441e38874e89f44bc5706533', 'category_sales': 'd65909da202698d325408c3848be850898da72dfc76da5adfe58914e63088099'}
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
{'batch_id': 'manual-2', 'rows': {'monthly_sales': 24, 'seller_sales': 2970, 'order_summary': 1, 'customer_summary': 1, 'category_sales': 74}, 'source_rows': 3070}
manual-2 signatures: {'monthly_sales': 'c2a6aa81e79fadbf0e616ca294be187c4345fc165fe30e8bb65e78351e6e0bfa', 'seller_sales': 'a22530977c68cfea9aee67b38b7fa5e7c1fa8d0c106598c0b77eec101f4d1a05', 'order_summary': 'e002cfa20177fd3fc766d05f24f4af921f35c769473c620cb4b843d0d123a32b', 'customer_summary': '6dda1cd32843a3caab1113dddd165721d81ae3d9441e38874e89f44bc5706533', 'category_sales': 'd65909da202698d325408c3848be850898da72dfc76da5adfe58914e63088099'}
monthly_sales: ClickHouse=24, Postgres=24
seller_sales: ClickHouse=2970, Postgres=2970
order_summary: ClickHouse=1, Postgres=1
customer_summary: ClickHouse=1, Postgres=1
category_sales: ClickHouse=74, Postgres=74
refresh_status: [('manual-2', datetime.datetime(2026, 10, 28, 13, 48, 52, 211000), 3070, 1)]
MANUAL_REPEAT_PASSED

[exit_code=0]
```

После расширения выполнен `publish('manual-3')`. Дополнительно сравнены SHA-256
полных строк прежних пяти витрин до/после; сравнение внутри этого сценария использует
сортировку по repr строки. Идемпотентный CREATE не изменил прежнюю схему; публикация
переключила проверенные данные штатным EXCHANGE.

```text
$ docker compose exec -T airflow python -
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
mart.product_price_history: строк=92686; проверки пройдены
mart.order_status_funnel: строк=9; проверки пройдены
mart.payments_by_type: строк=5; проверки пройдены
mart.payments_by_state_year: строк=75; проверки пройдены
mart.delivery_by_state: строк=27; проверки пройдены
mart.review_delivery: строк=5; проверки пройдены
{'batch_id': 'manual-3', 'rows': {'monthly_sales': 24, 'seller_sales': 2970, 'order_summary': 1, 'customer_summary': 1, 'category_sales': 74, 'product_price_history': 92686, 'order_status_funnel': 9, 'payments_by_type': 5, 'payments_by_state_year': 75, 'delivery_by_state': 27, 'review_delivery': 5}, 'source_rows': 95877}
EXISTING_FIVE_TABLES_UNCHANGED: {'monthly_sales': '47c5cf4708b76609e0567d01425f8eb4999564ebec2e9d7a4a1b2ab3f3156fbe', 'seller_sales': 'a22530977c68cfea9aee67b38b7fa5e7c1fa8d0c106598c0b77eec101f4d1a05', 'order_summary': 'e002cfa20177fd3fc766d05f24f4af921f35c769473c620cb4b843d0d123a32b', 'customer_summary': '6dda1cd32843a3caab1113dddd165721d81ae3d9441e38874e89f44bc5706533', 'category_sales': 'd65909da202698d325408c3848be850898da72dfc76da5adfe58914e63088099'}
refresh_status: [('manual-3', datetime.datetime(2026, 10, 28, 13, 58, 0, 348000), 95877, 1)]
MANUAL_3_PASSED

[exit_code=0]
```

## 4. Контроль блока А под bi_reader

Команда в логе передаёт пароль только через env внутри контейнера.
Средний чек вычислен `divideDecimal` с десятью дробными знаками: обычное деление
Decimal(18,2) на UInt64 в этом ClickHouse сохраняет лишь два знака и даёт 159.87.
Деньги в витринах остаются Decimal(18,2), формула отношения не меняется.

Выполненный SQL:

```sql
SELECT currentUser(), name, value FROM system.settings WHERE name = 'readonly';
SELECT count(), sum(revenue_amount) FROM mart.monthly_sales;
SELECT * FROM mart.monthly_sales WHERE month = '2016-11-01';
SELECT * FROM mart.order_summary;
SELECT divideDecimal(toDecimal128(order_value_amount, 2), toDecimal128(orders_count, 0), 10) AS avg_order_value, toFloat64(line_count) / orders_count AS avg_lines, toFloat64(unit_count) / orders_count AS avg_units FROM mart.order_summary;
SELECT *, toFloat64(repeat_customers_count) / customers_count AS repeat_share FROM mart.customer_summary;
SELECT seller_id, revenue_amount FROM mart.seller_sales ORDER BY revenue_amount DESC, seller_id LIMIT 1;
SELECT category_key, category_label, revenue_amount FROM mart.category_sales ORDER BY revenue_amount DESC, category_key LIMIT 1;
SELECT count(), sum(revenue_share), abs(sum(revenue_share) - 1) <= 1e-9 AS share_check FROM mart.category_sales;
SELECT category_key, category_label, is_unknown_category, is_translation_missing FROM mart.category_sales WHERE is_unknown_category OR is_translation_missing ORDER BY category_key;
SELECT * FROM (SELECT 'monthly_sales' AS model, sum(revenue_amount) AS revenue FROM mart.monthly_sales
UNION ALL SELECT 'seller_sales', sum(revenue_amount) FROM mart.seller_sales
UNION ALL SELECT 'category_sales', sum(revenue_amount) FROM mart.category_sales
UNION ALL SELECT 'order_summary', sum(revenue_amount) FROM mart.order_summary) ORDER BY model;
SELECT * FROM mart.refresh_status;
```

```text
$ docker compose exec -T clickhouse bash -c 'clickhouse-client --user bi_reader --password "$CLICKHOUSE_BI_PASSWORD" --multiquery --format TSVWithNames'
currentUser()	name	value
bi_reader	readonly	1
count()	sum(revenue_amount)
24	13221498.11
month	orders_all_count	orders_delivered_count	revenue_amount
2016-11-01	0	0	0
summary_key	orders_count	revenue_amount	freight_amount	order_value_amount	line_count	unit_count
delivered_all	96478	13221498.11	2202965.54	15424463.65	110197	110197
avg_order_value	avg_lines	avg_units
159.8754498434	1.1421982213561641	1.1421982213561641
summary_key	customers_count	repeat_customers_count	one_order_customers_count	orders_count	repeat_share
all_customers	96096	2997	93099	99441	0.031187562437562436
seller_id	revenue_amount
4869f7a5dfa277a7dca6462dcf3b52b2	226987.93
category_key	category_label	revenue_amount
beleza_saude	health_beauty	1233131.72
count()	sum(revenue_share)	share_check
74	1.0000000000000002	1
category_key	category_label	is_unknown_category	is_translation_missing
__unknown__	Категория не указана	1	0
pc_gamer	pc_gamer	0	1
portateis_cozinha_e_preparadores_de_alimentos	portateis_cozinha_e_preparadores_de_alimentos	0	1
model	revenue
category_sales	13221498.11
monthly_sales	13221498.11
order_summary	13221498.11
seller_sales	13221498.11
batch_id	published_at	source_rows	checks_passed
ch_publish_acceptance_2__20261028T1400	2026-10-28 14:01:22.205	95877	1

[exit_code=0]
```

## 5. Типы, порядок, ключи сортировки и Nullable-зерно

SQL команды `25-types-all`:

```sql
SELECT table, position, name, type FROM system.columns
WHERE database='mart' AND NOT endsWith(table,'__stg') ORDER BY table, position;
SELECT name, engine, sorting_key FROM system.tables WHERE database='mart' ORDER BY name;
SELECT count(), uniqExact(value), uniqExact(tuple(value))
FROM values('value Nullable(Int32)', 1, NULL);
SELECT customer_state, toString(delivery_days_sum) FROM mart.delivery_by_state WHERE customer_state='SP';
```

```text
$ docker compose exec -T clickhouse bash -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery --format TSVWithNames'
table	position	name	type
category_sales	1	category_key	String
category_sales	2	category_name_original	Nullable(String)
category_sales	3	category_name_english	Nullable(String)
category_sales	4	category_label	String
category_sales	5	is_unknown_category	UInt8
category_sales	6	is_translation_missing	UInt8
category_sales	7	orders_count	UInt64
category_sales	8	unit_count	UInt64
category_sales	9	revenue_amount	Decimal(18, 2)
category_sales	10	revenue_share	Float64
customer_summary	1	summary_key	String
customer_summary	2	customers_count	UInt64
customer_summary	3	repeat_customers_count	UInt64
customer_summary	4	one_order_customers_count	UInt64
customer_summary	5	orders_count	UInt64
delivery_by_state	1	customer_state	String
delivery_by_state	2	state_name	Nullable(String)
delivery_by_state	3	region	Nullable(String)
delivery_by_state	4	orders_count	UInt64
delivery_by_state	5	delivered_measured_count	UInt64
delivery_by_state	6	delivery_days_sum	Decimal(38, 19)
delivery_by_state	7	avg_delivery_days	Nullable(Float64)
delivery_by_state	8	late_count	UInt64
delivery_by_state	9	late_share	Nullable(Float64)
monthly_sales	1	month	Date
monthly_sales	2	orders_all_count	UInt64
monthly_sales	3	orders_delivered_count	UInt64
monthly_sales	4	revenue_amount	Decimal(18, 2)
order_status_funnel	1	order_status	String
order_status_funnel	2	status_group	String
order_status_funnel	3	orders_count	UInt64
order_status_funnel	4	order_share	Nullable(Float64)
order_status_funnel	5	orders_without_items_count	UInt64
order_summary	1	summary_key	String
order_summary	2	orders_count	UInt64
order_summary	3	revenue_amount	Decimal(18, 2)
order_summary	4	freight_amount	Decimal(18, 2)
order_summary	5	order_value_amount	Decimal(18, 2)
order_summary	6	line_count	UInt64
order_summary	7	unit_count	UInt64
payments_by_state_year	1	customer_state	String
payments_by_state_year	2	purchase_year	Nullable(Int32)
payments_by_state_year	3	payments_count	UInt64
payments_by_state_year	4	payment_amount	Decimal(18, 2)
payments_by_state_year	5	installments_sum	UInt64
payments_by_state_year	6	avg_installments	Float64
payments_by_state_year	7	zero_installments_count	UInt64
payments_by_type	1	payment_type	String
payments_by_type	2	payments_count	UInt64
payments_by_type	3	payment_amount	Decimal(18, 2)
payments_by_type	4	installments_sum	UInt64
payments_by_type	5	avg_installments	Float64
payments_by_type	6	zero_installments_count	UInt64
product_price_history	1	product_id	String
product_price_history	2	sale_date	Nullable(Date)
product_price_history	3	price	Decimal(18, 2)
product_price_history	4	unit_count	UInt64
product_price_history	5	orders_count	UInt64
product_price_history	6	distinct_price_count	UInt64
product_price_history	7	product_unit_count	UInt64
refresh_status	1	batch_id	String
refresh_status	2	published_at	DateTime64(3, \'UTC\')
refresh_status	3	source_rows	UInt64
refresh_status	4	checks_passed	UInt8
review_delivery	1	review_score	Nullable(Int32)
review_delivery	2	orders_count	UInt64
review_delivery	3	delivered_measured_count	UInt64
review_delivery	4	avg_delivery_days	Nullable(Float64)
review_delivery	5	late_count	UInt64
review_delivery	6	late_share	Nullable(Float64)
seller_sales	1	seller_id	String
seller_sales	2	seller_city	Nullable(String)
seller_sales	3	seller_state	Nullable(String)
seller_sales	4	orders_count	UInt64
seller_sales	5	line_count	UInt64
seller_sales	6	unit_count	UInt64
seller_sales	7	revenue_amount	Decimal(18, 2)
name	engine	sorting_key
category_sales	MergeTree	category_key
category_sales__stg	MergeTree	category_key
customer_summary	MergeTree	summary_key
customer_summary__stg	MergeTree	summary_key
delivery_by_state	MergeTree	customer_state
delivery_by_state__stg	MergeTree	customer_state
monthly_sales	MergeTree	month
monthly_sales__stg	MergeTree	month
order_status_funnel	MergeTree	order_status
order_status_funnel__stg	MergeTree	order_status
order_summary	MergeTree	summary_key
order_summary__stg	MergeTree	summary_key
payments_by_state_year	MergeTree	customer_state, purchase_year
payments_by_state_year__stg	MergeTree	customer_state, purchase_year
payments_by_type	MergeTree	payment_type
payments_by_type__stg	MergeTree	payment_type
product_price_history	MergeTree	product_id, sale_date, price
product_price_history__stg	MergeTree	product_id, sale_date, price
refresh_status	MergeTree	batch_id
refresh_status__stg	MergeTree	batch_id
review_delivery	MergeTree	review_score
review_delivery__stg	MergeTree	review_score
seller_sales	MergeTree	seller_id
seller_sales__stg	MergeTree	seller_id
count()	uniqExact(value)	uniqExact((value))
2	1	2
customer_state	toString(delivery_days_sum)
SP	354783.5645254629629664666

[exit_code=0]
```

```text
$ docker compose exec -T airflow python -
pipelines/mart_to_clickhouse.py: syntax OK
airflow/dags/marketplace_elt.py: syntax OK
SCHEMA_PASSED: 11 marts, 73 columns, refresh_status=4 columns
publish_clickhouse: upstream=['dbt_build', 'open_batch'], trigger_rule=TriggerRule.ALL_SUCCESS
max_active_runs=1

[exit_code=0]
```

## 6. Права bi_reader

Проверка INSERT использует WHERE 0: даже при ошибочной выдаче прав данные не
изменились бы. Для проверки другой базы etl создал пустую default.__bi_access_probe,
после проверки удалил её в finally. Неудачные запросы получают серверный код 497
(код возврата CLI 241). Доступ к `system.tables` разрешён, метаданные отфильтрованы
по доступным пользователю таблицам: на финальном стенде видны 24 mart-таблицы
(11 витрин, статус и 12 промежуточных). `information_schema.tables` закрыта.

Запуск: `python3 /tmp/ch-publish-acceptance/security.py`; скрипт вызывает
`docker compose exec -T clickhouse bash -c 'exec clickhouse-client --user ...
--password "$CLICKHOUSE_BI_PASSWORD" --multiquery'` и проверяет код каждого запроса.

```text
$ python3 /tmp/ch-publish-acceptance/security.py
USER=etl; SQL=CREATE TABLE default.__bi_access_probe (value UInt8) ENGINE = MergeTree ORDER BY tuple();
exit_code=0
USER=bi_reader; SQL=INSERT INTO mart.monthly_sales SELECT * FROM mart.monthly_sales WHERE 0;
Received exception from server (version 26.8.31):
Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: bi_reader: Not enough privileges. To execute this query, it's necessary to have the grant INSERT(month, orders_all_count, orders_delivered_count, revenue_amount) ON mart.monthly_sales. (ACCESS_DENIED)
(query: INSERT INTO mart.monthly_sales SELECT * FROM mart.monthly_sales WHERE 0;)
exit_code=241
USER=bi_reader; SQL=SELECT count() FROM postgresql(dwh, schema = 'mart', table = 'monthly_sales');
Received exception from server (version 26.8.31):
Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: bi_reader: Not enough privileges. To execute this query, it's necessary to have the grant NAMED COLLECTION ON dwh. (ACCESS_DENIED)
(query: SELECT count() FROM postgresql(dwh, schema = 'mart', table = 'monthly_sales');)
exit_code=241
USER=bi_reader; SQL=SELECT count() FROM default.__bi_access_probe;
Received exception from server (version 26.8.31):
Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: bi_reader: Not enough privileges. To execute this query, it's necessary to have the grant SELECT ON default.__bi_access_probe. (ACCESS_DENIED)
(query: SELECT count() FROM default.__bi_access_probe;)
exit_code=241
USER=bi_reader; SQL=SELECT count() FROM system.tables;
24
exit_code=0
USER=bi_reader; SQL=SELECT count() FROM information_schema.tables;
Received exception from server (version 26.8.31):
Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: bi_reader: Not enough privileges. To execute this query, it's necessary to have the grant SELECT ON information_schema.tables. (ACCESS_DENIED)
(query: SELECT count() FROM information_schema.tables;)
exit_code=241
USER=etl; SQL=DROP TABLE default.__bi_access_probe;
exit_code=0
BI_PERMISSIONS_PASSED

[exit_code=0]
```

## 7. Отказы не меняют опубликованный комплект

Сценарий выполняется в отдельном `docker compose exec -T airflow python -`;
рабочий код не меняется. Подмена `_source_totals` завышает count последней
витрины review_delivery на 1: к этому моменту все 11 staging-таблиц загружены.
Отдельно завышается выручка категории на 0.01. Реальное добавление колонки
только в category_sales__stg проверяет отказ схемы; колонка удаляется в finally.
INSERT NULL проверяет запрет тихих default-значений. До/после сравниваются SHA-256
полных строк **всех 11 витрин и refresh_status**, включая время публикации.

Исполненный сценарий:

```python
from contextlib import closing
from decimal import Decimal
from hashlib import sha256
from unittest.mock import patch
import pipelines.mart_to_clickhouse as m

def snapshot(ch):
    return {t: sha256(repr(sorted(ch.query(f'SELECT * FROM mart.{t}').result_rows, key=repr)).encode()).hexdigest() for t, key in {**m.MARTS, 'refresh_status': 'batch_id'}.items()}

with closing(m._ch()) as ch:
    before = snapshot(ch)
    print('BEFORE:', before)
    print('status_before:', ch.query('SELECT * FROM mart.refresh_status').result_rows)
    original = m._source_totals
    for mode in ('count', 'revenue'):
        def wrong(cur, table, has_revenue):
            result = original(cur, table, has_revenue)
            if table != ('review_delivery' if mode == 'count' else 'category_sales'):
                return result
            if mode == 'count':
                return (result[0] + 1, *result[1:])
            return (result[0], result[1] + Decimal('0.01'))
        with patch.object(m, '_source_totals', wrong):
            try:
                m.publish(f'must-not-publish-{mode}')
            except RuntimeError as e:
                print(f'EXPECTED_FAILURE ({mode}): {e}')
            else:
                raise AssertionError('publication should fail')
        after = snapshot(ch)
        print(f'AFTER ({mode}):', after)
        assert before == after
    ch.command('ALTER TABLE mart.category_sales__stg ADD COLUMN __schema_probe UInt8')
    try:
        try:
            m.publish('must-not-publish-schema')
        except RuntimeError as e:
            print(f'EXPECTED_FAILURE (schema): {e}')
        else:
            raise AssertionError('schema mismatch must fail')
        assert before == snapshot(ch)
    finally:
        ch.command('ALTER TABLE mart.category_sales__stg DROP COLUMN __schema_probe')
    try:
        ch.command("INSERT INTO mart.monthly_sales__stg SELECT toDate('2016-11-01'), NULL, toUInt64(0), toDecimal64(0, 2) SETTINGS insert_null_as_default = 0")
    except Exception as e:
        print(f'EXPECTED_FAILURE (NULL): {e}')
        assert 'Cannot convert NULL' in str(e)
    else:
        raise AssertionError('NULL must fail')
    assert before == snapshot(ch)
    print('status_after:', ch.query('SELECT * FROM mart.refresh_status').result_rows)
    print('FAILURE_PRESERVES_PUBLICATION_PASSED')
```

```text
$ docker compose exec -T airflow python -
BEFORE: {'monthly_sales': '47c5cf4708b76609e0567d01425f8eb4999564ebec2e9d7a4a1b2ab3f3156fbe', 'seller_sales': 'a22530977c68cfea9aee67b38b7fa5e7c1fa8d0c106598c0b77eec101f4d1a05', 'order_summary': 'e002cfa20177fd3fc766d05f24f4af921f35c769473c620cb4b843d0d123a32b', 'customer_summary': '6dda1cd32843a3caab1113dddd165721d81ae3d9441e38874e89f44bc5706533', 'category_sales': 'd65909da202698d325408c3848be850898da72dfc76da5adfe58914e63088099', 'product_price_history': '5e0b4d5c3d6bade4667690b734e86de8ce82a918a50359cf44f4ca3084577ab1', 'order_status_funnel': '5949fc2bf1f2b9bd1fc62c45936bfe412d42cf4533e368a1e07b57b680c6baa2', 'payments_by_type': 'f44eb4a359480b46ecec1e415232b536170914804a33f1ada8fe6795be9e0335', 'payments_by_state_year': 'd68fecd78dabcf2fcf20a32b0e124a72c37b50887fb347a96059ce83438a0c35', 'delivery_by_state': '865ef04a2cc4bee302e86e3d254509de3745bb6e4c99f28ab8579108fd6ab74d', 'review_delivery': 'dba5fef00f6bede566439ef3ae602d877428b82c7a48fb96790f70a6aea8edcf', 'refresh_status': 'a05bba514f907d3ba5dbed8c488b2b88cca2eeec2cf2c002a18b35fc184f8b5b'}
status_before: [('ch_publish_acceptance_2__20261028T1400', datetime.datetime(2026, 10, 28, 14, 1, 22, 205000), 95877, 1)]
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
mart.product_price_history: строк=92686; проверки пройдены
mart.order_status_funnel: строк=9; проверки пройдены
mart.payments_by_type: строк=5; проверки пройдены
mart.payments_by_state_year: строк=75; проверки пройдены
mart.delivery_by_state: строк=27; проверки пройдены
EXPECTED_FAILURE (count): mart.review_delivery: в ClickHouse 5 строк, в Postgres 6; требуется равенство и > 0
AFTER (count): {'monthly_sales': '47c5cf4708b76609e0567d01425f8eb4999564ebec2e9d7a4a1b2ab3f3156fbe', 'seller_sales': 'a22530977c68cfea9aee67b38b7fa5e7c1fa8d0c106598c0b77eec101f4d1a05', 'order_summary': 'e002cfa20177fd3fc766d05f24f4af921f35c769473c620cb4b843d0d123a32b', 'customer_summary': '6dda1cd32843a3caab1113dddd165721d81ae3d9441e38874e89f44bc5706533', 'category_sales': 'd65909da202698d325408c3848be850898da72dfc76da5adfe58914e63088099', 'product_price_history': '5e0b4d5c3d6bade4667690b734e86de8ce82a918a50359cf44f4ca3084577ab1', 'order_status_funnel': '5949fc2bf1f2b9bd1fc62c45936bfe412d42cf4533e368a1e07b57b680c6baa2', 'payments_by_type': 'f44eb4a359480b46ecec1e415232b536170914804a33f1ada8fe6795be9e0335', 'payments_by_state_year': 'd68fecd78dabcf2fcf20a32b0e124a72c37b50887fb347a96059ce83438a0c35', 'delivery_by_state': '865ef04a2cc4bee302e86e3d254509de3745bb6e4c99f28ab8579108fd6ab74d', 'review_delivery': 'dba5fef00f6bede566439ef3ae602d877428b82c7a48fb96790f70a6aea8edcf', 'refresh_status': 'a05bba514f907d3ba5dbed8c488b2b88cca2eeec2cf2c002a18b35fc184f8b5b'}
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
EXPECTED_FAILURE (revenue): mart.category_sales: выручка ClickHouse 13221498.11 не равна Postgres 13221498.12
AFTER (revenue): {'monthly_sales': '47c5cf4708b76609e0567d01425f8eb4999564ebec2e9d7a4a1b2ab3f3156fbe', 'seller_sales': 'a22530977c68cfea9aee67b38b7fa5e7c1fa8d0c106598c0b77eec101f4d1a05', 'order_summary': 'e002cfa20177fd3fc766d05f24f4af921f35c769473c620cb4b843d0d123a32b', 'customer_summary': '6dda1cd32843a3caab1113dddd165721d81ae3d9441e38874e89f44bc5706533', 'category_sales': 'd65909da202698d325408c3848be850898da72dfc76da5adfe58914e63088099', 'product_price_history': '5e0b4d5c3d6bade4667690b734e86de8ce82a918a50359cf44f4ca3084577ab1', 'order_status_funnel': '5949fc2bf1f2b9bd1fc62c45936bfe412d42cf4533e368a1e07b57b680c6baa2', 'payments_by_type': 'f44eb4a359480b46ecec1e415232b536170914804a33f1ada8fe6795be9e0335', 'payments_by_state_year': 'd68fecd78dabcf2fcf20a32b0e124a72c37b50887fb347a96059ce83438a0c35', 'delivery_by_state': '865ef04a2cc4bee302e86e3d254509de3745bb6e4c99f28ab8579108fd6ab74d', 'review_delivery': 'dba5fef00f6bede566439ef3ae602d877428b82c7a48fb96790f70a6aea8edcf', 'refresh_status': 'a05bba514f907d3ba5dbed8c488b2b88cca2eeec2cf2c002a18b35fc184f8b5b'}
EXPECTED_FAILURE (schema): mart.category_sales__stg: схема не совпадает с mart.sql; ожидалось [('category_key', 'String'), ('category_name_original', 'Nullable(String)'), ('category_name_english', 'Nullable(String)'), ('category_label', 'String'), ('is_unknown_category', 'UInt8'), ('is_translation_missing', 'UInt8'), ('orders_count', 'UInt64'), ('unit_count', 'UInt64'), ('revenue_amount', 'Decimal(18, 2)'), ('revenue_share', 'Float64')], получено [('category_key', 'String'), ('category_name_original', 'Nullable(String)'), ('category_name_english', 'Nullable(String)'), ('category_label', 'String'), ('is_unknown_category', 'UInt8'), ('is_translation_missing', 'UInt8'), ('orders_count', 'UInt64'), ('unit_count', 'UInt64'), ('revenue_amount', 'Decimal(18, 2)'), ('revenue_share', 'Float64'), ('__schema_probe', 'UInt8')]. Публикация отменена; нужна ручная миграция схемы.
EXPECTED_FAILURE (NULL): Received ClickHouse exception, code: 70, server response: Code: 70. DB::Exception: Cannot convert NULL to a non-nullable type: while converting source column `NULL` to destination column orders_all_count. (CANNOT_CONVERT_TYPE) (for url http://clickhouse:8123)
status_after: [('ch_publish_acceptance_2__20261028T1400', datetime.datetime(2026, 10, 28, 14, 1, 22, 205000), 95877, 1)]
FAILURE_PRESERVES_PUBLICATION_PASSED

[exit_code=0]
```

Аналогичный сценарий для первоначальных пяти витрин прошёл ранее:
`16-failure.log`, итог `FAILURE_PRESERVES_PUBLICATION_PASSED`.
Первоначальный файл сохранён отдельно; расширенная проверка выше повторяет его
на окончательном коде и всём комплекте.

## 8. Два полных прогона DAG

DAG уже был unpaused на стенде; команда unpause это фактически подтвердила.
Два ручных run_id заданы явно, каждый следующий запуск начат после успеха
и сверки предыдущего.

```text
$ docker compose exec -T airflow airflow dags unpause marketplace_elt
2026-10-28T13:50:40.941094Z [info     ] setup plugin alembic.autogenerate.schemas [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:50:40.941279Z [info     ] setup plugin alembic.autogenerate.tables [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:50:40.941347Z [info     ] setup plugin alembic.autogenerate.types [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:50:40.941413Z [info     ] setup plugin alembic.autogenerate.constraints [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:50:40.941465Z [info     ] setup plugin alembic.autogenerate.defaults [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:50:40.941515Z [info     ] setup plugin alembic.autogenerate.comments [alembic.runtime.plugins] loc=plugins.py:37
No paused DAGs were found

[exit_code=0]
```

```text
$ docker compose exec -T airflow airflow dags trigger marketplace_elt --run-id ch_publish_acceptance_1__20261028T1400
2026-10-28T13:58:02.132950Z [info     ] setup plugin alembic.autogenerate.schemas [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:58:02.133195Z [info     ] setup plugin alembic.autogenerate.tables [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:58:02.133308Z [info     ] setup plugin alembic.autogenerate.types [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:58:02.133410Z [info     ] setup plugin alembic.autogenerate.constraints [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:58:02.133491Z [info     ] setup plugin alembic.autogenerate.defaults [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:58:02.133571Z [info     ] setup plugin alembic.autogenerate.comments [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T13:58:02.528329Z [info     ] creating dag run               [airflow.serialization.definitions.dag] loc=dag.py:607 logical_date=None partition_key=None run_after=datetime.datetime(2026, 10, 28, 13, 58, 2, 504221, tzinfo=Timezone('UTC')) run_id=ch_publish_acceptance_1__20261028T1400
conf | dag_id          | dag_run_id               | data_interval_start | data_interval_end | end_date | last_scheduling_decision | logical_date | run_type | start_date | state  | triggering_user_name
=====+=================+==========================+=====================+===================+==========+==========================+==============+==========+============+========+=====================
{}   | marketplace_elt | ch_publish_acceptance_1_ | None                | None              | None     | None                     | None         | manual   | None       | queued | default             
     |                 | _20261028T1400           |                     |                   |          |                          |              |          |            |        |                     
                                                                                                                                                                                                        

[exit_code=0]
```

```text
$ docker compose exec -T airflow airflow dags trigger marketplace_elt --run-id ch_publish_acceptance_2__20261028T1400
2026-10-28T14:00:08.392576Z [info     ] setup plugin alembic.autogenerate.schemas [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T14:00:08.392852Z [info     ] setup plugin alembic.autogenerate.tables [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T14:00:08.392975Z [info     ] setup plugin alembic.autogenerate.types [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T14:00:08.393085Z [info     ] setup plugin alembic.autogenerate.constraints [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T14:00:08.393168Z [info     ] setup plugin alembic.autogenerate.defaults [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T14:00:08.393253Z [info     ] setup plugin alembic.autogenerate.comments [alembic.runtime.plugins] loc=plugins.py:37
2026-10-28T14:00:08.821927Z [info     ] creating dag run               [airflow.serialization.definitions.dag] loc=dag.py:607 logical_date=None partition_key=None run_after=datetime.datetime(2026, 10, 28, 14, 0, 8, 795276, tzinfo=Timezone('UTC')) run_id=ch_publish_acceptance_2__20261028T1400
conf | dag_id          | dag_run_id               | data_interval_start | data_interval_end | end_date | last_scheduling_decision | logical_date | run_type | start_date | state  | triggering_user_name
=====+=================+==========================+=====================+===================+==========+==========================+==============+==========+============+========+=====================
{}   | marketplace_elt | ch_publish_acceptance_2_ | None                | None              | None     | None                     | None         | manual   | None       | queued | default             
     |                 | _20261028T1400           |                     |                   |          |                          |              |          |            |        |                     
                                                                                                                                                                                                        

[exit_code=0]
```

Состояния получены из metadata Airflow только SELECT-запросами:

```sql
SELECT run_id, state, start_date, end_date FROM dag_run
WHERE run_id IN ('ch_publish_acceptance_1__20261028T1400','ch_publish_acceptance_2__20261028T1400') ORDER BY id;
SELECT run_id, task_id, map_index, state, try_number FROM task_instance
WHERE run_id IN ('ch_publish_acceptance_1__20261028T1400','ch_publish_acceptance_2__20261028T1400')
ORDER BY run_id, task_id, map_index;
```

```text
$ docker compose exec -T airflow-db psql -X -U airflow -d airflow
                 run_id                 |  state  |          start_date           |           end_date            
----------------------------------------+---------+-------------------------------+-------------------------------
 ch_publish_acceptance_1__20261028T1400 | success | 2026-10-28 13:58:02.903811+00 | 2026-10-28 13:59:17.193572+00
 ch_publish_acceptance_2__20261028T1400 | success | 2026-10-28 14:00:09.700934+00 | 2026-10-28 14:01:22.923299+00
(2 rows)

                 run_id                 |      task_id       | map_index |  state  | try_number 
----------------------------------------+--------------------+-----------+---------+------------
 ch_publish_acceptance_1__20261028T1400 | dbt_build          |        -1 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         0 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         1 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         2 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         3 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         4 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         5 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         6 | success |          1
 ch_publish_acceptance_1__20261028T1400 | extract_load       |         7 | success |          1
 ch_publish_acceptance_1__20261028T1400 | open_batch         |        -1 | success |          1
 ch_publish_acceptance_1__20261028T1400 | publish_clickhouse |        -1 | success |          1
 ch_publish_acceptance_2__20261028T1400 | dbt_build          |        -1 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         0 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         1 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         2 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         3 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         4 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         5 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         6 | success |          1
 ch_publish_acceptance_2__20261028T1400 | extract_load       |         7 | success |          1
 ch_publish_acceptance_2__20261028T1400 | open_batch         |        -1 | success |          1
 ch_publish_acceptance_2__20261028T1400 | publish_clickhouse |        -1 | success |          1
(22 rows)


[exit_code=0]
```

Фактические строки из файлов логов dbt_build и publish_clickhouse обоих DAG;
выборка сообщений по Finished running / Done. PASS / строк= / source_rows:

```text
$ docker compose exec -T airflow python -
RUN=ch_publish_acceptance_1__20261028T1400
LOG=/opt/airflow/logs/dag_id=marketplace_elt/run_id=ch_publish_acceptance_1__20261028T1400/task_id=dbt_build/attempt=1.log
13:59:07  Finished running 34 incremental models, 3 seeds, 32 table models, 402 data tests, 17 view models in 0 hours 0 minutes and 57.95 seconds (57.95s).
13:59:08  Done. PASS=485 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=488
LOG=/opt/airflow/logs/dag_id=marketplace_elt/run_id=ch_publish_acceptance_1__20261028T1400/task_id=publish_clickhouse/attempt=1.log
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
mart.product_price_history: строк=92686; проверки пройдены
mart.order_status_funnel: строк=9; проверки пройдены
mart.payments_by_type: строк=5; проверки пройдены
mart.payments_by_state_year: строк=75; проверки пройдены
mart.delivery_by_state: строк=27; проверки пройдены
mart.review_delivery: строк=5; проверки пройдены
Done. Returned value was: {'batch_id': 'ch_publish_acceptance_1__20261028T1400', 'rows': {'monthly_sales': 24, 'seller_sales': 2970, 'order_summary': 1, 'customer_summary': 1, 'category_sales': 74, 'product_price_history': 92686, 'order_status_funnel': 9, 'payments_by_type': 5, 'payments_by_state_year': 75, 'delivery_by_state': 27, 'review_delivery': 5}, 'source_rows': 95877}
RUN=ch_publish_acceptance_2__20261028T1400
LOG=/opt/airflow/logs/dag_id=marketplace_elt/run_id=ch_publish_acceptance_2__20261028T1400/task_id=dbt_build/attempt=1.log
14:01:18  Finished running 34 incremental models, 3 seeds, 32 table models, 402 data tests, 17 view models in 0 hours 1 minutes and 0.81 seconds (60.81s).
14:01:18  Done. PASS=485 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=488
LOG=/opt/airflow/logs/dag_id=marketplace_elt/run_id=ch_publish_acceptance_2__20261028T1400/task_id=publish_clickhouse/attempt=1.log
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
mart.product_price_history: строк=92686; проверки пройдены
mart.order_status_funnel: строк=9; проверки пройдены
mart.payments_by_type: строк=5; проверки пройдены
mart.payments_by_state_year: строк=75; проверки пройдены
mart.delivery_by_state: строк=27; проверки пройдены
mart.review_delivery: строк=5; проверки пройдены
Done. Returned value was: {'batch_id': 'ch_publish_acceptance_2__20261028T1400', 'rows': {'monthly_sales': 24, 'seller_sales': 2970, 'order_summary': 1, 'customer_summary': 1, 'category_sales': 74, 'product_price_history': 92686, 'order_status_funnel': 9, 'payments_by_type': 5, 'payments_by_state_year': 75, 'delivery_by_state': 27, 'review_delivery': 5}, 'source_rows': 95877}
run_results: {'success': 86, 'pass': 399, 'warn': 3}

[exit_code=0]
```

### Количества под bi_reader и подписи всех 11 витрин

`verify_all.py` получает count(*) Postgres через `docker compose exec -T dwh psql`.
ClickHouse-запросы выполняет **bi_reader** через `docker compose exec -T clickhouse`.
Для каждой витрины вычисляются count(), sum(cityHash64(tuple(*))),
groupBitXor(cityHash64(tuple(*))) — подписи всех колонок, независимые от порядка строк.
Затем проверяются одна строка статуса, run_id, сумма строк и checks_passed=1;
второй набор подписей сравнивается с сохранённым первым.

```text
$ python3 /tmp/ch-publish-acceptance/verify_all.py dag-1 ch_publish_acceptance_1__20261028T1400
monthly_sales: bi_reader=24, Postgres=24, hash_sum=9102683596522594352, hash_xor=13605368715530620870
seller_sales: bi_reader=2970, Postgres=2970, hash_sum=15594402038619235997, hash_xor=10451869419977540311
order_summary: bi_reader=1, Postgres=1, hash_sum=5894152861677788226, hash_xor=5894152861677788226
customer_summary: bi_reader=1, Postgres=1, hash_sum=334762284967081747, hash_xor=334762284967081747
category_sales: bi_reader=74, Postgres=74, hash_sum=16039935481627431215, hash_xor=16990544785089730353
product_price_history: bi_reader=92686, Postgres=92686, hash_sum=5933132563761167566, hash_xor=7903940328352336496
order_status_funnel: bi_reader=9, Postgres=9, hash_sum=14165139162241529740, hash_xor=2738902912854515120
payments_by_type: bi_reader=5, Postgres=5, hash_sum=15642371319951537359, hash_xor=2780172309127910225
payments_by_state_year: bi_reader=75, Postgres=75, hash_sum=14010170545849376059, hash_xor=10364911354860145377
delivery_by_state: bi_reader=27, Postgres=27, hash_sum=6521073497486124496, hash_xor=8156856013212898062
review_delivery: bi_reader=5, Postgres=5, hash_sum=6174851392035840191, hash_xor=12758264580165870891
refresh_status: ch_publish_acceptance_1__20261028T1400	2026-10-28 13:59:16.499	95877	1
dag-1: ALL_11_COUNTS_AND_STATUS_PASSED; source_rows=95877

[exit_code=0]
```

```text
$ python3 /tmp/ch-publish-acceptance/verify_all.py dag-2 ch_publish_acceptance_2__20261028T1400
monthly_sales: bi_reader=24, Postgres=24, hash_sum=9102683596522594352, hash_xor=13605368715530620870
seller_sales: bi_reader=2970, Postgres=2970, hash_sum=15594402038619235997, hash_xor=10451869419977540311
order_summary: bi_reader=1, Postgres=1, hash_sum=5894152861677788226, hash_xor=5894152861677788226
customer_summary: bi_reader=1, Postgres=1, hash_sum=334762284967081747, hash_xor=334762284967081747
category_sales: bi_reader=74, Postgres=74, hash_sum=16039935481627431215, hash_xor=16990544785089730353
product_price_history: bi_reader=92686, Postgres=92686, hash_sum=5933132563761167566, hash_xor=7903940328352336496
order_status_funnel: bi_reader=9, Postgres=9, hash_sum=14165139162241529740, hash_xor=2738902912854515120
payments_by_type: bi_reader=5, Postgres=5, hash_sum=15642371319951537359, hash_xor=2780172309127910225
payments_by_state_year: bi_reader=75, Postgres=75, hash_sum=14010170545849376059, hash_xor=10364911354860145377
delivery_by_state: bi_reader=27, Postgres=27, hash_sum=6521073497486124496, hash_xor=8156856013212898062
review_delivery: bi_reader=5, Postgres=5, hash_sum=6174851392035840191, hash_xor=12758264580165870891
refresh_status: ch_publish_acceptance_2__20261028T1400	2026-10-28 14:01:22.205	95877	1
DAG_REPEAT_SIGNATURES_IDENTICAL
dag-2: ALL_11_COUNTS_AND_STATUS_PASSED; source_rows=95877

[exit_code=0]
```

## 9. Поиск старой архитектуры и состояние сервисов

Буквальный `grep -rnw mp` не пуст: две строки являются именами проекта/образа,
не именем базы. Повтор с исключением **только этих двух строк** пуст (exit 1).
`grep -rni Cloudberry pipelines` пуст (exit 1). Удалены два устаревших сгенерированных
.pyc в pipelines/__pycache__ от прежнего кода, содержавшие старые строки;
актуальные исходники и остальные файлы не затрагивались.

```text
$ bash -c 'grep -rnw mp compose.yaml Makefile infra pipelines airflow; printf "literal_grep_exit=%s\n" "$?"; grep -rnw mp compose.yaml Makefile infra pipelines airflow | grep -vE "compose.yaml:(1:name: mp-dwh|[0-9]+:    image: mp-airflow:)"; printf "database_name_grep_exit=%s\n" "$?"; grep -rni Cloudberry pipelines; printf "Cloudberry_grep_exit=%s\n" "$?"; bash -n infra/clickhouse/init/02_bi_user.sh; printf "bash_syntax_exit=%s\n" "$?"'
compose.yaml:1:name: mp-dwh
compose.yaml:111:    image: mp-airflow:3.3.0
literal_grep_exit=0
database_name_grep_exit=1
Cloudberry_grep_exit=1
bash_syntax_exit=0

[exit_code=0]
```

```text
$ docker compose ps
NAME                  IMAGE                                    COMMAND                  SERVICE      CREATED          STATUS                    PORTS
mp-dwh-airflow-1      mp-airflow:3.3.0                         "/usr/bin/dumb-init …"   airflow      4 hours ago      Up 4 hours (healthy)      127.0.0.1:18090->8080/tcp
mp-dwh-airflow-db-1   postgres:16-alpine                       "docker-entrypoint.s…"   airflow-db   6 hours ago      Up 6 hours (healthy)      5432/tcp
mp-dwh-clickhouse-1   clickhouse/clickhouse-server:26.8.31.2   "/entrypoint.sh"         clickhouse   15 minutes ago   Up 15 minutes (healthy)   9009/tcp, 127.0.0.1:18123->8123/tcp, 127.0.0.1:19000->9000/tcp
mp-dwh-dwh-1          postgres:16-alpine                       "docker-entrypoint.s…"   dwh          6 hours ago      Up 6 hours (healthy)      127.0.0.1:15433->5432/tcp
mp-dwh-source-1       postgres:16-alpine                       "docker-entrypoint.s…"   source       6 hours ago      Up 6 hours (healthy)      127.0.0.1:15432->5432/tcp

[exit_code=0]
```

## Ограничения

- 11 EXCHANGE и переключение статуса не образуют одну транзакцию. Между ними
  возможно окно рассогласования; сбой во время переключения может оставить
  частично новый комплект и прежний refresh_status. Отказы **проверок до EXCHANGE**
  сохраняют весь опубликованный комплект, что подтверждено сценариями.
- `max_active_runs=1` сериализует DAG. Ручные publish нельзя запускать параллельно
  друг с другом или одновременно со сборкой/публикацией DAG: staging общие.
- Контрольные агрегаты Python читаются в Postgres REPEATABLE READ READ ONLY,
  но запросы ClickHouse к Postgres используют отдельные соединения; общей
  межсистемной транзакции снимка нет. В рабочей цепочке dbt завершён до публикации.
- PostgreSQL numeric без typmod отражается table function как Decimal(38,19).
  Это ограничивает точность delivery_days_sum: исходные 20 дробных знаков
  сокращаются до 19. Денежные numeric(18,2) сохраняются точно и сверяются.
  Средние блока Б явно опубликованы как Float64 в разрешённых владельцем пределах.
- Контракт непустоты из ТЗ сохранён для всех 11 витрин; пустой новый набор
  блокирует публикацию. История попыток остаётся в Airflow; статус хранит только успех.
- Superset не разворачивался; прежние dbt-предупреждения данных не исправлялись.

## Вопросы к владельцу

1. **Буквальный grep и имя проекта.** Сохранён `name: mp-dwh`: смена имени Compose
   меняет имена контейнеров/сети/томов и могла бы поднять новый пустой стек вместо
   существующих source/dwh/airflow-db. Это противоречит цели безопасно использовать
   текущий стенд. Минимальный вариант — исключить имя проекта, как уже исключено
   имя образа `mp-airflow`. Все ссылки на прежнюю базу удалены; фактический grep выше.
2. **Точность numeric без typmod.** Нужен ли для следующего этапа отдельный контракт
   экспорта точных 20 знаков delivery_days_sum? Сейчас сохранён вариант A без новых
   PG views и без правки dbt: Decimal(38,19) как у postgresql(). Для SP фактически
   Postgres `354783.56452546296296646668`, ClickHouse
   `354783.5645254629629664666`. Для сохранения всех разрядов потребуется отдельное
   решение по представлению/чтению numeric; молчаливого обещания полной точности нет.
3. **Допустимое NULL-зерно блока Б.** Исходное ТЗ запрещало NULL ключа, но расширение
   ссылается на контракт, разрешающий NULL sale_date, purchase_year и review_score.
   Применено минимальное правило: разрешены только объявленные Nullable-компоненты,
   каждая NULL-группа участвует в точном distinct через tuple; обязательные ключи
   остаются без NULL. На текущих данных все эти компоненты заполнены. Подтвердить
   это правило как постоянное при появлении неизвестных дат/оценок.
4. **Средний чек в будущем Superset.** В формуле гайда Decimal / UInt64 ClickHouse
   возвращает 159.87 вместо нужного 159.8754498… (первые контрольные запросы выявили
   это на стенде). Для приёмки использован точный `divideDecimal(..., 10)` с результатом
   159.8754498434. При реализации Superset согласовать метрику достаточной точности;
   сам гайд и Superset в этой задаче не менялись.

## Факты ограничения numeric

```text
$ docker compose exec -T clickhouse bash -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery --format TSVWithNames'
name	type	default_type	default_expression	comment	codec_expression	ttl_expression
customer_state	Nullable(String)					
state_name	Nullable(String)					
region	Nullable(String)					
orders_count	Nullable(Int64)					
delivered_measured_count	Nullable(Int64)					
delivery_days_sum	Nullable(Decimal(38, 19))					
avg_delivery_days	Nullable(Decimal(38, 19))					
late_count	Nullable(Int64)					
late_share	Nullable(Float64)					
name	type	default_type	default_expression	comment	codec_expression	ttl_expression
payment_type	Nullable(String)					
payments_count	Nullable(Int64)					
payment_amount	Nullable(Decimal(18, 2))					
installments_sum	Nullable(Int64)					
avg_installments	Nullable(Decimal(38, 19))					
zero_installments_count	Nullable(Int64)					

[exit_code=0]
```

```text
$ docker compose exec -T dwh psql -X -U dwh -d dwh -c 'SELECT customer_state, delivery_days_sum, scale(delivery_days_sum) FROM mart.delivery_by_state WHERE customer_state='"'"'SP'"'"''
 customer_state |      delivery_days_sum      | scale 
----------------+-----------------------------+-------
 SP             | 354783.56452546296296646668 |    20
(1 row)


[exit_code=0]
```

## Артефакты

Полные логи и сценарии находятся на хосте в `/tmp/ch-publish-acceptance/`.
Существенные результаты скопированы выше, отчёт не требует сохранности /tmp.
`10-dbt-build.log` содержит весь ручной build; `32-dag-build-logs.log` — итоговые
сообщения двух полных DAG build. `dag-1-signatures.json` / `dag-2-signatures.json`
содержат сравнивавшиеся подписи. `33-failure-all.log` — финальный негативный сценарий.
`controls.sql`, `verify_all.py`, `failure.py`, `security.py` — исполнявшиеся проверки.

При отладке самих проверочных скриптов были исправлены несовместимый с установленным
Airflow аргумент DagBag, область ORDER BY в проверочном UNION ClickHouse, ожидание
доступа к information_schema и ожидаемое имя ошибки NULL (фактический код 70).
Эти ошибки проверочных команд не требовали изменения рабочего пайплайна.
Первые варианты сохранены в `13-controls-initial.log`, `15-security-initial.log`,
`16-failure-initial.log`; итоговые успешные выводы включены выше.


## Финальная сверка контракта после негативных сценариев

Все 73 колонки сверены программно по именам и порядку одновременно с Postgres
`information_schema.columns` и `dbt/models/mart/_mart.yml`; повторно проверены
схемы целевых и промежуточных таблиц и сохранность последнего успешного run_id.

```text
$ docker compose exec -T airflow python -
monthly_sales: column order matches Postgres and _mart.yml (4)
seller_sales: column order matches Postgres and _mart.yml (7)
order_summary: column order matches Postgres and _mart.yml (7)
customer_summary: column order matches Postgres and _mart.yml (5)
category_sales: column order matches Postgres and _mart.yml (10)
product_price_history: column order matches Postgres and _mart.yml (7)
order_status_funnel: column order matches Postgres and _mart.yml (5)
payments_by_type: column order matches Postgres and _mart.yml (6)
payments_by_state_year: column order matches Postgres and _mart.yml (7)
delivery_by_state: column order matches Postgres and _mart.yml (9)
review_delivery: column order matches Postgres and _mart.yml (6)
FINAL_STATUS: [('ch_publish_acceptance_2__20261028T1400', datetime.datetime(2026, 10, 28, 14, 1, 22, 205000), 95877, 1)]
ALL_11_SCHEMA_CONTRACT_PASSED

[exit_code=0]
```


## Шлифовка блока Б

Приёмка ТЗ `.plans/mart-polish-tz.md` выполнена 28.10.2026 на уже поднятом
стенде. Пункты **1–6 пройдены**. Все команды dbt выполнялись через
`docker compose exec -T airflow`. Ни ODS/DDS, ни JOIN, фильтры и зерно витрин
не изменялись. Superset не добавлялся. Коммит не создавался.

### Изменения и миграция

- `dbt/models/mart/delivery_by_state.sql`: сначала round до 2 знаков, затем
  numeric(18,2) для суммы дней и numeric(10,2) для среднего. state_name и region
  получают `coalesce(..., 'Unknown')`.
- `dbt/models/mart/review_delivery.sql`: среднее дней — round до 2 знаков,
  затем numeric(10,2).
- `dbt/models/mart/payments_by_type.sql` и `payments_by_state_year.sql`:
  avg_installments — round до 2 знаков, затем numeric(10,2).
- `dbt/models/mart/_mart.yml`: три новых not_null для sale_date, purchase_year,
  review_score; описания приведены к новому контракту. У новых тестов
  фактическая severity=ERROR (проверено в manifest.json).
- `infra/clickhouse/mart.sql`: соответствующие Decimal(18,2) / Decimal(10,2),
  sale_date Date, purchase_year и review_score Int32 без Nullable;
  state_name/region String. Удалены все три allow_nullable_key.
  avg_delivery_days, late_share и order_share остаются Nullable;
  типы долей Float64 и формулы без округления сохранены.
- `Makefile`: добавлена `ch-reset-mart`, а `publish` использует exec -T.
  `pipelines/mart_to_clickhouse.py` менять не потребовалось: проверка схемы,
  публикация и точная Decimal-сверка выручки прошли с новыми типами.

Разовая процедура для существующего тома:

```sh
docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build --select mart'
make ch-reset-mart
```

Запускать после успешной сборки, без активных DAG и ручных publish. Перед
миграцией по списку запусков все DAG были завершены. Цель удаляет **только**
product_price_history, payments_by_type, payments_by_state_year, delivery_by_state,
review_delivery и соответствующие `__stg` (DROP TABLE IF EXISTS ... SYNC),
затем вызывает `make publish`: DDL пересоздаёт таблицы, пайплайн проверяет и
заполняет их. Остальные таблицы и том ClickHouse не удаляются.
Во время этой разовой миграции пять таблиц недоступны до завершения publish;
при ошибке после DROP нужно устранить причину и повторить publish. Обычная
публикация по-прежнему использует staging и EXCHANGE.

Фактический вывод миграции:

```text
$ make ch-reset-mart
docker compose exec -T clickhouse bash -euc 'for table in product_price_history payments_by_type payments_by_state_year delivery_by_state review_delivery; do for suffix in "" __stg; do clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "DROP TABLE IF EXISTS mart.${table}${suffix} SYNC"; done; done'
make publish
make[1]: Entering directory '/home/leanleanlean96/warehouse_rwb/marketplace-dwh'
docker compose exec -T airflow python -c "from pipelines.mart_to_clickhouse import publish; print(publish('manual'))"
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
mart.product_price_history: строк=92686; проверки пройдены
mart.order_status_funnel: строк=9; проверки пройдены
mart.payments_by_type: строк=5; проверки пройдены
mart.payments_by_state_year: строк=75; проверки пройдены
mart.delivery_by_state: строк=27; проверки пройдены
mart.review_delivery: строк=5; проверки пройдены
{'batch_id': 'manual', 'rows': {'monthly_sales': 24, 'seller_sales': 2970, 'order_summary': 1, 'customer_summary': 1, 'category_sales': 74, 'product_price_history': 92686, 'order_status_funnel': 9, 'payments_by_type': 5, 'payments_by_state_year': 75, 'delivery_by_state': 27, 'review_delivery': 5}, 'source_rows': 95877}
make[1]: Leaving directory '/home/leanleanlean96/warehouse_rwb/marketplace-dwh'

[exit_code=0]
```

### 1. dbt build --select mart

Команда завершилась с кодом 0: 11 table models + 44 data tests. Ниже строки
новых тестов и итог из полного лога (ANSI удалены):

```text
$ docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build --select mart'
14:11:24  40 of 55 PASS not_null_payments_by_state_year_purchase_year .................... [PASS in 0.17s]
14:11:24  44 of 55 PASS not_null_review_delivery_review_score ............................ [PASS in 0.11s]
14:11:24  54 of 55 PASS not_null_product_price_history_sale_date ......................... [PASS in 0.08s]
14:11:25  Finished running 11 table models, 44 data tests in 0 hours 0 minutes and 3.91 seconds (3.91s).
14:11:25  Done. PASS=55 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=55
[exit_code=0]
```

### 2–3. Фактические типы и ключи

Верификатор прочитал information_schema.columns Postgres
(table_name, column_name, numeric_precision, numeric_scale) и system.columns
ClickHouse (name, type ORDER BY position). Схемы **всех** целевых и staging-таблиц
совпали с DDL. Все 73 колонки по именам и порядку совпали с Postgres и _mart.yml.
Во всех опубликованных витринах нет Decimal(38,...). Для всех шести таблиц
блока Б проверены типы компонентов зерна без Nullable и SHOW CREATE без
allow_nullable_key. Фактический вывод, включая SHOW CREATE:

```text
$ docker compose exec -T airflow python -
ALL_TARGET_AND_STAGING_SCHEMAS_MATCH_DDL
ALL_73_COLUMNS_MATCH_PG_YAML_AND_CH_ORDER
PG delivery_by_state.delivery_days_sum: numeric(18,2)
PG delivery_by_state.avg_delivery_days: numeric(10,2)
PG payments_by_state_year.avg_installments: numeric(10,2)
PG payments_by_type.avg_installments: numeric(10,2)
PG review_delivery.avg_delivery_days: numeric(10,2)
NO_DECIMAL_38_IN_PUBLISHED_MARTS
SHOW CREATE: CREATE TABLE mart.product_price_history
(
    `product_id` String,
    `sale_date` Date,
    `price` Decimal(18, 2),
    `unit_count` UInt64,
    `orders_count` UInt64,
    `distinct_price_count` UInt64,
    `product_unit_count` UInt64
)
ENGINE = MergeTree
ORDER BY (product_id, sale_date, price)
SETTINGS index_granularity = 8192
SHOW CREATE: CREATE TABLE mart.order_status_funnel
(
    `order_status` String,
    `status_group` String,
    `orders_count` UInt64,
    `order_share` Nullable(Float64),
    `orders_without_items_count` UInt64
)
ENGINE = MergeTree
ORDER BY order_status
SETTINGS index_granularity = 8192
SHOW CREATE: CREATE TABLE mart.payments_by_type
(
    `payment_type` String,
    `payments_count` UInt64,
    `payment_amount` Decimal(18, 2),
    `installments_sum` UInt64,
    `avg_installments` Decimal(10, 2),
    `zero_installments_count` UInt64
)
ENGINE = MergeTree
ORDER BY payment_type
SETTINGS index_granularity = 8192
SHOW CREATE: CREATE TABLE mart.payments_by_state_year
(
    `customer_state` String,
    `purchase_year` Int32,
    `payments_count` UInt64,
    `payment_amount` Decimal(18, 2),
    `installments_sum` UInt64,
    `avg_installments` Decimal(10, 2),
    `zero_installments_count` UInt64
)
ENGINE = MergeTree
ORDER BY (customer_state, purchase_year)
SETTINGS index_granularity = 8192
SHOW CREATE: CREATE TABLE mart.delivery_by_state
(
    `customer_state` String,
    `state_name` String,
    `region` String,
    `orders_count` UInt64,
    `delivered_measured_count` UInt64,
    `delivery_days_sum` Decimal(18, 2),
    `avg_delivery_days` Nullable(Decimal(10, 2)),
    `late_count` UInt64,
    `late_share` Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY customer_state
SETTINGS index_granularity = 8192
SHOW CREATE: CREATE TABLE mart.review_delivery
(
    `review_score` Int32,
    `orders_count` UInt64,
    `delivered_measured_count` UInt64,
    `avg_delivery_days` Nullable(Decimal(10, 2)),
    `late_count` UInt64,
    `late_share` Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY review_score
SETTINGS index_granularity = 8192
BLOCK_B_KEYS_NOT_NULLABLE_AND_NO_ALLOW_NULLABLE_KEY
```

### 4. Полный DAG, строки и выручка

После ручной публикации запущен реальный DAG через
`docker compose exec -T airflow airflow dags trigger marketplace_elt --run-id mart_polish_acceptance__20261028T1413`.
Запуск завершился success в 14:14:02 UTC; все 11 экземпляров задач — success
с первой попытки. Полный dbt build содержит три прежних предупреждения DDS
(14 / 166 / 8 строк), ERROR=0; они не относятся к новым not_null.

Фактическое состояние из БД Airflow:

```text
$ docker compose exec -T airflow-db psql -X -U airflow -d airflow -c 'SELECT run_id,state,start_date,end_date FROM dag_run WHERE run_id='"'"'mart_polish_acceptance__20261028T1413'"'"'; SELECT task_id,map_index,state,try_number FROM task_instance WHERE run_id='"'"'mart_polish_acceptance__20261028T1413'"'"' ORDER BY task_id,map_index;'
                run_id                 |  state  |          start_date          |           end_date
---------------------------------------+---------+------------------------------+-------------------------------
 mart_polish_acceptance__20261028T1413 | success | 2026-10-28 14:12:17.87086+00 | 2026-10-28 14:14:02.251615+00
(1 row)

      task_id       | map_index |  state  | try_number
--------------------+-----------+---------+------------
 dbt_build          |        -1 | success |          1
 extract_load       |         0 | success |          1
 extract_load       |         1 | success |          1
 extract_load       |         2 | success |          1
 extract_load       |         3 | success |          1
 extract_load       |         4 | success |          1
 extract_load       |         5 | success |          1
 extract_load       |         6 | success |          1
 extract_load       |         7 | success |          1
 open_batch         |        -1 | success |          1
 publish_clickhouse |        -1 | success |          1
(11 rows)


[exit_code=0]
```

Итоги из логов задач DAG:

```text
$ docker compose exec -T airflow python -
TASK: dbt_build
LOG: /opt/airflow/logs/dag_id=marketplace_elt/run_id=mart_polish_acceptance__20261028T1413/task_id=dbt_build/attempt=1.log
14:13:51  425 of 491 WARN 14 dds_warn_is_approved_missing ................................ [WARN 14 in 0.17s]
14:13:51  426 of 491 WARN 166 dds_warn_is_carrier_before_purchase ........................ [WARN 166 in 0.19s]
14:13:51  427 of 491 WARN 8 dds_warn_is_delivered_without_date ........................... [WARN 8 in 0.12s]
14:13:57  Finished running 34 incremental models, 3 seeds, 32 table models, 405 data tests, 17 view models in 0 hours 1 minutes and 30.45 seconds (90.45s).
14:13:57  Done. PASS=488 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=491
Command exited with return code 0
TASK: publish_clickhouse
LOG: /opt/airflow/logs/dag_id=marketplace_elt/run_id=mart_polish_acceptance__20261028T1413/task_id=publish_clickhouse/attempt=1.log
mart.monthly_sales: строк=24; проверки пройдены
mart.seller_sales: строк=2970; проверки пройдены
mart.order_summary: строк=1; проверки пройдены
mart.customer_summary: строк=1; проверки пройдены
mart.category_sales: строк=74; проверки пройдены
mart.product_price_history: строк=92686; проверки пройдены
mart.order_status_funnel: строк=9; проверки пройдены
mart.payments_by_type: строк=5; проверки пройдены
mart.payments_by_state_year: строк=75; проверки пройдены
mart.delivery_by_state: строк=27; проверки пройдены
mart.review_delivery: строк=5; проверки пройдены
Done. Returned value was: {'batch_id': 'mart_polish_acceptance__20261028T1413', 'rows': {'monthly_sales': 24, 'seller_sales': 2970, 'order_summary': 1, 'customer_summary': 1, 'category_sales': 74, 'product_price_history': 92686, 'order_status_funnel': 9, 'payments_by_type': 5, 'payments_by_state_year': 75, 'delivery_by_state': 27, 'review_delivery': 5}, 'source_rows': 95877}

[exit_code=0]
```

После окончания DAG повторно сверены количества **всех 11 витрин** между
Postgres и ClickHouse и точная выручка четырёх витрин (без Float и допусков).
Построчно, с сортировкой по зерну, сравнены новые Decimal-метрики всех четырёх
изменённых SQL-моделей; для delivery_by_state также обе подписи, сумма дней и
late_share, для review_delivery — late_share. Совпадение точное, включая доли.
Сумма строк 95 877; refresh_status содержит ровно одну запись этого DAG,
checks_passed=1. Фактический вывод:

```text
ROWS monthly_sales: PG=24 CH=24
ROWS seller_sales: PG=2970 CH=2970
ROWS order_summary: PG=1 CH=1
ROWS customer_summary: PG=1 CH=1
ROWS category_sales: PG=74 CH=74
ROWS product_price_history: PG=92686 CH=92686
ROWS order_status_funnel: PG=9 CH=9
ROWS payments_by_type: PG=5 CH=5
ROWS payments_by_state_year: PG=75 CH=75
ROWS delivery_by_state: PG=27 CH=27
ROWS review_delivery: PG=5 CH=5
TOTAL_ROWS: 95877
REVENUE monthly_sales: PG=13221498.11 CH=13221498.11
REVENUE seller_sales: PG=13221498.11 CH=13221498.11
REVENUE category_sales: PG=13221498.11 CH=13221498.11
REVENUE order_summary: PG=13221498.11 CH=13221498.11
EXACT_PG_CH_VALUES delivery_by_state: PASS (including unrounded shares)
EXACT_PG_CH_VALUES review_delivery: PASS (including unrounded shares)
EXACT_PG_CH_VALUES payments_by_type: PASS (including unrounded shares)
EXACT_PG_CH_VALUES payments_by_state_year: PASS (including unrounded shares)
STATE_LABEL_NULLS: 0
REFRESH_STATUS: [('mart_polish_acceptance__20261028T1413', datetime.datetime(2026, 10, 28, 14, 14, 1, 452000), 95877, 1)]
ACCEPTANCE_CHECKS_PASSED

[exit_code=0]
```

### 5. Запросы под bi_reader

Оба запроса из ТЗ выполнены после успешного DAG через clickhouse-client
с `--user bi_reader --password "$CLICKHOUSE_BI_PASSWORD"` (пароль не выводился):

```sql
select * from mart.review_delivery order by review_score;
select customer_state, state_name, avg_delivery_days
from mart.delivery_by_state order by avg_delivery_days desc limit 5;
select countIf(isNull(state_name) OR isNull(region)) as null_labels
from mart.delivery_by_state;
select payment_type, avg_installments from mart.payments_by_type order by payment_type;
```

Фактический TSVWithNames (средние доставки имеют 2 знака, null_labels=0;
обычный TSV целые Decimal avg_installments отображает как 1 при типе Decimal(10,2)):

```text
$ docker compose exec -T clickhouse bash -c 'exec clickhouse-client --user bi_reader --password "$CLICKHOUSE_BI_PASSWORD" --multiquery --format TSVWithNames'
review_score	orders_count	delivered_measured_count	avg_delivery_days	late_count	late_share
1	11366	9353	21.32	3432	0.36694108842082757
2	3129	2919	16.63	550	0.1884206920178143
3	8133	7916	14.26	694	0.08767054067710965
4	19044	18894	12.31	649	0.034349528950989736
5	57001	56742	10.68	1056	0.018610553029501955
customer_state	state_name	avg_delivery_days
RR	Roraima	29.39
AP	Amapá	27.19
AM	Amazonas	26.43
AL	Alagoas	24.54
PA	Pará	23.77
null_labels
0
payment_type	avg_installments
boleto	1
credit_card	3.51
debit_card	1
not_defined	1
voucher	1

[exit_code=0]
```

### 6. Отчёт и артефакты

Этот раздел добавлен **в конец** исходного отчёта; все прежние байты сохранены
и проверены программно. Предыдущие выводы о Decimal(38,19), Float64 средних и
Nullable-компонентах зерна описывают прошлую приёмку; текущий контракт приведён
в разделе «Шлифовка блока Б» выше. Все пять сервисов после приёмки healthy.
Полные логи и проверочные скрипты: `/tmp/mart-polish-acceptance/`:
01-dbt-build.log, 02-reset-publish.log, 03-trigger.log, 05-dag-state.log,
06-verify.log, 07-bi-reader.log, 08-dag-logs.log, 09-health.log;
verify.py, bi.sql и dag_logs.py. Существенный вывод сохранён здесь и не требует
сохранности /tmp.

### Вопросы к владельцу

Новых развилок по этому ТЗ нет. Решения владельца из mart-polish-tz.md реализованы
полностью и закрывают прежние вопросы №2 (точность numeric без typmod) и №3
(Nullable-зерно блока Б). При неизвестной дате/годе/оценке строки не отбрасываются
и JOIN не меняется: новые not_null с severity=ERROR блокируют успешный build,
а зависимая публикация DAG не запускается. Прежние вопросы об имени Compose-проекта
и будущей метрике Superset этим ТЗ не затрагиваются.
