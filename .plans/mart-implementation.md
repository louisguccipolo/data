# Приёмка mart — 28.10.2026

## Что реализовано

Пять моделей в `dbt/models/mart/`: `monthly_sales`, `seller_sales`,
`order_summary`, `customer_summary`, `category_sales`. Блок `models.marketplace.mart`
в `dbt_project.yml` задаёт `+schema: mart`, `+materialized: table`.
Состав и порядок всех **33 колонок** соответствуют §3.1–3.5 `superset-guide.md`.
`refresh_status` не создаётся: это отдельный этап публикации в ClickHouse.

В `_mart.yml` описаны все пять моделей и каждая колонка; на ключах зерна —
10 тестов `unique` / `not_null` (по два на модель). В `dbt/tests/mart_*` добавлены
12 singular-тестов: 7 инвариантов `error` и 5 регрессий Olist `warn`.
Все пять моделей имеют `-- depends_on: {{ ref('err_cast') }}`.
README и SQL 01–05 бизнес-вопросов обновлены под контракт витрин;
чужое описание DDS заменено описанием активных views, SK и общего среза.

Исходный код staging / ODS / DDS, seeds, макросов, EL, compose, init.sql,
ClickHouse и Superset не менялся. Разрешённый полный build пересобрал
upstream-объекты из существующего кода. Git не использовался, коммитов нет.

## Принятые решения

- Все метрики читают активные факты. Позиции продавцов и категорий дополнительно
  соединяются с активным заказом: удалённый заказ не участвует в выручке, даже
  если его позиции сами не помечены удалёнными.
- Доходные заказы определяются через текущую строку `dim_order_status.is_revenue`;
  статус `delivered` в фильтрах mart не захардкожен. На текущем seed это delivered.
- Месячный итог и итог по заказам используют меры `fct_order`: `items_price`,
  `items_freight`, `items_value`, `items_count`. Прямого соединения заказов
  с позициями в этих двух моделях нет; заказы без позиций не теряются.
- Месяцы берутся из `dim_date.month_start` в окне `[2016-09-01, 2018-09-01)`;
  пропуски заполняются нулями. Остальные витрины охватывают всю выгрузку.
- По §20 DDS SK факта указывает на версию на момент продажи (первое
  наблюдение связи), с правилом первой полной версии для позднего измерения.
  Категория продаж определяется по product_sk; названия категории — из
  текущих товаров с этим кодом, иначе из последней версии кода. Продавец
  агрегируется по seller_hk, адрес — из его текущей версии. Дополнительный
  фильтр удаления продавца или товара не применяется: их удаление не удаляет продажи.
- Категория: код DDS, PT/EN без ручных переводов; EN → PT, для `__unknown__` —
  «Категория не указана». Обе известные категории без перевода сохранены отдельно.
  Проверка текущих измерений не обнаружила коллизий зарезервированных ключей.
- Покупатели считаются по `customer_unique_id`, без позиций и без фильтра
  статуса. Unknown с NULL BK не считается одной личностью; все его заказы
  сохраняются в `customer_summary.orders_count`. Текущих таких заказов **0**.
- Unknown-продавец сохраняет продажи в отдельной группе `__unknown__`,
  география может быть NULL. Сейчас позиций с Unknown-продавцом **0**.
- Деньги явно приведены к `numeric(18,2)`, количества — `bigint`, месяц — `date`,
  **флаги — `boolean`**, идентификаторы — `text`, `revenue_share` —
  **`double precision`**, доля 0–1. Промежуточные денежные суммы точные;
  доля вычисляется в numeric и лишь затем преобразуется в double.
  Физические типы всех 33 колонок проверены через `information_schema.columns`.
- `line_count = unit_count`: одна исходная позиция равна единице. Средние
  и доли вычисляются из числителей и знаменателей без округления в витринах.
  При нулевом обороте доля категории по умолчанию 0: деления на ноль нет,
  обязательный инвариант суммы долей сообщает об отсутствии распределения.
- Платежи и отзывы к позициям не присоединяются. `orders_count` продавцов
  и категорий неаддитивен; доли и средние также нельзя складывать.

## Сборка и тесты

Запуски выполнялись только внутри контейнера Airflow:

```bash
docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build --select +path:models/mart'
docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build'
docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build --select +path:models/mart'
docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build'
docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build'
```

Первый частичный запуск: все 27 mart-узлов успешны, но общий результат
`PASS=423 WARN=3 ERROR=1 SKIP=0 NO-OP=0 TOTAL=427`.
Причина вне mart: существующий `dds_reconcile` был выбран косвенно и прочитал
отсутствующий `dds.fct_payment__active`. Эта view не входит в предков mart,
поэтому частичный build её не создаёт. Полный build создал DDS views из
неизменённого кода и прошёл без ошибок. Частичный повтор снова завершился
с теми же `PASS=423 WARN=3 ERROR=1 SKIP=0 TOTAL=427`: пересборка базового
`fct_payment` удаляет зависимую view при замене таблицы, а выбор mart её
не включает. Поэтому просто однократно создать view недостаточно.
Финальная приёмка и детерминизм выполнены **двумя полными build подряд**;
изменять DDS или dbt-макросы для исправления частичного выбора запрещено задачей.
Все mart-модели и тесты успешны и в обоих частичных запусках.

Вывод полного build:

```text
Finished running 34 incremental models, 3 seeds, 26 table models, 376 data tests, 17 view models
in 0 hours 1 minutes and 8.39 seconds (68.39s).
Done. PASS=453 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=456
```


Два финальных полных build завершились с кодом выхода 0:

```text
Первый финальный полный build:
13:17:33  Finished running 34 incremental models, 3 seeds, 26 table models, 376 data tests, 17 view models in 0 hours 1 minutes and 6.81 seconds (66.81s).
13:17:33  Done. PASS=453 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=456

Второй финальный полный build:
13:18:58  Finished running 34 incremental models, 3 seeds, 26 table models, 376 data tests, 17 view models in 0 hours 1 minutes and 8.14 seconds (68.14s).
13:18:59  Done. PASS=453 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=456
```

Проверка итогового `run_results.json`: **456 узлов = 80 success + 373 pass +
3 warn**. В mart **27 узлов = 5 success моделей + 22 pass тестов**,
0 error / fail / skipped. Все SQL 01–05 также выполнены через psql без ошибок.
Финальные контрольные запросы дали байт-в-байт тот же вывод, что приведён ниже.
`orders_all_count = 99 421` в месячном окне и `orders_count = 99 441`
во всей выгрузке относятся к разным выборкам; это ожидаемо.

Инварианты `severity: error`:

- `mart_monthly_calendar`: ровно 24 строки, начало/конец окна и начало месяца.
- `mart_summary_grain`: ровно одна строка в каждой summary и правильные константы ключей.
- `mart_revenue_reconciliation`: выручка месяцев = продавцов = категорий = итогу заказов.
- `mart_order_value`: стоимость заказа = товары + доставка.
- `mart_revenue_from_dds`: итог выручки = сумма `items_price` активных доходных заказов DDS.
- `mart_category_share`: сумма долей 1 с допуском 1e-9; каждая доля в диапазоне 0–1.
- `mart_customer_partition`: покупатели = повторные + с одним заказом.

`mart_control_monthly`, `mart_control_order_summary`, `mart_control_customer_summary`,
`mart_control_sellers`, `mart_control_categories` проверяют все запрошенные
контрольные цифры, отображаемые средние, лидеров, unknown и две категории без
перевода. Их `severity: warn` отделяет регрессию исходного Olist от инвариантов:
новая корректная загрузка может изменить фиксированные числа и не должна
блокировать публикацию только из-за этого.

Предупреждения данных во всех запусках — только прежние DDS:

| Тест | Строк |
|---|---:|
| `dds_warn_is_delivered_without_date` | 8 |
| `dds_warn_is_approved_missing` | 14 |
| `dds_warn_is_carrier_before_purchase` | 166 |

Mart-предупреждений нет. Отдельные прежние сообщения CLI: версия dbt 1.10.23
устарела; `snapshots.marketplace` не применяется ни к одному ресурсу.
Они не входят в счётчик WARN узлов данных. Адаптер postgres 1.10.2, target dev.

## Контрольные SQL и фактический вывод

Все запросы выполнялись через:

```bash
docker compose exec -T dwh psql -X -v ON_ERROR_STOP=1 -U dwh -d dwh
```

Контрольный SQL:

```sql
select as_of_dts from dds.int_as_of;
select count(*) as months_count, min(month) as first_month, max(month) as last_month,
    sum(orders_all_count) as orders_all_count, sum(orders_delivered_count) as orders_delivered_count,
    sum(revenue_amount) as revenue_amount from mart.monthly_sales;
select * from mart.monthly_sales where month = date '2016-11-01';
select * from mart.order_summary;
select order_value_amount / nullif(orders_count, 0) as avg_order_value,
    line_count::numeric / nullif(orders_count, 0) as avg_lines,
    unit_count::numeric / nullif(orders_count, 0) as avg_units from mart.order_summary;
select *, repeat_customers_count::numeric / nullif(customers_count, 0) as repeat_share from mart.customer_summary;
select seller_id, revenue_amount from mart.seller_sales order by revenue_amount desc, seller_id limit 1;
select category_key, category_label, revenue_amount from mart.category_sales order by revenue_amount desc, category_key limit 1;
select count(*) as category_count, sum(revenue_share) as revenue_share_total from mart.category_sales;
select category_key, category_label, is_unknown_category, is_translation_missing from mart.category_sales
    where is_unknown_category or is_translation_missing order by category_key;
select 'monthly_sales' as model, sum(revenue_amount) as revenue_amount from mart.monthly_sales
union all select 'seller_sales', sum(revenue_amount) from mart.seller_sales
union all select 'category_sales', sum(revenue_amount) from mart.category_sales
union all select 'order_summary', revenue_amount from mart.order_summary
union all select 'dds_active_revenue', sum(o.items_price) from dds.fct_order__active o
    join dds.dim_order_status s on s.order_status = o.order_status and s.is_current and s.is_revenue;
select count(*) as active_orders_without_items from dds.fct_order__active where items_count = 0;
select count(*) as unknown_customer_orders from dds.fct_order__active o join dds.dim_customer c using (customer_sk)
    where c.customer_unique_id is null;
select count(*) as unknown_seller_items from dds.fct_order_item__active i join dds.dim_seller s using (seller_sk)
    where s.seller_id is null;
select count(*) as category_reserved_key_collisions from dds.dim_product
    where is_current and category_code = '__unknown__' and category_name_pt is not null;
select count(*) as seller_reserved_key_collisions from dds.dim_seller
    where is_current and seller_id = '__unknown__';
select count(*) as revenue_orders_outside_month_window from dds.fct_order__active o
    join dds.dim_order_status s on s.order_status = o.order_status and s.is_current and s.is_revenue
    where o.order_purchase_ts < timestamp '2016-09-01' or o.order_purchase_ts >= timestamp '2018-09-01' or o.order_purchase_ts is null;
```

Фактический вывод:

```text
Pager usage is off.
           as_of_dts
-------------------------------
 2026-10-28 12:37:17.709264+00
(1 row)

 months_count | first_month | last_month | orders_all_count | orders_delivered_count | revenue_amount
--------------+-------------+------------+------------------+------------------------+----------------
           24 | 2016-09-01  | 2018-08-01 |            99421 |                  96478 |    13221498.11
(1 row)

   month    | orders_all_count | orders_delivered_count | revenue_amount
------------+------------------+------------------------+----------------
 2016-11-01 |                0 |                      0 |           0.00
(1 row)

  summary_key  | orders_count | revenue_amount | freight_amount | order_value_amount | line_count | unit_count
---------------+--------------+----------------+----------------+--------------------+------------+------------
 delivered_all |        96478 |    13221498.11 |     2202965.54 |        15424463.65 |     110197 |     110197
(1 row)

   avg_order_value    |     avg_lines      |     avg_units
----------------------+--------------------+--------------------
 159.8754498434876345 | 1.1421982213561641 | 1.1421982213561641
(1 row)

  summary_key  | customers_count | repeat_customers_count | one_order_customers_count | orders_count |      repeat_share
---------------+-----------------+------------------------+---------------------------+--------------+------------------------
 all_customers |           96096 |                   2997 |                     93099 |        99441 | 0.03118756243756243756
(1 row)

            seller_id             | revenue_amount
----------------------------------+----------------
 4869f7a5dfa277a7dca6462dcf3b52b2 |      226987.93
(1 row)

 category_key | category_label | revenue_amount
--------------+----------------+----------------
 beleza_saude | health_beauty  |     1233131.72
(1 row)

 category_count | revenue_share_total
----------------+---------------------
             74 |  1.0000000000000002
(1 row)

                 category_key                  |                category_label                 | is_unknown_category | is_translation_missing
-----------------------------------------------+-----------------------------------------------+---------------------+------------------------
 __unknown__                                   | Категория не указана                          | t                   | f
 pc_gamer                                      | pc_gamer                                      | f                   | t
 portateis_cozinha_e_preparadores_de_alimentos | portateis_cozinha_e_preparadores_de_alimentos | f                   | t
(3 rows)

       model        | revenue_amount
--------------------+----------------
 monthly_sales      |    13221498.11
 seller_sales       |    13221498.11
 category_sales     |    13221498.11
 order_summary      |    13221498.11
 dds_active_revenue |    13221498.11
(5 rows)

 active_orders_without_items
-----------------------------
                         775
(1 row)

 unknown_customer_orders
-------------------------
                       0
(1 row)

 unknown_seller_items
----------------------
                    0
(1 row)

 category_reserved_key_collisions
----------------------------------
                                0
(1 row)

 seller_reserved_key_collisions
--------------------------------
                              0
(1 row)

 revenue_orders_outside_month_window
-------------------------------------
                                   0
(1 row)

```


## Детерминизм

Между двумя последними полными build данные не загружались. Оба использовали
одну отсечку `2026-10-28 12:37:17.709264+00` (по умолчанию максимум ODS).
После каждого запуска получены подписи **полных строк** пяти mart-таблиц:

```sql
select 'category_sales' as model, count(*) as rows, md5(string_agg(md5(t::text), '' order by md5(t::text))) as signature from mart.category_sales t
union all select 'customer_summary', count(*), md5(string_agg(md5(t::text), '' order by md5(t::text))) from mart.customer_summary t
union all select 'monthly_sales', count(*), md5(string_agg(md5(t::text), '' order by md5(t::text))) from mart.monthly_sales t
union all select 'order_summary', count(*), md5(string_agg(md5(t::text), '' order by md5(t::text))) from mart.order_summary t
union all select 'seller_sales', count(*), md5(string_agg(md5(t::text), '' order by md5(t::text))) from mart.seller_sales t
order by model;
```

Оба результата совпали побайтно (`cmp` завершился с кодом 0):

```text
      model       | rows |            signature
------------------+------+----------------------------------
 category_sales   |   74 | 1497c3b484104211f612f38d04960b3a
 customer_summary |    1 | 1ff80def7e69dd5ee88923370c6e94b4
 monthly_sales    |   24 | e10a6417e07a2b5f90dc82003716681b
 order_summary    |    1 | db2e34578a447cde9ffed02c9ca4b053
 seller_sales     | 2970 | daa1df3b0c867e5de78394ccd123c4a3
(5 rows)

```

Результат: **MART_DETERMINISM_PASSED**, все пять таблиц равны по подписям.
Служебных меняющихся полей в mart нет. Полные подписи также совпадают
с подписями после самого первого успешного полного build.

## Права

`dwh/init.sql` только прочитан. В живой БД проверены `has_schema_privilege`,
`has_table_privilege`, запись `pg_default_acl` и реальные `SELECT count(*)`
после `SET ROLE analyst` / `SET ROLE ch_reader` по каждой из пяти таблиц.
Вывод повторной проверки после финального build:

```text
Pager usage is off.
 role_name |    table_name    | schema_usage | table_select
-----------+------------------+--------------+--------------
 analyst   | category_sales   | t            | t
 analyst   | customer_summary | t            | t
 analyst   | monthly_sales    | t            | t
 analyst   | order_summary    | t            | t
 analyst   | seller_sales     | t            | t
 ch_reader | category_sales   | t            | t
 ch_reader | customer_summary | t            | t
 ch_reader | monthly_sales    | t            | t
 ch_reader | order_summary    | t            | t
 ch_reader | seller_sales     | t            | t
(10 rows)

 owner | schema | defaclobjtype |            defaclacl
-------+--------+---------------+---------------------------------
 dwh   | mart   | r             | {analyst=r/dwh,ch_reader=r/dwh}
(1 row)

SET
 role_name |      model       | count
-----------+------------------+-------
 analyst   | monthly_sales    |    24
 analyst   | seller_sales     |  2970
 analyst   | order_summary    |     1
 analyst   | customer_summary |     1
 analyst   | category_sales   |    74
(5 rows)

RESET
SET
 role_name |      model       | count
-----------+------------------+-------
 ch_reader | monthly_sales    |    24
 ch_reader | seller_sales     |  2970
 ch_reader | order_summary    |     1
 ch_reader | customer_summary |     1
 ch_reader | category_sales   |    74
(5 rows)

RESET
```

## Ограничения

- Материализация атомарна по одной таблице, а не по набору: во время сборки
  возможно окно рассогласования. Публикация в ClickHouse и `refresh_status`
  сюда не входят.
- Gate через `err_cast` действует при `dbt build`; отдельный `dbt run`
  не запускает проверки. Негативные fixtures в рабочую DDS не вносились.
- Наследуются ограничения DDS / ODS по выбранному срезу, обнаружению удалений
  и истории времени наблюдения. Mart собственную историю не накапливает.
- Частичный selector `+path:models/mart` не сохраняет невыбранные DDS views,
  которые зависят от пересоздаваемых таблиц. Существующий `dds_reconcile`
  использует одну из них. Рабочая команда в текущем объёме — полный `dbt build`.
- Новые корректные числа Olist вызывают warn, но обязательные инварианты всё
  равно могут остановить сборку: прежде всего доходный заказ вне месячного окна
  или нулевая общая выручка. Эти случаи нельзя молча обойти, сохранив контракт.

## Вопросы к владельцу

1. **Окно и срез.** Сохранять ли равенство выручки фиксированных 24 месяцев
   и всей выгрузки после появления доходных заказов вне окна или при раннем
   `as_of_dts`, когда календарь DDS ещё не покрывает окно? Сейчас приоритет
   у явного контракта: окно фиксировано, инварианты `error`, остальные итоги
   по всей выгрузке. На текущих данных доходных заказов вне окна нет.
2. **Неизвестные личности.** Подтвердить выбранные безопасные правила:
   Unknown-покупателя не считать одной реальной личностью, сохраняя его заказы
   в `orders_count`; Unknown-продавца показывать отдельным `__unknown__`,
   сохраняя его выручку. В коде есть комментарии; текущий результат эти решения
   не меняют. Коллизий системных ключей на текущем срезе нет.
3. **Нулевая выручка.** По умолчанию доля 0 при нулевом знаменателе;
   обязательный тест суммы долей 1 останавливает такую сборку.
   Нужен ли отдельный согласованный контракт пустого набора / нулевого оборота?
4. **Частичная сборка DDS.** Два частичных запуска выявили повторяемое удаление
   невыбранного `fct_payment__active` при пересборке его базы и ошибку в
   `dds_reconcile`. Полный build проходит и восстановил views. Поддержка
   частичного selector требует отдельного решения по выбору DDS-моделей / тестов
   за пределами разрешённых изменений; до этого использовать полный build.

## Артефакты проверки

Логи на хосте: `/tmp/mart-build-1.log`, `/tmp/mart-build-2.log`
(частичные запуски с описанной ошибкой upstream), `/tmp/mart-build-full.log`,
`/tmp/mart-build-full-2.log`, `/tmp/mart-build-full-3.log` (успешные полные).
SQL и выводы: `/tmp/mart-controls.sql`, `/tmp/mart-controls-final.out`,
`/tmp/mart-signatures.sql`, `/tmp/mart-signatures-1.out`,
`/tmp/mart-signatures-2.out`, `/tmp/mart-privileges.sql`,
`/tmp/mart-privileges-final.out`, `/tmp/mart-types.out`,
`/tmp/mart-business-queries.out`. Существенные результаты включены в этот отчёт,
чтобы он не зависел от сохранности временных файлов.


## Вброс и блок Б (28.10)

### Что сделано и изменённые файлы

Реализована согласованная `mart-spec-vbros-block-b.md`, без изменения её решений.
Сначала исправлено позднее измерение в DDS: первая полная версия HK на срезе
при неполной/отсутствующей версии на bind_dts; иначе версия на bind_dts или
первая версия HK. Unknown только при неизвестном/отсутствующем HK. Результат
описан в «Вброс: привязка версии» `dds-implementation.md`; вопрос о позднем
покупателе из прежнего отчёта закрыт. Иных изменений DDS в этой задаче нет.

- DDS: `dbt/macros/dds_bound_version.sql`, `dbt/models/dds/fct_order_item.sql`,
  `dbt/models/dds/fct_order.sql`, `.plans/dds-implementation.md`.
- Блок А: `dbt/models/mart/seller_sales.sql` (агрегация по HK, текущий адрес),
  `category_sales.sql` (исторический код по SK, агрегация только по коду,
  актуальные названия), `_mart.yml` и прежнее описание привязки в этом отчёте.
- Шесть новых моделей `dbt/models/mart/`: `product_price_history.sql`,
  `delivery_by_state.sql`, `order_status_funnel.sql`, `payments_by_state_year.sql`,
  `payments_by_type.sql`, `review_delivery.sql`. Все с зависимостью от err_cast.
  Все 40 новых колонок описаны в `_mart.yml`, зерно каждой модели протестировано.
- Семь новых singular-тестов: `dbt/tests/mart_price_reconciliation.sql`,
  `mart_delivery_reconciliation.sql`, `mart_funnel_reconciliation.sql`,
  `mart_payments_reconciliation.sql`, `mart_review_reconciliation.sql`,
  `mart_block_b_shares.sql` (error), `mart_control_block_b.sql` (warn).
- `scripts/check_dds.py`: сборка DDS и mart, проверки сценариев вброса,
  детерминизма, прошлой отсечки, full-refresh и сохранения mart при gate.
- SQL ответов `analysis/business_questions/sql/06_price_history.sql`,
  `07_delivery_by_state.sql`, `08_status_funnel.sql`, `09_payments.sql`,
  `10_review_delivery.sql` и `README.md` в этой папке.

Деньги — numeric(18,2), количества — bigint, даты — date, год и оценка — integer.
Сумма дней и средние — numeric без округления в витринах. Доли вычисляются
из numeric, затем приводятся к double precision; нулевой знаменатель даёт NULL.
Зерно с NULL-датой/годом проверяется через jsonb_build_array, NULL-оценка —
через coalesce(review_score, 0), поэтому NULL-группы тоже проверяются на дубли.
Категорийные названия выбираются сначала среди is_current, затем по valid_from
DESC; равные даты разрешаются HK и SK. Дополнительного фильтра удаления
измерений нет. Порог, год и выбор товара-лидера остаются в SQL ответов.

### Приёмка 1: полный build и тесты

Команды:

```bash
docker compose exec -T airflow bash -c 'cd /opt/project/dbt && dbt build --profiles-dir .'
docker compose exec -T airflow python - < scripts/check_dds.py
```

Отдельный build только с правкой DDS: **PASS=460 WARN=3 ERROR=0 SKIP=0 TOTAL=463**.
После реализации блока Б выполнены четыре полных build: каждый завершился
**PASS=485 WARN=3 ERROR=0 SKIP=0 TOTAL=488**. Финальная пара:

```text
build-3: 13:49:06 Done. PASS=485 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=488
build-4: 13:50:35 Done. PASS=485 WARN=3 ERROR=0 SKIP=0 NO-OP=0 TOTAL=488
```

Проверен финальный run_results.json: 86 success, 399 pass, 3 warn.
В mart: **11 success моделей + 41 pass тестов**, без warn/error/skip.
Новые инварианты сверяют платежи по сумме/числу/рассрочкам, воронку по заказам
и отсутствию позиций, отзывы по числу заказов, доставку, единицы и агрегаты
истории цен; доли — по диапазону и числителю/знаменателю. Прежние контроли
блока А успешны. Все SQL 01–10 выполнены через psql с ON_ERROR_STOP=1.
Через information_schema.columns проверены физические типы всех 73 колонок mart.
Синтаксис Python и `git diff --check` успешны; git-команды изменения состояния
не выполнялись. Уже имеющиеся изменения за пределами задачи не редактировались.

Предупреждения на живых данных — только прежние DDS:

| Тест | Строк |
|---|---:|
| dds_warn_is_approved_missing | 14 |
| dds_warn_is_carrier_before_purchase | 166 |
| dds_warn_is_delivered_without_date | 8 |

CLI также сообщает прежние замечания про версию dbt и неиспользуемый snapshots.marketplace;
это не data-test WARN.

### Приёмка 2: неизменность DDS и блока А

Отсечка всех живых build: **2026-10-28 12:37:17.709264+00**.
До любых изменений сохранены count(*) и MD5 полных строк 22 DDS-таблиц
(включая seeds) и 5 mart-таблиц; отдельно as_of_dts из build_info.
После отдельного исправления DDS все 28 записей совпали (`cmp` = 0).
После всей реализации те же 28 записей присутствуют без изменений.
У build_info исключены только built_at и dbt_invocation_id.

Сохранены все числа блока А: 24 месяца / 13 221 498.11 BRL; продавец-лидер
226 987.93; средний чек 159.88; 96 096 покупателей и 2 997 повторных;
74 категории, лидер 1 233 131.72 BRL. Все 33 исходные колонки, их порядок
и физические типы сохранены. Подписи полных строк блока А до = после:

| Таблица | Строк | MD5 полных строк |
|---|---:|---|
| `mart.category_sales` | 74 | `1497c3b484104211f612f38d04960b3a` |
| `mart.customer_summary` | 1 | `1ff80def7e69dd5ee88923370c6e94b4` |
| `mart.monthly_sales` | 24 | `e10a6417e07a2b5f90dc82003716681b` |
| `mart.order_summary` | 1 | `db2e34578a447cde9ffed02c9ca4b053` |
| `mart.seller_sales` | 2970 | `daa1df3b0c867e5de78394ccd123c4a3` |

Полные подписи DDS, включая добавленные ранее HK (до = после):

| Таблица | Строк | MD5 полных строк |
|---|---:|---|
| `dds.dim_customer` | 96097 | `8f374b777bc4cfa6f3ba1b0454dfc62d` |
| `dds.dim_date` | 1097 | `7ead4e8dc1e004df5bb2321fa546e66e` |
| `dds.dim_geo` | 28 | `2b78073313b41774b8d1b48da9e91412` |
| `dds.dim_order_status` | 9 | `eeed0b1fbed58709b948054213d67ac0` |
| `dds.dim_payment_type` | 6 | `f9deb9e041389f1208490965b5380591` |
| `dds.dim_product` | 32952 | `fcfce323742b92ebfed778f3a98984b8` |
| `dds.dim_seller` | 3096 | `3c1d44e9f44f724ef23e9d3bfe55da4c` |
| `dds.fct_order` | 99441 | `cd215eb1ff906ed3dfd2e516c7b6fe27` |
| `dds.fct_order_item` | 112650 | `a064d20924d2cde3e0bdb22406919620` |
| `dds.fct_payment` | 103886 | `99816f66adbe24aac5d4984a866a35b0` |
| `dds.fct_review` | 99224 | `7a3671c651302192bd2f74a88913c4d8` |
| `dds.int_as_of` | 1 | `c4df6ab9aff7bee71faf065da298b4b9` |
| `dds.int_order_item_candidates` | 112650 | `f3799ace1d074a9d2327f1ecdc169a03` |
| `dds.int_order_item_on_slice` | 112650 | `f3799ace1d074a9d2327f1ecdc169a03` |
| `dds.int_order_on_slice` | 99441 | `7ba74ec821a9a8744b85f457d06e24b6` |
| `dds.int_order_profile_on_slice` | 99441 | `66ee8ced5f7b7b988650cca6c2e70f53` |
| `dds.int_payment_on_slice` | 103886 | `1d0e489bd6c4e5c08186514d4508fb7e` |
| `dds.int_product_versions` | 32951 | `8980121416db72f4e888bd186c838c2d` |
| `dds.int_seller_versions` | 3095 | `7506b5fdc7dcd8f4329a42c9772d82f7` |
| `dds.seed_br_state` | 27 | `16d0933fe83968696b87bc301ca9ebd3` |
| `dds.seed_order_status` | 8 | `d2c77752d3500d4ba3e73b6deb9e04a4` |
| `dds.seed_payment_type` | 5 | `2221c0973f6c72e7e5d0b220111c1a8c` |

### Приёмка 3: ответы на вопросы 6–10

Округление ниже только для показа: средние до 4 знаков, доли до 6 (воронка
до 8). Доли остаются 0–1, деньги — BRL. Полные разрезы хранятся в витринах.

**6. История цен.** Лидер определяется по числу цен DESC, единицам DESC,
product_id ASC. Шапка ответа:

| product_id | distinct_price_count | product_unit_count |
|---|---|---|
| 437c05a395e9e47f9762e677a7068ce7 | 31 | 151 |

Первые 10 строк истории; SQL 06 возвращает её целиком по дате и цене:

| sale_date | price | unit_count |
|---|---|---|
| 2018-02-19 | 44.98 | 2 |
| 2018-02-20 | 45.36 | 4 |
| 2018-02-21 | 45.83 | 1 |
| 2018-02-21 | 46.33 | 4 |
| 2018-02-21 | 47.21 | 1 |
| 2018-02-22 | 47.61 | 1 |
| 2018-02-22 | 47.91 | 3 |
| 2018-02-24 | 49.83 | 1 |
| 2018-02-24 | 50.21 | 1 |
| 2018-02-25 | 50.70 | 1 |

**7. Доставка по штатам.** Порог 500 по всем активным заказам; показатели
доставки по is_revenue с датой. Все **17 строк** ответа:

| customer_state | state_name | region | orders_count | delivered_measured_count | avg_delivery_days | late_count | late_share |
|---|---|---|---|---|---|---|---|
| SP | São Paulo | Sudeste | 41746 | 40494 | 8.7614 | 1820 | 0.044945 |
| PR | Paraná | Sul | 5045 | 4923 | 11.9916 | 199 | 0.040423 |
| MG | Minas Gerais | Sudeste | 11635 | 11354 | 12.0087 | 519 | 0.045711 |
| DF | Distrito Federal | Centro-Oeste | 2140 | 2080 | 12.9676 | 118 | 0.056731 |
| SC | Santa Catarina | Sul | 3637 | 3546 | 14.9548 | 291 | 0.082064 |
| RS | Rio Grande do Sul | Sul | 5466 | 5344 | 15.3003 | 325 | 0.060816 |
| RJ | Rio de Janeiro | Sudeste | 12852 | 12350 | 15.3094 | 1495 | 0.121053 |
| GO | Goiás | Centro-Oeste | 2020 | 1957 | 15.6063 | 128 | 0.065406 |
| MS | Mato Grosso do Sul | Centro-Oeste | 715 | 701 | 15.6183 | 68 | 0.097004 |
| ES | Espírito Santo | Sudeste | 2033 | 1995 | 15.7893 | 214 | 0.107268 |
| MT | Mato Grosso | Centro-Oeste | 907 | 886 | 18.0558 | 53 | 0.059819 |
| PE | Pernambuco | Nordeste | 1652 | 1593 | 18.4483 | 153 | 0.096045 |
| BA | Bahia | Nordeste | 3380 | 3256 | 19.3355 | 396 | 0.121622 |
| PB | Paraíba | Nordeste | 536 | 517 | 20.4268 | 54 | 0.104449 |
| CE | Ceará | Nordeste | 1336 | 1279 | 21.2666 | 176 | 0.137608 |
| MA | Maranhão | Nordeste | 747 | 717 | 21.5730 | 125 | 0.174338 |
| PA | Pará | Norte | 975 | 946 | 23.7729 | 106 | 0.112051 |

**8. Воронка статусов.** Все **9 строк**, включая Unknown и нулевые статусы.
Итог **99 441 заказ**, без позиций **775**:

| order_status | status_group | orders_count | order_share | orders_without_items_count |
|---|---|---|---|---|
| delivered | выручка | 96478 | 0.97020344 | 0 |
| shipped | в процессе | 1107 | 0.01113223 | 1 |
| canceled | отмена | 625 | 0.00628513 | 164 |
| unavailable | отмена | 609 | 0.00612423 | 603 |
| invoiced | в процессе | 314 | 0.00315765 | 2 |
| processing | в процессе | 301 | 0.00302692 | 0 |
| created | в процессе | 5 | 0.00005028 | 5 |
| approved | в процессе | 2 | 0.00002011 | 0 |
| __unknown__ | __unknown__ | 0 | 0.00000000 | 0 |

**9. Платежи.** Любой статус заказа, каждый активный платёж — отдельная строка.
Все **27 штатов за 2018**:

| customer_state | purchase_year | payments_count | payment_amount | installments_sum | avg_installments | zero_installments_count |
|---|---|---|---|---|---|---|
| SP | 2018 | 24669 | 3419478.51 | 61865 | 2.5078 | 1 |
| RJ | 2018 | 6891 | 1075017.63 | 19375 | 2.8116 | 0 |
| MG | 2018 | 6385 | 1011722.65 | 18418 | 2.8846 | 0 |
| RS | 2018 | 2874 | 461414.54 | 8182 | 2.8469 | 0 |
| PR | 2018 | 2868 | 454774.16 | 7909 | 2.7577 | 0 |
| SC | 2018 | 1962 | 342903.73 | 5534 | 2.8206 | 0 |
| BA | 2018 | 1896 | 331326.10 | 5704 | 3.0084 | 1 |
| DF | 2018 | 1244 | 196851.14 | 3220 | 2.5884 | 0 |
| GO | 2018 | 1103 | 184863.90 | 3203 | 2.9039 | 0 |
| ES | 2018 | 1093 | 181783.48 | 3091 | 2.8280 | 0 |
| PE | 2018 | 904 | 169880.06 | 3127 | 3.4591 | 0 |
| CE | 2018 | 702 | 139854.11 | 2365 | 3.3689 | 0 |
| PA | 2018 | 481 | 106809.30 | 1507 | 3.1331 | 0 |
| MT | 2018 | 496 | 95208.89 | 1475 | 2.9738 | 0 |
| PB | 2018 | 294 | 78124.68 | 1063 | 3.6156 | 0 |
| MS | 2018 | 428 | 75279.66 | 1160 | 2.7103 | 0 |
| MA | 2018 | 368 | 75053.61 | 1048 | 2.8478 | 0 |
| PI | 2018 | 281 | 63184.61 | 945 | 3.3630 | 0 |
| RN | 2018 | 263 | 57185.92 | 952 | 3.6198 | 0 |
| AL | 2018 | 208 | 44537.49 | 671 | 3.2260 | 0 |
| SE | 2018 | 159 | 34066.53 | 563 | 3.5409 | 0 |
| TO | 2018 | 162 | 32657.39 | 444 | 2.7407 | 0 |
| RO | 2018 | 115 | 29382.48 | 358 | 3.1130 | 0 |
| AM | 2018 | 75 | 14935.32 | 213 | 2.8400 | 0 |
| AP | 2018 | 40 | 8931.27 | 108 | 2.7000 | 0 |
| RR | 2018 | 26 | 7852.79 | 81 | 3.1154 | 0 |
| AC | 2018 | 28 | 6683.10 | 85 | 3.0357 | 0 |

Все **5 способов за весь период**:

| payment_type | payments_count | payment_amount | installments_sum | avg_installments | zero_installments_count |
|---|---|---|---|---|---|
| credit_card | 76795 | 12542084.19 | 269341 | 3.5073 | 2 |
| boleto | 19784 | 2869361.27 | 19802 | 1.0009 | 0 |
| voucher | 5775 | 379436.87 | 5775 | 1.0000 | 0 |
| debit_card | 1529 | 217989.79 | 1529 | 1.0000 | 0 |
| not_defined | 3 | 0.00 | 3 | 1.0000 | 0 |

Сверки полных разрезов и отдельный итог 2018:

| model | payments_count | payment_amount | installments_sum | zero_installments_count |
|---|---|---|---|---|
| dds | 103886 | 16008872.12 | 296450 | 2 |
| state_year | 103886 | 16008872.12 | 296450 | 2 |
| type | 103886 | 16008872.12 | 296450 | 2 |
| state_year_2018 | 56015 | 8699763.05 | 152666 | 2 |

`not_defined` сохранён: 3 платежа на 0.00 BRL. Две нулевые рассрочки
у credit_card включены как 0 в сумму и среднее, обе в 2018.

**10. Оценка и доставка.** Последний активный отзыв на активный заказ;
даты DESC NULLS LAST, затем review_id DESC. Все строки ответа:

| review_score | orders_count | delivered_measured_count | avg_delivery_days | late_count | late_share |
|---|---|---|---|---|---|
| 1 | 11366 | 9353 | 21.3247 | 3432 | 0.366941 |
| 2 | 3129 | 2919 | 16.6274 | 550 | 0.188421 |
| 3 | 8133 | 7916 | 14.2562 | 694 | 0.087671 |
| 4 | 19044 | 18894 | 12.3060 | 649 | 0.034350 |
| 5 | 57001 | 56742 | 10.6810 | 1056 | 0.018611 |

Сверка с DDS по заказам, имеющим хотя бы один активный отзыв:

| model | orders_with_review | measured | late_count |
|---|---|---|---|
| mart | 98673 | 95824 | 6381 |
| dds | 98673 | 95824 | 6381 |

В текущих данных NULL-оценок у выбранных отзывов нет. Чем ниже оценка,
тем больше наблюдаемый срок и доля опозданий; это описательная связь.

### Приёмка 4: детерминизм

После завершения каждого из двух финальных последовательных полных build
сняты подписи всех DDS и mart. Формула:

```sql
select count(*), md5(string_agg(md5(t::text), '' order by md5(t::text)))
from mart.product_price_history t;
```

`after-build-3.tsv` = `after-build-4.tsv` побайтно; **MART_DETERMINISM_PASSED**.
Равны все 11 mart и все 23 DDS-таблицы (build_info — по as_of_dts).
Один промежуточный снимок до окончания build попал на временный __dbt_backup
и не использовался; финальные снимки выполнены строго после завершения процессов.

| Таблица | Строк | MD5 полных строк |
|---|---:|---|
| `mart.category_sales` | 74 | `1497c3b484104211f612f38d04960b3a` |
| `mart.customer_summary` | 1 | `1ff80def7e69dd5ee88923370c6e94b4` |
| `mart.delivery_by_state` | 27 | `602f10be0f39a523f211f8b98c08d7c9` |
| `mart.monthly_sales` | 24 | `e10a6417e07a2b5f90dc82003716681b` |
| `mart.order_status_funnel` | 9 | `236afad1a154c4dbbbcccca9efcae450` |
| `mart.order_summary` | 1 | `db2e34578a447cde9ffed02c9ca4b053` |
| `mart.payments_by_state_year` | 75 | `c8f1e4412636dcf2990581cd3a829983` |
| `mart.payments_by_type` | 5 | `c2e2a106762d3b41ac6e8722a602df55` |
| `mart.product_price_history` | 92686 | `33f4c5891912e3b6cbad6cc038ef148a` |
| `mart.review_delivery` | 5 | `968ed1c71c15cec33801463f0a4f9d5d` |
| `mart.seller_sales` | 2970 | `daa1df3b0c867e5de78394ccd123c4a3` |

### Приёмка 5: harness DDS + mart

Итог: **DDS_ACCEPTANCE_PASSED; tables compared: 23**, плюс отдельно
сравнены все 11 mart. Все прежние DDS-сценарии сохранены.
Восемь успешных запусков dbt (семь обычных и один на прошлую отсечку),
три ожидаемо отрицательных:

| Запуск | PASS | WARN | ERROR | SKIP | TOTAL |
|---|---:|---:|---:|---:|---:|
| 01_initial, 02_repeat, 03_changes, 05_latest, 06_return, 07_latest_repeat, 08_full_refresh (каждый) | 477 | 9 | 0 | 0 | 486 |
| 04_past_slice (DDS и mart без пересборки ODS) | 197 | 9 | 0 | 0 | 206 |
| 09_stg_order_profile_conflicts | 220 | 0 | 1 | 265 | 486 |
| 09_stg_customer_profile_link_conflicts | 232 | 0 | 1 | 253 | 486 |
| 10_critical | 274 | 0 | 1 | 211 | 486 |

В синтетической БД исключён только `mart_monthly_calendar`: fixture не содержит
фиксированных 24 месяцев Olist. Остальные error-инварианты mart выполняются.
Все шесть `mart_control_*` остаются warn и ожидаемо предупреждают на fixture;
ещё три предупреждения — `dds_warn_product_incomplete`,
`dds_warn_is_approved_missing`, `dds_warn_is_delivered_without_date`.
На живом полном build календарь и все контроли проверяются без исключений.

Подтверждены сценарии mart:

- A→B: прежняя выручка A **100.00** сохраняется; B получает новую продажу
  **100.00** и поздний товар **30.00**, итог B **130.00**; коды не дублируются.
- Продавец Old→New: **одна строка New с выручкой 200.00**, хотя прежняя
  продажа DDS сохраняет исторический SK адреса Old.
- Профиль Other/RJ→Moved/MG: **два старых заказа остаются в RJ**, две новые
  связи относятся к MG. Смена профиля заказа проверена отдельно в DDS.
- Постоянная история товара после смены категории содержит один product_id
  и две единицы; поздний перевод B обновляет название без дробления кода.
- O5 до появления профиля — Unknown; после прихода — известный HK/SK полной
  версии покупателя с customer_unique_id из 32 символов b и географией Late.
- Прошлая отсечка, повтор последнего среза и full-refresh воспроизводят
  подписи всех DDS и mart. Оба staging-конфликта сохраняют прежние таблицы;
  критическая цена пропускает все 24 DDS-модели и все 11 mart-моделей,
  не меняя их полных подписей (build_info здесь сравнивается целиком).

Временная БД `dds_review_36ae6ebafa66` удалена в finally. После harness
`pg_database` не содержит ни одной `dds_review_%` БД. Логи и обе группы
подписей внутри Airflow: `/tmp/dds-review-80j3swmh/`,
`fingerprints.json` и `mart-fingerprints.json`.

### Вопросы к владельцу

1. Спецификация не задаёт порядок NULL при выборе последнего отзыва.
   Консервативно известная дата приоритетнее отсутствующей (`DESC NULLS LAST`
   для creation и answer), затем максимальный review_id. Подтвердить это
   правило для будущих отзывов без дат. Сейчас таких активных отзывов **0**,
   на приведённые ответы это не влияет.
2. Спецификация требует сохранить все активные платежи и использовать активные
   факты. Если активного заказа для платежа нет, LEFT JOIN сохраняет платёж
   в `__unknown__`-штате с NULL-годом (не попадает в срез 2018). Подтвердить
   эту географию/год для такого случая. Сейчас таких платежей **0**;
   оба полных разреза всегда сверяются со всеми активными платежами.

Вопрос о позднем измерении из отчёта DDS закрыт уточнённым §20 и сценарием O5.

Логи и машинные результаты на хосте: `/tmp/mart-block-b/`:
`before.tsv`, `after-dds.tsv`, `after-build-3.tsv`, `after-build-4.tsv`,
`dds-fix-build.log`, `build-1.log`–`build-4.log`, `harness.log`,
результаты SQL 01–10, CSV ответов и `columns.csv`.
