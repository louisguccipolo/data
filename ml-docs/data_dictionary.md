# Словарь данных Olist

Описание 9 таблиц из [`datasets/`](../datasets): что хранит каждая таблица, её столбцы, типы данных, ключи (PK/FK) и найденные особенности данных.

Все выводы сделаны по самим CSV-файлам: проверены уникальность ключей, пропуски, диапазоны значений и ссылочная целостность между таблицами.

**Обозначения в столбце «Ключ»:**

- **PK** — первичный ключ (или часть составного первичного ключа).
- **FK → таблица.столбец** — внешний ключ.
- **FK (логич.)** — логическая связь без настоящей ссылочной целостности: в родительской таблице значение не уникально или есть «сироты».
- **—** — обычный атрибут.

**Типы данных:** `pandas` — какой тип стоит задать при чтении, `SQL` — рекомендуемый тип для загрузки в реляционную БД (PostgreSQL).

---

## Схема связей

```
                    order_payments ──(order_id)──┐
                                                 │
order_reviews ──(order_id)──► orders ◄──(order_id)── order_items ──(product_id)──► products ──(category)──► category_translation
                                │                         │
                          (customer_id)              (seller_id)
                                ▼                         ▼
                            customers                  sellers
                                │                         │
                         (zip_code_prefix)         (zip_code_prefix)
                                └──────► geolocation ◄────┘
```

| Связь | Кардинальность | Целостность |
|---|---|---|
| `orders.customer_id` → `customers.customer_id` | 1 : 1 | полная |
| `order_items.order_id` → `orders.order_id` | N : 1 | полная; 775 заказов без товаров |
| `order_items.product_id` → `products.product_id` | N : 1 | полная |
| `order_items.seller_id` → `sellers.seller_id` | N : 1 | полная |
| `order_payments.order_id` → `orders.order_id` | N : 1 | полная; 1 заказ без оплаты |
| `order_reviews.order_id` → `orders.order_id` | N : 1 | полная; 768 заказов без отзыва |
| `products.product_category_name` → `category_translation.product_category_name` | N : 1 | 2 категории (13 товаров) без перевода |
| `customers.customer_zip_code_prefix` → `geolocation.geolocation_zip_code_prefix` | N : M | 158 индексов (279 клиентов) нет в geolocation |
| `sellers.seller_zip_code_prefix` → `geolocation.geolocation_zip_code_prefix` | N : M | 8 индексов (8 продавцов) нет в geolocation |

---

## 1. `olist_orders_dataset.csv` — заказы

Центральная таблица модели: одна строка = один заказ, с его статусом и датами жизненного цикла (покупка → подтверждение оплаты → передача перевозчику → доставка клиенту).

**Строк:** 99 441 · **PK:** `order_id` · **Период:** 2016-09-04 — 2018-10-17

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `order_id` | **PK** | `string` | `CHAR(32)` | 0 | Уникальный идентификатор заказа (32-символьный hex-хэш). |
| `customer_id` | **FK** → `customers.customer_id` | `string` | `CHAR(32)` | 0 | Идентификатор клиента в рамках этого заказа. Уникален в таблице: у каждого заказа свой `customer_id` (связь 1:1). |
| `order_status` | — | `category` | `VARCHAR(11)` | 0 | Статус заказа, 8 значений: `delivered` (96 478), `shipped` (1 107), `canceled` (625), `unavailable` (609), `invoiced` (314), `processing` (301), `created` (5), `approved` (2). |
| `order_purchase_timestamp` | — | `datetime64` | `TIMESTAMP` | 0 | Дата и время оформления заказа. |
| `order_approved_at` | — | `datetime64` | `TIMESTAMP` | 160 | Дата и время подтверждения оплаты. |
| `order_delivered_carrier_date` | — | `datetime64` | `TIMESTAMP` | 1 783 | Дата и время передачи заказа логистическому партнёру. |
| `order_delivered_customer_date` | — | `datetime64` | `TIMESTAMP` | 2 965 | Фактическая дата и время доставки клиенту. |
| `order_estimated_delivery_date` | — | `datetime64` | `DATE` | 0 | Обещанная клиенту дата доставки (время всегда `00:00:00`, по сути это дата). |

**Особенности:**

- Пропуски в датах ожидаемы для неоконченных и отменённых заказов. Но у 8 заказов со статусом `delivered` нет даты доставки.
- 775 заказов не имеют строк в `order_items`: в основном `unavailable` (603) и `canceled` (164).

---

## 2. `olist_order_items_dataset.csv` — позиции заказа

Одна строка = одна единица товара в заказе. Если в заказе 3 одинаковых товара, будет 3 строки с `order_item_id` = 1, 2, 3. Количества (quantity) в таблице нет.

**Строк:** 112 650 · **PK:** (`order_id`, `order_item_id`)

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `order_id` | **PK**, **FK** → `orders.order_id` | `string` | `CHAR(32)` | 0 | Заказ, к которому относится позиция. |
| `order_item_id` | **PK** | `int8` | `SMALLINT` | 0 | Порядковый номер позиции внутри заказа (1–21). Максимальный номер равен числу товаров в заказе. |
| `product_id` | **FK** → `products.product_id` | `string` | `CHAR(32)` | 0 | Купленный товар. |
| `seller_id` | **FK** → `sellers.seller_id` | `string` | `CHAR(32)` | 0 | Продавец, который продал эту позицию. |
| `shipping_limit_date` | — | `datetime64` | `TIMESTAMP` | 0 | Крайний срок, до которого продавец должен передать товар перевозчику. |
| `price` | — | `float64` | `NUMERIC(10,2)` | 0 | Цена единицы товара, BRL (0.85 — 6 735.00). |
| `freight_value` | — | `float64` | `NUMERIC(10,2)` | 0 | Стоимость доставки этой позиции, BRL (0.00 — 409.68). Если в заказе несколько товаров, фрахт делится между позициями. |

**Особенности:**

- В 1 278 заказах товары от нескольких продавцов. Один и тот же товар (`product_id`) продают разные продавцы в 1 225 случаях.
- `shipping_limit_date` доходит до 2020-04-09: явные выбросы.
- Сумма `price + freight_value` по заказу совпадает с суммой оплат (`order_payments`) в 99.5 % заказов.

---

## 3. `olist_order_payments_dataset.csv` — оплаты

Одна строка = один платёж по заказу. Заказ можно оплатить несколькими способами (например, карта + ваучеры), тогда платежей несколько.

**Строк:** 103 886 · **PK:** (`order_id`, `payment_sequential`)

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `order_id` | **PK**, **FK** → `orders.order_id` | `string` | `CHAR(32)` | 0 | Оплачиваемый заказ. |
| `payment_sequential` | **PK** | `int8` | `SMALLINT` | 0 | Порядковый номер платежа внутри заказа (1–29). |
| `payment_type` | — | `category` | `VARCHAR(11)` | 0 | Способ оплаты: `credit_card` (76 795), `boleto` (19 784, банковский счёт-квитанция), `voucher` (5 775), `debit_card` (1 529), `not_defined` (3). |
| `payment_installments` | — | `int8` | `SMALLINT` | 0 | Количество платежей в рассрочку (0–24). Для не-кредитных способов обычно 1. |
| `payment_value` | — | `float64` | `NUMERIC(10,2)` | 0 | Сумма платежа, BRL (0.00 — 13 664.08). |

**Особенности:** 2 платежа с `payment_installments = 0`, 9 платежей с `payment_value = 0`, 3 платежа с типом `not_defined`.

---

## 4. `olist_order_reviews_dataset.csv` — отзывы

Оценки и комментарии клиентов. Опрос отправляется клиенту после доставки (или после ожидаемой даты доставки).

**Строк:** 99 224 · **PK:** (`review_id`, `order_id`)

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `review_id` | **PK** | `string` | `CHAR(32)` | 0 | Идентификатор отзыва. **Не уникален сам по себе:** 814 дублей, один отзыв может относиться к 2–3 заказам. |
| `order_id` | **PK**, **FK** → `orders.order_id` | `string` | `CHAR(32)` | 0 | Заказ, к которому оставлен отзыв. У 547 заказов больше одного отзыва (до 3). |
| `review_score` | — | `int8` | `SMALLINT` | 0 | Оценка от 1 до 5: 5 (57 328), 4 (19 142), 3 (8 179), 2 (3 151), 1 (11 424). |
| `review_comment_title` | — | `string` | `VARCHAR(30)` | 87 656 (88 %) | Заголовок комментария, на португальском. |
| `review_comment_message` | — | `string` | `TEXT` | 58 247 (59 %) | Текст комментария, на португальском (до 208 символов). |
| `review_creation_date` | — | `datetime64` | `TIMESTAMP` | 0 | Дата отправки опроса клиенту. |
| `review_answer_timestamp` | — | `datetime64` | `TIMESTAMP` | 0 | Дата и время ответа клиента на опрос. |

---

## 5. `olist_customers_dataset.csv` — клиенты

Адресные данные клиента. Важно: `customer_id` создаётся заново на каждый заказ, а реального покупателя идентифицирует `customer_unique_id`.

**Строк:** 99 441 · **PK:** `customer_id`

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `customer_id` | **PK** | `string` | `CHAR(32)` | 0 | Идентификатор клиента в заказе (ключ для связи с `orders`, 1:1). |
| `customer_unique_id` | — | `string` | `CHAR(32)` | 0 | Постоянный идентификатор покупателя: 96 096 уникальных значений, у одного покупателя до 17 заказов. Используйте его для анализа повторных покупок и когорт. |
| `customer_zip_code_prefix` | **FK (логич.)** → `geolocation.geolocation_zip_code_prefix` | `string` | `CHAR(5)` | 0 | Первые 5 цифр почтового индекса (CEP). Ведущие нули потеряны: значения длиной 4–5 символов, нужно дополнять через `zfill(5)`. |
| `customer_city` | — | `string` | `VARCHAR(40)` | 0 | Город, в нижнем регистре, без диакритики (4 119 городов). |
| `customer_state` | — | `category` | `CHAR(2)` | 0 | Код штата Бразилии (27 значений, например `SP`, `RJ`, `MG`). |

---

## 6. `olist_sellers_dataset.csv` — продавцы

Справочник продавцов (магазинов-партнёров), которые выполняли заказы.

**Строк:** 3 095 · **PK:** `seller_id`

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `seller_id` | **PK** | `string` | `CHAR(32)` | 0 | Уникальный идентификатор продавца. |
| `seller_zip_code_prefix` | **FK (логич.)** → `geolocation.geolocation_zip_code_prefix` | `string` | `CHAR(5)` | 0 | Первые 5 цифр почтового индекса продавца. Ведущие нули тоже потеряны. |
| `seller_city` | — | `string` | `VARCHAR(40)` | 0 | Город продавца (611 городов). |
| `seller_state` | — | `category` | `CHAR(2)` | 0 | Штат продавца (23 штата). 60 % продавцов из `SP` (1 849). |

---

## 7. `olist_products_dataset.csv` — товары

Справочник товаров с категорией, характеристиками карточки и габаритами.

**Строк:** 32 951 · **PK:** `product_id`

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `product_id` | **PK** | `string` | `CHAR(32)` | 0 | Уникальный идентификатор товара. |
| `product_category_name` | **FK** → `category_translation.product_category_name` | `category` | `VARCHAR(50)` | 610 | Категория товара на португальском (73 категории). |
| `product_name_lenght` | — | `Int16` | `SMALLINT` | 610 | Длина названия товара, символов (5–76). Опечатка `lenght` в исходном имени столбца. |
| `product_description_lenght` | — | `Int16` | `SMALLINT` | 610 | Длина описания товара, символов (4–3 992). |
| `product_photos_qty` | — | `Int8` | `SMALLINT` | 610 | Количество фото в карточке (1–20). |
| `product_weight_g` | — | `Int32` | `INTEGER` | 2 | Вес, граммы (0–40 425). У 4 товаров вес 0. |
| `product_length_cm` | — | `Int16` | `SMALLINT` | 2 | Длина, см (7–105). |
| `product_height_cm` | — | `Int16` | `SMALLINT` | 2 | Высота, см (2–105). |
| `product_width_cm` | — | `Int16` | `SMALLINT` | 2 | Ширина, см (6–118). |

**Особенности:**

- Для 610 товаров нет ни категории, ни характеристик карточки (это одни и те же строки).
- В `pandas` эти числовые столбцы читаются как `float64` из-за пропусков. Чтобы сохранить целые, используйте nullable-типы (`Int16` и т.п.).
- Категорий `pc_gamer` и `portateis_cozinha_e_preparadores_de_alimentos` нет в таблице переводов.

---

## 8. `product_category_name_translation.csv` — перевод категорий

Справочник: название категории на португальском → на английском.

**Строк:** 71 · **PK:** `product_category_name`

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `product_category_name` | **PK** | `string` | `VARCHAR(50)` | 0 | Название категории на португальском (как в `products`). |
| `product_category_name_english` | — | `string` | `VARCHAR(50)` | 0 | Название категории на английском (например, `health_beauty`). |

**Особенности:** файл начинается с BOM. Читайте с `encoding="utf-8-sig"`, иначе первый столбец будет называться `﻿product_category_name`.

---

## 9. `olist_geolocation_dataset.csv` — геолокация

Координаты, привязанные к 5-значному префиксу почтового индекса. На один префикс приходится много точек (медиана 29, максимум 1 146).

**Строк:** 1 000 163 · **PK:** нет

| Столбец | Ключ | pandas | SQL | Пропуски | Описание |
|---|---|---|---|---|---|
| `geolocation_zip_code_prefix` | — (логический ключ связи, неуникален) | `string` | `CHAR(5)` | 0 | Префикс индекса, здесь уже с ведущими нулями (`"01037"`). 19 015 уникальных значений. |
| `geolocation_lat` | — | `float64` | `DOUBLE PRECISION` | 0 | Широта. |
| `geolocation_lng` | — | `float64` | `DOUBLE PRECISION` | 0 | Долгота. |
| `geolocation_city` | — | `string` | `VARCHAR(40)` | 0 | Город. Написание неоднородно (с диакритикой и без): у одного префикса до 5 вариантов. |
| `geolocation_state` | — | `category` | `CHAR(2)` | 0 | Штат (27 значений). |

**Особенности:**

- 261 831 строка — полные дубликаты.
- Естественного первичного ключа нет. Чтобы связывать по индексу, сначала агрегируйте до одной строки на префикс (например, медиана lat/lng) и используйте префикс как PK.
- 31 точка лежит за пределами Бразилии (широта до 45°, долгота от −101° до 121°): это ошибки координат.

---

## Рекомендации по чтению в pandas

```python
import pandas as pd

zip_cols = {
    "customer_zip_code_prefix": "string",
    "seller_zip_code_prefix": "string",
    "geolocation_zip_code_prefix": "string",
}

orders = pd.read_csv(
    "datasets/olist_orders_dataset.csv",
    parse_dates=[
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
)
customers = pd.read_csv("datasets/olist_customers_dataset.csv", dtype=zip_cols)
customers["customer_zip_code_prefix"] = customers["customer_zip_code_prefix"].str.zfill(5)
translation = pd.read_csv("datasets/product_category_name_translation.csv", encoding="utf-8-sig")
```
