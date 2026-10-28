CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.monthly_sales
(
    month Date,
    orders_all_count UInt64,
    orders_delivered_count UInt64,
    revenue_amount Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY month;

CREATE TABLE IF NOT EXISTS mart.seller_sales
(
    seller_id String,
    seller_city Nullable(String),
    seller_state Nullable(String),
    orders_count UInt64,
    line_count UInt64,
    unit_count UInt64,
    revenue_amount Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS mart.order_summary
(
    summary_key String,
    orders_count UInt64,
    revenue_amount Decimal(18, 2),
    freight_amount Decimal(18, 2),
    order_value_amount Decimal(18, 2),
    line_count UInt64,
    unit_count UInt64
)
ENGINE = MergeTree
ORDER BY summary_key;

CREATE TABLE IF NOT EXISTS mart.customer_summary
(
    summary_key String,
    customers_count UInt64,
    repeat_customers_count UInt64,
    one_order_customers_count UInt64,
    orders_count UInt64
)
ENGINE = MergeTree
ORDER BY summary_key;

CREATE TABLE IF NOT EXISTS mart.category_sales
(
    category_key String,
    category_name_original Nullable(String),
    category_name_english Nullable(String),
    category_label String,
    is_unknown_category UInt8,
    is_translation_missing UInt8,
    orders_count UInt64,
    unit_count UInt64,
    revenue_amount Decimal(18, 2),
    revenue_share Float64
)
ENGINE = MergeTree
ORDER BY category_key;

CREATE TABLE IF NOT EXISTS mart.refresh_status
(
    batch_id String,
    published_at DateTime64(3, 'UTC'),
    source_rows UInt64,
    checks_passed UInt8
)
ENGINE = MergeTree
ORDER BY batch_id;

CREATE TABLE IF NOT EXISTS mart.product_price_history
(
    product_id String,
    sale_date Nullable(Date),
    price Decimal(18, 2),
    unit_count UInt64,
    orders_count UInt64,
    distinct_price_count UInt64,
    product_unit_count UInt64
)
ENGINE = MergeTree
ORDER BY (product_id, sale_date, price)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS mart.order_status_funnel
(
    order_status String,
    status_group String,
    orders_count UInt64,
    order_share Nullable(Float64),
    orders_without_items_count UInt64
)
ENGINE = MergeTree
ORDER BY order_status;

CREATE TABLE IF NOT EXISTS mart.payments_by_type
(
    payment_type String,
    payments_count UInt64,
    payment_amount Decimal(18, 2),
    installments_sum UInt64,
    avg_installments Float64,
    zero_installments_count UInt64
)
ENGINE = MergeTree
ORDER BY payment_type;

CREATE TABLE IF NOT EXISTS mart.payments_by_state_year
(
    customer_state String,
    purchase_year Nullable(Int32),
    payments_count UInt64,
    payment_amount Decimal(18, 2),
    installments_sum UInt64,
    avg_installments Float64,
    zero_installments_count UInt64
)
ENGINE = MergeTree
ORDER BY (customer_state, purchase_year)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS mart.delivery_by_state
(
    customer_state String,
    state_name Nullable(String),
    region Nullable(String),
    orders_count UInt64,
    delivered_measured_count UInt64,
    delivery_days_sum Decimal(38, 19),
    avg_delivery_days Nullable(Float64),
    late_count UInt64,
    late_share Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY customer_state;

CREATE TABLE IF NOT EXISTS mart.review_delivery
(
    review_score Nullable(Int32),
    orders_count UInt64,
    delivered_measured_count UInt64,
    avg_delivery_days Nullable(Float64),
    late_count UInt64,
    late_share Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY review_score
SETTINGS allow_nullable_key = 1;
