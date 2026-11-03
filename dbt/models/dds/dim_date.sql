-- depends_on: {{ ref('err_cast') }}
with dates as (
    select unnest(array[order_purchase_ts, order_approved_ts, order_delivered_carrier_ts,
        order_delivered_customer_ts, order_estimated_delivery_ts])::date as date
    from {{ ref('int_order_on_slice') }}
), calendar as (
    select generate_series(date_trunc('year', min(date)),
        date_trunc('year', max(date)) + interval '1 year - 1 day', interval '1 day')::date as date
    from dates
)
select to_char(date, 'YYYYMMDD')::int as date_key, date,
    extract(year from date)::int as year, extract(quarter from date)::int as quarter,
    extract(month from date)::int as month, date_trunc('month', date)::date as month_start,
    to_char(date, 'YYYY-MM') as year_month, extract(day from date)::int as day_of_month,
    extract(isodow from date)::int as iso_day_of_week, extract(isodow from date) > 5 as is_weekend
from calendar
union all select -1, null::date, null::int, null::int, null::int, null::date, null::text, null::int, null::int, null::boolean
