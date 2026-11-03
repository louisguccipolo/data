{{ config(severity='warn') }}
select * from {{ ref('customer_summary') }}
where customers_count is distinct from 96096::bigint
    or repeat_customers_count is distinct from 2997::bigint
    or one_order_customers_count is distinct from 93099::bigint
    or orders_count is distinct from 99441::bigint
    or round(100.0 * repeat_customers_count / nullif(customers_count, 0), 2) is distinct from 3.12::numeric
