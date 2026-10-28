{{ config(severity='warn') }}
select * from {{ ref('order_summary') }}
where orders_count is distinct from 96478::bigint
    or revenue_amount is distinct from 13221498.11::numeric
    or freight_amount is distinct from 2202965.54::numeric
    or order_value_amount is distinct from 15424463.65::numeric
    or line_count is distinct from 110197::bigint
    or unit_count is distinct from 110197::bigint
    or round(order_value_amount / nullif(orders_count, 0), 2) is distinct from 159.88::numeric
    or round(line_count::numeric / nullif(orders_count, 0), 3) is distinct from 1.142::numeric
    or round(unit_count::numeric / nullif(orders_count, 0), 3) is distinct from 1.142::numeric
