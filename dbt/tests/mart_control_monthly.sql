{{ config(severity='warn') }}
-- Регрессия исходного Olist: изменение набора данных не блокирует публикацию.
select 'revenue_amount' as check_name from {{ ref('monthly_sales') }}
having sum(revenue_amount) is distinct from 13221498.11::numeric
union all
select 'orders_delivered_count' from {{ ref('monthly_sales') }}
having sum(orders_delivered_count) is distinct from 96478::numeric
union all
select 'november_2016_zero' from {{ ref('monthly_sales') }}
having count(*) filter (where month = date '2016-11-01'
    and orders_all_count = 0 and orders_delivered_count = 0 and revenue_amount = 0) <> 1
