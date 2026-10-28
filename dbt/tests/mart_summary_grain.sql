{{ config(severity='error') }}
select 'order_summary' as model, count(*) as actual_rows
from {{ ref('order_summary') }}
having count(*) <> 1 or min(summary_key) is distinct from 'delivered_all'
union all
select 'customer_summary', count(*)
from {{ ref('customer_summary') }}
having count(*) <> 1 or min(summary_key) is distinct from 'all_customers'
