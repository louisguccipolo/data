{{ config(severity='error') }}
select count(*) as actual_months
from {{ ref('monthly_sales') }}
having count(*) <> 24
    or min(month) is distinct from date '2016-09-01'
    or max(month) is distinct from date '2018-08-01'
    or count(*) filter (where month <> date_trunc('month', month)::date) <> 0
