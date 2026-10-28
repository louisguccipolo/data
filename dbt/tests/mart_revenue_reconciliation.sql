{{ config(severity='error') }}
with totals as (
    select 'monthly_sales' as model, sum(revenue_amount) as revenue_amount from {{ ref('monthly_sales') }}
    union all
    select 'seller_sales', sum(revenue_amount) from {{ ref('seller_sales') }}
    union all
    select 'category_sales', sum(revenue_amount) from {{ ref('category_sales') }}
), expected as (
    select revenue_amount from {{ ref('order_summary') }}
)
select t.model, t.revenue_amount as actual, e.revenue_amount as expected
from totals t cross join expected e
where t.revenue_amount is distinct from e.revenue_amount
