{{ config(severity='error') }}
select 'share_total' as check_name
from {{ ref('category_sales') }}
having sum(revenue_share) is null or abs(sum(revenue_share) - 1::double precision) > 1e-9
union all
select category_key from {{ ref('category_sales') }}
where revenue_share is null or revenue_share < 0 or revenue_share > 1
