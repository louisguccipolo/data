select customer_profile_hk, load_dts
from {{ ref('stg_customers') }}
group by customer_profile_hk, load_dts having count(distinct customer_profile_hashdiff) > 1
