select order_hk, load_dts
from {{ ref('stg_orders') }}
group by order_hk, load_dts having count(distinct order_status_hashdiff) > 1
