select product_hk, load_dts
from {{ ref('stg_products') }}
group by product_hk, load_dts having count(distinct product_hashdiff) > 1
