select product_hk, load_dts
from {{ ref('stg_products') }}
group by 1,2 having count(distinct product_category_hk) > 1
