select review_hk, load_dts
from {{ ref('stg_order_reviews') }}
group by review_hk, load_dts having count(distinct review_hashdiff) > 1
