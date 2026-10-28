select seller_hk, load_dts
from {{ ref('stg_sellers') }}
group by seller_hk, load_dts having count(distinct seller_hashdiff) > 1
