select category_hk, load_dts
from {{ ref('stg_product_category_translation') }}
group by category_hk, load_dts having count(distinct category_hashdiff) > 1
