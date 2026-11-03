-- depends_on: {{ ref('err_cast') }}
select distinct on (order_hk, order_item_id) *
from {{ ref('int_order_item_candidates') }}
order by order_hk, order_item_id, priority,
    case when priority = 3 then eff_load_dts end desc,
    case when priority = 2 then load_dts end desc,
    case when priority = 2 then sat_load_dts end desc nulls last, order_item_hk
