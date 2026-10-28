-- depends_on: {{ ref('err_cast') }}
select h.order_id, i.order_item_id, i.order_hk, i.order_item_hk,
    coalesce(p.product_sk, repeat('0',32)::char(32)) as product_sk,
    coalesce(s.seller_sk, repeat('0',32)::char(32)) as seller_sk,
    coalesce(d.date_key, -1) as order_purchase_date_key,
    i.price, i.freight_value, i.shipping_limit_ts, i.is_deleted
from {{ ref('int_order_item_on_slice') }} i join {{ ref('hub_order') }} h using (order_hk)
left join {{ ref('dim_product') }} p on p.product_hk = i.product_hk and p.is_current
left join {{ ref('dim_seller') }} s on s.seller_hk = i.seller_hk and s.is_current
left join {{ ref('int_order_on_slice') }} o using (order_hk)
left join {{ ref('dim_date') }} d on d.date = o.order_purchase_ts::date
