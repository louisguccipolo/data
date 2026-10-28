-- depends_on: {{ ref('err_cast') }}
select h.order_id, p.order_hk, p.payment_sequential,
    coalesce(t.payment_type, '__unknown__') as payment_type, p.payment_installments, p.payment_value,
    coalesce(d.date_key, -1) as order_purchase_date_key, p.is_deleted
from {{ ref('int_payment_on_slice') }} p join {{ ref('hub_order') }} h using (order_hk)
left join {{ ref('dim_payment_type') }} t on t.payment_type = p.payment_type
left join {{ ref('int_order_on_slice') }} o using (order_hk)
left join {{ ref('dim_date') }} d on d.date = o.order_purchase_ts::date
