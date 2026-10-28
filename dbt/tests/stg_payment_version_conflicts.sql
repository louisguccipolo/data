select order_hk, load_dts, payment_sequential from {{ ref('stg_order_payments') }} group by 1,2,3 having count(distinct payment_row_hashdiff) > 1
