{{ config(severity='error') }}
select 'product' as entity, i.order_hk, i.product_sk as dimension_sk,
    i.product_hk as fact_hk, p.product_hk as dimension_hk
from {{ ref('fct_order_item') }} i
left join {{ ref('dim_product') }} p using (product_sk)
where i.product_hk is distinct from p.product_hk
union all
select 'seller', i.order_hk, i.seller_sk, i.seller_hk, s.seller_hk
from {{ ref('fct_order_item') }} i
left join {{ ref('dim_seller') }} s using (seller_sk)
where i.seller_hk is distinct from s.seller_hk
union all
select 'customer', o.order_hk, o.customer_sk, o.customer_hk, c.customer_hk
from {{ ref('fct_order') }} o
left join {{ ref('dim_customer') }} c using (customer_sk)
where o.customer_hk is distinct from c.customer_hk
