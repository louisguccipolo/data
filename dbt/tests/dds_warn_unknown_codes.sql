{{ config(severity='warn') }}
select 'payment_type' as domain, p.payment_type as code
from {{ ref('int_payment_on_slice') }} p left join {{ ref('seed_payment_type') }} s using (payment_type)
where p.payment_type is not null and s.payment_type is null
union all
select 'order_status', o.order_status
from {{ ref('int_order_on_slice') }} o left join {{ ref('seed_order_status') }} s using (order_status)
where o.order_status is not null and s.order_status is null
union all
select 'customer_state', p.customer_state
from {{ ref('int_order_profile_on_slice') }} p left join {{ ref('seed_br_state') }} s on s.state_code = upper(trim(p.customer_state))
where p.customer_state is not null and s.state_code is null
union all
select 'seller_state', p.seller_state
from {{ ref('dim_seller') }} p left join {{ ref('seed_br_state') }} s on s.state_code = upper(trim(p.seller_state))
where p.is_current and p.seller_state is not null and s.state_code is null
