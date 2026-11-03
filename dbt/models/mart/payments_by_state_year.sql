-- depends_on: {{ ref('err_cast') }}
-- Все платежи, включая нулевую рассрочку и любой статус заказа.
select coalesce(nullif(upper(trim(o.customer_state)), ''), '__unknown__')::text as customer_state,
    extract(year from o.order_purchase_ts)::integer as purchase_year,
    count(*)::bigint as payments_count,
    coalesce(sum(p.payment_value), 0)::numeric(18,2) as payment_amount,
    coalesce(sum(p.payment_installments), 0)::bigint as installments_sum,
    round(avg(p.payment_installments::numeric), 2)::numeric(10,2) as avg_installments,
    count(*) filter (where p.payment_installments = 0)::bigint as zero_installments_count
from {{ ref('fct_payment__active') }} p
-- LEFT JOIN сохраняет платёж, даже если нет активного заказа на срезе.
left join {{ ref('fct_order__active') }} o on o.order_hk = p.order_hk
group by 1, 2
