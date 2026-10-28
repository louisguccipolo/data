-- depends_on: {{ ref('err_cast') }}
select p.payment_type::text as payment_type, count(*)::bigint as payments_count,
    coalesce(sum(p.payment_value), 0)::numeric(18,2) as payment_amount,
    coalesce(sum(p.payment_installments), 0)::bigint as installments_sum,
    avg(p.payment_installments::numeric)::numeric as avg_installments,
    count(*) filter (where p.payment_installments = 0)::bigint as zero_installments_count
from {{ ref('fct_payment__active') }} p
group by p.payment_type
