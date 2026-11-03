-- depends_on: {{ ref('err_cast') }}
with history as (
    select s.*, max(s.load_dts) over (partition by order_hk) as latest_set
    from {{ ref('sat_order_payment') }} s
    where s.load_dts <= (select as_of_dts from {{ ref('int_as_of') }}) and order_hk <> repeat('0',32)
)
select distinct on (order_hk, payment_sequential) *, load_dts < latest_set as is_deleted
from history order by order_hk, payment_sequential, load_dts desc
