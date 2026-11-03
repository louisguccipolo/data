-- depends_on: {{ ref('err_cast') }}
with members as (select * from {{ ref('seed_payment_type') }})
select {{ dds_sk("md5(upper(trim(payment_type)))::char(32)", "'1970-01-01 00:00:00+00'::timestamptz") }} as payment_type_sk,
    md5(upper(trim(payment_type)))::char(32) as payment_type_hk, '1970-01-01 00:00:00+00'::timestamptz as valid_from,
    'infinity'::timestamptz as valid_to, true as is_current, false as is_deleted,
    false as is_incomplete, false as is_unknown, payment_type, payment_type_name
from members
union all
select repeat('0',32)::char(32), repeat('0',32)::char(32), '-infinity'::timestamptz,
    'infinity'::timestamptz, true, false, false, true, '__unknown__', 'Unknown'
