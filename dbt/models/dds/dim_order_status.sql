-- depends_on: {{ ref('err_cast') }}
with members as (select * from {{ ref('seed_order_status') }})
select {{ dds_sk("md5(upper(trim(order_status)))::char(32)", "'1970-01-01 00:00:00+00'::timestamptz") }} as order_status_sk,
    md5(upper(trim(order_status)))::char(32) as order_status_hk, '1970-01-01 00:00:00+00'::timestamptz as valid_from,
    'infinity'::timestamptz as valid_to, true as is_current, false as is_deleted,
    false as is_incomplete, false as is_unknown, order_status, status_group, is_revenue
from members
union all
select repeat('0',32)::char(32), repeat('0',32)::char(32), '-infinity'::timestamptz,
    'infinity'::timestamptz, true, false, false, true, '__unknown__', '__unknown__', false
