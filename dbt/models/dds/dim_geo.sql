-- depends_on: {{ ref('err_cast') }}
with members as (select * from {{ ref('seed_br_state') }})
select {{ dds_sk("md5(upper(trim(state_code)))::char(32)", "'1970-01-01 00:00:00+00'::timestamptz") }} as geo_sk,
    md5(upper(trim(state_code)))::char(32) as geo_hk, '1970-01-01 00:00:00+00'::timestamptz as valid_from,
    'infinity'::timestamptz as valid_to, true as is_current, false as is_deleted,
    false as is_incomplete, false as is_unknown, state_code, state_name, region
from members
union all
select repeat('0',32)::char(32), repeat('0',32)::char(32), '-infinity'::timestamptz,
    'infinity'::timestamptz, true, false, false, true, '__unknown__', 'Unknown', '__unknown__'
