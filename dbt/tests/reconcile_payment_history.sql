{# Raw contains snapshots, vault contains complete sets only at set transitions. #}
with rows as (
    select distinct order_hk, load_dts, payment_sequential, payment_row_hashdiff
    from {{ ref('stg_order_payments') }}
), sets as (
    select order_hk, load_dts, count(*) as members,
        md5(string_agg(payment_row_hashdiff::text, '||' order by payment_sequential nulls first, payment_row_hashdiff)) as diff
    from rows group by 1,2
), ordered as (
    select *, lag(diff) over (partition by order_hk order by load_dts) as previous from sets
)
select 'payment_set_member_count' as check_name
where (select coalesce(sum(members),0) from ordered where diff is distinct from previous)
   <> (select count(*) from {{ ref('sat_order_payment') }})
