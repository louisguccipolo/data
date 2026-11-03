with ordered as (
    select *, lead(valid_from) over (partition by geo_hk order by valid_from) as next_start,
        count(*) filter (where is_current) over (partition by geo_hk) as current_count
    from {{ ref('dim_geo') }}
)
select geo_sk from ordered
where valid_from >= valid_to or current_count <> 1
    or (next_start is not null and valid_to <> next_start)
    or is_current <> (valid_to = 'infinity'::timestamptz)
    or (next_start is null and not is_current)
union all
select repeat('0',32) from {{ ref('dim_geo') }}
having count(*) filter (where is_unknown and geo_sk = repeat('0',32)
    and geo_hk = repeat('0',32) and valid_from = '-infinity'::timestamptz
    and valid_to = 'infinity'::timestamptz) <> 1
    or count(*) filter (where is_unknown) <> 1
    or count(*) filter (where not is_unknown and geo_hk = repeat('0',32)) <> 0
