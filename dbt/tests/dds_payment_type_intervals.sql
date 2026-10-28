with ordered as (
    select *, lead(valid_from) over (partition by payment_type_hk order by valid_from) as next_start,
        count(*) filter (where is_current) over (partition by payment_type_hk) as current_count
    from {{ ref('dim_payment_type') }}
)
select payment_type_sk from ordered
where valid_from >= valid_to or current_count <> 1
    or (next_start is not null and valid_to <> next_start)
    or is_current <> (valid_to = 'infinity'::timestamptz)
    or (next_start is null and not is_current)
union all
select repeat('0',32) from {{ ref('dim_payment_type') }}
having count(*) filter (where is_unknown and payment_type_sk = repeat('0',32)
    and payment_type_hk = repeat('0',32) and valid_from = '-infinity'::timestamptz
    and valid_to = 'infinity'::timestamptz) <> 1
    or count(*) filter (where is_unknown) <> 1
    or count(*) filter (where not is_unknown and payment_type_hk = repeat('0',32)) <> 0
