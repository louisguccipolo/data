-- depends_on: {{ ref('err_cast') }}
with sat as ({{ dds_latest('sat_review', 'review_hk') }}),
eff as ({{ dds_latest('eff_review', 'review_hk') }})
select r.review_id, h.order_id, l.review_hk, l.order_hk, s.review_score,
    nullif(trim(s.review_comment_title), '') is not null as has_comment_title,
    nullif(trim(s.review_comment_message), '') is not null as has_comment_message,
    s.review_creation_date as review_creation_ts, s.review_answer_timestamp as review_answer_ts,
    case when s.review_answer_timestamp >= s.review_creation_date
        then extract(epoch from (s.review_answer_timestamp - s.review_creation_date)) / 3600 end as answer_hours,
    coalesce(d.date_key, -1) as review_creation_date_key,
    coalesce(od.date_key, -1) as order_purchase_date_key,
    coalesce(e.is_deleted, false) as is_deleted, s.review_hk is null as is_incomplete
from {{ ref('link_order_review') }} l
join {{ ref('hub_review') }} r using (review_hk) join {{ ref('hub_order') }} h using (order_hk)
left join sat s using (review_hk) left join eff e using (review_hk)
left join {{ ref('dim_date') }} d on d.date = s.review_creation_date::date
left join {{ ref('int_order_on_slice') }} o using (order_hk)
left join {{ ref('dim_date') }} od on od.date = o.order_purchase_ts::date
where l.load_dts <= (select as_of_dts from {{ ref('int_as_of') }}) and l.review_hk <> repeat('0',32) and l.order_hk <> repeat('0',32)
