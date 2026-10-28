with counts as (
    select 'order' as entity, (select count(*) from {{ ref('fct_order') }}) as actual,
        (select count(*) from {{ ref('hub_order') }} h cross join {{ ref('int_as_of') }} a
         where h.load_dts <= a.as_of_dts and h.order_hk <> repeat('0',32)) as expected
    union all
    select 'item', (select count(*) from {{ ref('fct_order_item') }}),
        (select count(*) from (select distinct order_hk, order_item_id from {{ ref('link_order_item') }} l
         cross join {{ ref('int_as_of') }} a where l.load_dts <= a.as_of_dts
         and l.order_hk <> repeat('0',32) and l.order_item_hk <> repeat('0',32)) t)
    union all
    select 'payment', (select count(*) from {{ ref('fct_payment') }}),
        (select count(*) from (select distinct order_hk, payment_sequential from {{ ref('sat_order_payment') }} s
         cross join {{ ref('int_as_of') }} a where s.load_dts <= a.as_of_dts and s.order_hk <> repeat('0',32)) t)
    union all
    select 'review', (select count(*) from {{ ref('fct_review') }}),
        (select count(*) from {{ ref('link_order_review') }} l cross join {{ ref('int_as_of') }} a
         where l.load_dts <= a.as_of_dts and l.order_hk <> repeat('0',32) and l.review_hk <> repeat('0',32))
    union all
    select 'items_price', (select coalesce(sum(items_price),0) from {{ ref('fct_order') }}),
        (select coalesce(sum(price),0) from {{ ref('fct_order_item__active') }})
    union all
    select 'payment_value', (select coalesce(sum(payment_value),0) from {{ ref('fct_order') }}),
        (select coalesce(sum(payment_value),0) from {{ ref('fct_payment__active') }})
)
select * from counts where actual <> expected
