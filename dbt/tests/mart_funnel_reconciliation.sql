{{ config(severity='error') }}
select 'orders' as check_name from {{ ref('order_status_funnel') }}
having coalesce(sum(orders_count), 0) <> (select count(*) from {{ ref('fct_order__active') }})
union all
select 'without_items' from {{ ref('order_status_funnel') }}
having coalesce(sum(orders_without_items_count), 0) <>
    (select count(*) from {{ ref('fct_order__active') }} where items_count = 0)
union all
select 'status_members' from {{ ref('order_status_funnel') }}
having count(*) <> (select count(*) from {{ ref('dim_order_status') }} where is_current)
