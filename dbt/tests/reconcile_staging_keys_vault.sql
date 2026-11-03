select 'hub_order' as relation_name where exists ((select order_hk from {{ ref('stg_orders') }} union select order_hk from {{ ref('stg_order_items') }} union select order_hk from {{ ref('stg_order_payments') }} union select order_hk from {{ ref('stg_order_reviews') }}) except select order_hk from {{ ref('hub_order') }}) or exists (select order_hk from {{ ref('hub_order') }} except (select order_hk from {{ ref('stg_orders') }} union select order_hk from {{ ref('stg_order_items') }} union select order_hk from {{ ref('stg_order_payments') }} union select order_hk from {{ ref('stg_order_reviews') }}))
union all
select 'hub_customer' as relation_name where exists ((select customer_hk from {{ ref('stg_customers') }}) except select customer_hk from {{ ref('hub_customer') }}) or exists (select customer_hk from {{ ref('hub_customer') }} except (select customer_hk from {{ ref('stg_customers') }}))
union all
select 'hub_customer_profile' as relation_name where exists ((select customer_profile_hk from {{ ref('stg_customers') }} union select customer_profile_hk from {{ ref('stg_orders') }}) except select customer_profile_hk from {{ ref('hub_customer_profile') }}) or exists (select customer_profile_hk from {{ ref('hub_customer_profile') }} except (select customer_profile_hk from {{ ref('stg_customers') }} union select customer_profile_hk from {{ ref('stg_orders') }}))
union all
select 'hub_product' as relation_name where exists ((select product_hk from {{ ref('stg_products') }} union select product_hk from {{ ref('stg_order_items') }}) except select product_hk from {{ ref('hub_product') }}) or exists (select product_hk from {{ ref('hub_product') }} except (select product_hk from {{ ref('stg_products') }} union select product_hk from {{ ref('stg_order_items') }}))
union all
select 'hub_seller' as relation_name where exists ((select seller_hk from {{ ref('stg_sellers') }} union select seller_hk from {{ ref('stg_order_items') }}) except select seller_hk from {{ ref('hub_seller') }}) or exists (select seller_hk from {{ ref('hub_seller') }} except (select seller_hk from {{ ref('stg_sellers') }} union select seller_hk from {{ ref('stg_order_items') }}))
union all
select 'hub_category' as relation_name where exists ((select category_hk from {{ ref('stg_products') }} union select category_hk from {{ ref('stg_product_category_translation') }} union select repeat('0',32)::char(32)) except select category_hk from {{ ref('hub_category') }}) or exists (select category_hk from {{ ref('hub_category') }} except (select category_hk from {{ ref('stg_products') }} union select category_hk from {{ ref('stg_product_category_translation') }} union select repeat('0',32)::char(32)))
union all
select 'hub_review' as relation_name where exists ((select review_hk from {{ ref('stg_order_reviews') }}) except select review_hk from {{ ref('hub_review') }}) or exists (select review_hk from {{ ref('hub_review') }} except (select review_hk from {{ ref('stg_order_reviews') }}))
union all
select 'link_order_profile' as relation_name where exists (select order_profile_hk from {{ ref('stg_orders') }} except select order_profile_hk from {{ ref('link_order_profile') }}) or exists (select order_profile_hk from {{ ref('link_order_profile') }} except select order_profile_hk from {{ ref('stg_orders') }})
union all
select 'link_customer_profile' as relation_name where exists (select customer_profile_lhk from {{ ref('stg_customers') }} except select customer_profile_lhk from {{ ref('link_customer_profile') }}) or exists (select customer_profile_lhk from {{ ref('link_customer_profile') }} except select customer_profile_lhk from {{ ref('stg_customers') }})
union all
select 'link_order_item' as relation_name where exists (select order_item_hk from {{ ref('stg_order_items') }} except select order_item_hk from {{ ref('link_order_item') }}) or exists (select order_item_hk from {{ ref('link_order_item') }} except select order_item_hk from {{ ref('stg_order_items') }})
union all
select 'link_product_category' as relation_name where exists (select product_category_hk from {{ ref('stg_products') }} except select product_category_hk from {{ ref('link_product_category') }}) or exists (select product_category_hk from {{ ref('link_product_category') }} except select product_category_hk from {{ ref('stg_products') }})
union all
select 'link_order_review' as relation_name where exists (select order_review_hk from {{ ref('stg_order_reviews') }} except select order_review_hk from {{ ref('link_order_review') }}) or exists (select order_review_hk from {{ ref('link_order_review') }} except select order_review_hk from {{ ref('stg_order_reviews') }})
union all
select 'rts_order' as relation_name where exists (select order_hk,load_dts from {{ ref('stg_orders__keys') }} except select order_hk,load_dts from {{ ref('rts_order') }}) or exists (select order_hk,load_dts from {{ ref('rts_order') }} except select order_hk,load_dts from {{ ref('stg_orders__keys') }})
union all
select 'rts_order_item' as relation_name where exists (select order_item_hk,load_dts from {{ ref('stg_order_items__keys') }} except select order_item_hk,load_dts from {{ ref('rts_order_item') }}) or exists (select order_item_hk,load_dts from {{ ref('rts_order_item') }} except select order_item_hk,load_dts from {{ ref('stg_order_items__keys') }})
union all
select 'rts_product' as relation_name where exists (select product_hk,load_dts from {{ ref('stg_products__keys') }} except select product_hk,load_dts from {{ ref('rts_product') }}) or exists (select product_hk,load_dts from {{ ref('rts_product') }} except select product_hk,load_dts from {{ ref('stg_products__keys') }})
union all
select 'rts_seller' as relation_name where exists (select seller_hk,load_dts from {{ ref('stg_sellers__keys') }} except select seller_hk,load_dts from {{ ref('rts_seller') }}) or exists (select seller_hk,load_dts from {{ ref('rts_seller') }} except select seller_hk,load_dts from {{ ref('stg_sellers__keys') }})
union all
select 'rts_review' as relation_name where exists (select review_hk,load_dts from {{ ref('stg_order_reviews__keys') }} except select review_hk,load_dts from {{ ref('rts_review') }}) or exists (select review_hk,load_dts from {{ ref('rts_review') }} except select review_hk,load_dts from {{ ref('stg_order_reviews__keys') }})
