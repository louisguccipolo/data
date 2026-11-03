{# Explicit refs make this a post-build catalog check, including empty models. #}
{% set models = [
    'stg_orders', 'stg_order_items', 'stg_order_payments', 'stg_order_reviews',
    'stg_products', 'stg_sellers', 'stg_customers', 'stg_product_category_translation',
    'hub_order', 'hub_customer', 'hub_customer_profile', 'hub_product', 'hub_seller', 'hub_category', 'hub_review',
    'link_order_profile', 'link_customer_profile', 'link_order_item', 'link_product_category', 'link_order_review',
    'sat_order', 'sat_order_status', 'sat_order_item', 'sat_order_payment', 'sat_customer_profile',
    'sat_product', 'sat_seller', 'sat_category', 'sat_review',
    'rts_order', 'rts_order_item', 'rts_product', 'rts_seller', 'rts_review',
    'eff_order', 'eff_order_item', 'eff_product', 'eff_seller', 'eff_review', 'eff_product_category'
] %}
select table_name, column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
from information_schema.columns
where (table_schema, table_name) in (
    {% for model in models %}
    {% set relation = ref(model) %}
    ('{{ relation.schema }}', '{{ relation.identifier }}'){% if not loop.last %},{% endif %}
    {% endfor %}
)
and (
    (column_name ~ '(_hk|_lhk|hashdiff)$'
        and (data_type <> 'character' or character_maximum_length <> 32))
    or (column_name in ('price', 'freight_value', 'payment_value')
        and (data_type <> 'numeric' or numeric_precision <> 18 or numeric_scale <> 2))
    or (column_name in ('order_item_id', 'payment_sequential', 'payment_installments', 'review_score')
        and data_type <> 'integer')
    or (column_name = 'load_dts' and data_type <> 'timestamp with time zone')
    or (column_name = 'is_deleted' and data_type <> 'boolean')
)
