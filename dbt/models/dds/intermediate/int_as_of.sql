-- depends_on: {{ ref('err_cast') }}
{% if var('as_of_dts', none) is not none %}
select '{{ var("as_of_dts") | replace("'", "''") }}'::timestamptz as as_of_dts
{% else %}
select max(load_dts) as as_of_dts from (
select max(load_dts) as load_dts from {{ ref('eff_customer_profile') }}
union all
select max(load_dts) as load_dts from {{ ref('eff_order') }}
union all
select max(load_dts) as load_dts from {{ ref('eff_order_item') }}
union all
select max(load_dts) as load_dts from {{ ref('eff_order_profile') }}
union all
select max(load_dts) as load_dts from {{ ref('eff_product') }}
union all
select max(load_dts) as load_dts from {{ ref('eff_product_category') }}
union all
select max(load_dts) as load_dts from {{ ref('eff_review') }}
union all
select max(load_dts) as load_dts from {{ ref('eff_seller') }}
union all
select max(load_dts) as load_dts from {{ ref('hub_category') }}
union all
select max(load_dts) as load_dts from {{ ref('hub_customer') }}
union all
select max(load_dts) as load_dts from {{ ref('hub_customer_profile') }}
union all
select max(load_dts) as load_dts from {{ ref('hub_order') }}
union all
select max(load_dts) as load_dts from {{ ref('hub_product') }}
union all
select max(load_dts) as load_dts from {{ ref('hub_review') }}
union all
select max(load_dts) as load_dts from {{ ref('hub_seller') }}
union all
select max(load_dts) as load_dts from {{ ref('link_customer_profile') }}
union all
select max(load_dts) as load_dts from {{ ref('link_order_item') }}
union all
select max(load_dts) as load_dts from {{ ref('link_order_profile') }}
union all
select max(load_dts) as load_dts from {{ ref('link_order_review') }}
union all
select max(load_dts) as load_dts from {{ ref('link_product_category') }}
union all
select max(load_dts) as load_dts from {{ ref('rts_order') }}
union all
select max(load_dts) as load_dts from {{ ref('rts_order_item') }}
union all
select max(load_dts) as load_dts from {{ ref('rts_product') }}
union all
select max(load_dts) as load_dts from {{ ref('rts_review') }}
union all
select max(load_dts) as load_dts from {{ ref('rts_seller') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_category') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_customer_profile') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_order') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_order_item') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_order_payment') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_order_status') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_product') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_review') }}
union all
select max(load_dts) as load_dts from {{ ref('sat_seller') }}
) batches
{% endif %}
