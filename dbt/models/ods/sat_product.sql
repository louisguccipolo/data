{{ config(materialized='incremental', schema='ods', incremental_strategy='append') }}
{{ sat('stg_products', 'product_hk', ['product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'], 'product_hashdiff') }}
