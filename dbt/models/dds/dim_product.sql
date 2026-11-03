-- depends_on: {{ ref('err_cast') }}
{{ dds_scd('product', {
    'product_id': 'null::text',
    'product_name_length': 'null::integer',
    'product_description_length': 'null::integer',
    'product_photos_qty': 'null::integer',
    'product_weight_g': 'null::integer',
    'product_length_cm': 'null::integer',
    'product_height_cm': 'null::integer',
    'product_width_cm': 'null::integer',
    'category_code': "'__unknown__'::text",
    'category_name_pt': 'null::text',
    'category_name_en': 'null::text',
    'category_display_name': "'__unknown__'::text",
    'is_translation_missing': 'false'
}) }}
