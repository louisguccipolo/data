-- depends_on: {{ ref('err_cast') }}
{{ dds_scd('seller', {
    'seller_id': 'null::text',
    'seller_zip_code_prefix': 'null::text',
    'seller_city': 'null::text',
    'seller_state': 'null::text',
    'seller_region': 'null::text'
}) }}
