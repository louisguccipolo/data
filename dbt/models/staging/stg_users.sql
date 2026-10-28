-- Содержит PII. Дальше разрезается на core.dim_customer (публичное) и pii.customer_pii.
select
    user_id,
    lower(trim(email))                       as email,
    {{ pii_hash('lower(trim(email))') }}     as email_hash,
    phone,
    full_name,
    birth_date,
    extract(year from birth_date)::int       as birth_year,
    gender,
    city_id,
    status,
    created_at                               as registered_at,
    updated_at
from ({{ latest_version('users', 'user_id') }}) s
