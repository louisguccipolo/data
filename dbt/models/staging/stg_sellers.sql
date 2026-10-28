-- Содержит чувствительные данные продавца (ИНН, счёт, контакты).
select
    seller_id,
    shop_name,
    legal_name,
    inn,
    bank_account,
    contact_email,
    contact_phone,
    city_id,
    status,
    created_at as registered_at,
    updated_at
from ({{ latest_version('sellers', 'seller_id') }}) s
