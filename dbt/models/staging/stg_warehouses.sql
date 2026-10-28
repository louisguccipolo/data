select warehouse_id, name as warehouse_name, city_id, updated_at
from ({{ latest_version('warehouses', 'warehouse_id') }}) s
