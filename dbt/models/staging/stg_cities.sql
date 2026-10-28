select city_id, name as city_name, region as region_name, updated_at
from ({{ latest_version('cities', 'city_id') }}) s
