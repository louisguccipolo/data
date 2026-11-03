{% macro dds_latest(model, keys) -%}
{# Скалярная отсечка не размножает оценку строк до появления статистики CTAS. #}
select distinct on ({{ keys }}) s.*
from {{ ref(model) }} s
where s.load_dts <= (select as_of_dts from {{ ref('int_as_of') }})
order by {{ keys }}, s.load_dts desc
{%- endmacro %}
