{#
  Переопределение postgres__create_table_as под Cloudberry/Greenplum:
  добавляет WITH (storage) и DISTRIBUTED BY / RANDOMLY / REPLICATED.
  По умолчанию Cloudberry сам выбирает ключ (обычно первая колонка) — для фактов задавайте явно.
#}
{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set storage = config.get('storage', none) -%}
  {%- set distributed_by = config.get('distributed_by', none) -%}
  {%- set contract_config = config.get('contract') -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%} temporary {%- endif %} table {{ relation }}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {% endif -%}
  {% if contract_config.enforced and (not temporary) -%}
    {{ get_table_columns_and_constraints() }}
    {{ cloudberry_storage_clause(storage, temporary) }}
    {{ cloudberry_distribution_clause(distributed_by) }};
    insert into {{ relation }} ({{ adapter.dispatch('get_column_names', 'dbt')() }})
    {%- set sql = get_select_subquery(sql) %}
    ( {{ sql }} );
  {% else %}
    {{ cloudberry_storage_clause(storage, temporary) }}
    as ( {{ sql }} )
    {{ cloudberry_distribution_clause(distributed_by) }};
  {% endif %}
{%- endmacro %}

{% macro cloudberry_storage_clause(storage, temporary) -%}
  {%- if storage and not temporary -%} with ({{ storage }}) {%- endif -%}
{%- endmacro %}

{% macro cloudberry_distribution_clause(distributed_by) -%}
  {%- if distributed_by == 'replicated' -%} distributed replicated
  {%- elif distributed_by == 'randomly' -%} distributed randomly
  {%- elif distributed_by -%} distributed by ({{ distributed_by }})
  {%- endif -%}
{%- endmacro %}
