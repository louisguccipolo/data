{# A single-valued relationship is driven by one parent. Changes in observations
   deactivate its former link and activate its new link, preserving A→B→A.
   Parent deletion is independently tracked by eff_product, not invented here. #}
{% macro relationship_effectivity(source_model, driving_key, link_key, related_key) %}
with observations as (
    select distinct {{ driving_key }}, {{ link_key }}, {{ related_key }}, load_dts, record_source
    from {{ ref(source_model) }}
), ordered as (
    select *,
        lag({{ link_key }}) over (partition by {{ driving_key }} order by load_dts) as previous_link,
        lag({{ related_key }}) over (partition by {{ driving_key }} order by load_dts) as previous_related
    from observations
), changes as (
    select * from ordered where {{ link_key }} is distinct from previous_link
), events as (
    select {{ link_key }}, {{ driving_key }}, {{ related_key }}, false as is_deleted, load_dts, record_source
    from changes
    union all
    select previous_link as {{ link_key }}, {{ driving_key }}, previous_related as {{ related_key }},
        true as is_deleted, load_dts, record_source
    from changes where previous_link is not null
)
select * from events e
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} t where t.{{ link_key }} = e.{{ link_key }} and t.load_dts = e.load_dts
)
{% endif %}
{% endmacro %}
