-- depends_on: {{ ref('err_cast') }}
select as_of_dts, '{{ run_started_at.isoformat() }}'::timestamptz as built_at,
    '{{ invocation_id }}'::text as dbt_invocation_id from {{ ref('int_as_of') }}
