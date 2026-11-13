{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trip_key',
        liquid_clustered_by=['pickup_date', 'pu_location_id']
    )
}}

select * from {{ ref('stg_trips') }}

{% if is_incremental() %}
-- By default, reload the last 31 days. The silver job replaces whole months, so a rerun of the
-- newest month changes rows that are already here, and the merge on trip_key updates them.
-- To merge an older month that silver rebuilt, pass --vars '{reload_from: "2024-03-01"}'.
-- The merge never deletes. If a rule change removes trips from silver, run a full refresh.
where pickup_date >= {% if var('reload_from', none) %}
    to_date('{{ var("reload_from") }}')
{% else %}
    (select date_sub(max(pickup_date), 31) from {{ this }})
{% endif %}
{% endif %}
