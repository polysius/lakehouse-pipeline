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
-- Reload the last 31 days. The silver job replaces whole months, so a rerun of the newest
-- month changes rows that are already here, and the merge on trip_key updates them in place.
where pickup_date >= (select date_sub(max(pickup_date), 31) from {{ this }})
{% endif %}
