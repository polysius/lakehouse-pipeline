-- Metric 2: revenue by pickup borough. One row per month and borough.
select
    date_trunc('month', trips.pickup_date) as month,
    zones.borough,
    count(*) as trips,
    round(sum(trips.total_amount), 2) as revenue
from {{ ref('fct_trips') }} as trips
inner join {{ ref('dim_zone') }} as zones on trips.pu_location_id = zones.zone_id
group by 1, 2
