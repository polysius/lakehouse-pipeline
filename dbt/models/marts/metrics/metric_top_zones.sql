-- Metric 5: the 15 pickup zones with the most revenue.
select
    zones.zone,
    zones.borough,
    count(*) as trips,
    round(sum(trips.total_amount), 2) as revenue,
    round(avg(trips.total_amount), 2) as avg_total
from {{ ref('fct_trips') }} as trips
inner join {{ ref('dim_zone') }} as zones on trips.pu_location_id = zones.zone_id
group by zones.zone, zones.borough
order by revenue desc
limit 15
