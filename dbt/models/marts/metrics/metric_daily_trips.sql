-- Metric 1: demand and revenue trend. One row per day.
select
    pickup_date,
    count(*) as trips,
    round(sum(total_amount), 2) as revenue,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance_miles
from {{ ref('fct_trips') }}
group by pickup_date
