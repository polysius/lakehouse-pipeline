-- Metric 4: when demand peaks. One row per weekday and pickup hour.
select
    dates.day_of_week,
    dates.day_name,
    hour(trips.pickup_datetime) as pickup_hour,
    count(*) as trips
from {{ ref('fct_trips') }} as trips
inner join {{ ref('dim_date') }} as dates on trips.pickup_date = dates.date_day
group by 1, 2, 3
