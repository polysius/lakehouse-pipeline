select trip_key, pickup_datetime, dropoff_datetime
from {{ ref('fct_trips') }}
where dropoff_datetime <= pickup_datetime
