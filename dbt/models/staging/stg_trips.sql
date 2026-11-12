-- Silver trips with a trip key and the columns the fact table needs.
-- Names for zones, payment types and so on belong to the dimensions, so they are left out here.
select
    {{ trip_key() }} as trip_key,
    pickup_date,
    pickup_datetime,
    dropoff_datetime,
    vendor_id,
    pu_location_id,
    do_location_id,
    payment_type as payment_type_id,
    -- About 10% of rows have no rate code. Map them to the dictionary's "Unknown" code.
    coalesce(rate_code_id, 99) as rate_code_id,
    passenger_count,
    trip_distance,
    (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60 as trip_minutes,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    source_month
from {{ source('silver', 'silver_trips') }}
