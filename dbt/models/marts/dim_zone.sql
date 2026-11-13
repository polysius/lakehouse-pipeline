select
    location_id as zone_id,
    borough,
    zone,
    service_zone
from {{ ref('stg_zones') }}
