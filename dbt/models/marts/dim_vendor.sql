select vendor_id, vendor_name
from {{ ref('vendors') }}
