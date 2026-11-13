-- Returns the bad rows, so the test passes only when this query finds none.
-- The silver job rejects trips with a non-positive total or a negative fare.
-- Anything found here means that rule was bypassed or the fact table was loaded from elsewhere.
select trip_key, fare_amount, total_amount
from {{ ref('fct_trips') }}
where total_amount <= 0 or fare_amount < 0
