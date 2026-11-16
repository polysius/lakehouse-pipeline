-- Metric 3: tip rate by payment type. Tip as a share of the metered fare.
-- Cash tips are not recorded in the data, so cash trips show a rate near zero.
select
    payment.payment_type_name,
    count(*) as trips,
    round(avg(trips.tip_amount / nullif(trips.fare_amount, 0)), 4) as avg_tip_rate
from {{ ref('fct_trips') }} as trips
inner join {{ ref('dim_payment_type') }} as payment on trips.payment_type_id = payment.payment_type_id
group by payment.payment_type_name
