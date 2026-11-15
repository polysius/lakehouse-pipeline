-- Every metric that totals all trips must agree with the fact table. Returns a row on a mismatch.
with expected as (select count(*) as trips from {{ ref('fct_trips') }}),
actual as (
    select
        (select sum(trips) from {{ ref('metric_daily_trips') }}) as daily,
        (select sum(trips) from {{ ref('metric_borough_revenue') }}) as borough,
        (select sum(trips) from {{ ref('metric_hourly_demand') }}) as hourly
)
select * from expected, actual
where daily <> expected.trips
   or borough <> expected.trips
   or hourly <> expected.trips
