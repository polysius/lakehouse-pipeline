with days as (
    select explode(sequence(
        to_date('{{ var("calendar_start") }}'),
        to_date('{{ var("calendar_end") }}'),
        interval 1 day
    )) as date_day
)

select
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    dayofweek(date_day) as day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    dayofweek(date_day) in (1, 7) as is_weekend
from days
