{{ config(materialized='table') }}

select distinct
    to_date(date_trunc('day', event_timestamp))  as date_day,
    date_trunc('hour', event_timestamp)          as date_hour,
    extract(year      from event_timestamp)      as year,
    extract(month     from event_timestamp)      as month,
    extract(day       from event_timestamp)      as day,
    extract(hour      from event_timestamp)      as hour,
    extract(dayofweek from event_timestamp)      as day_of_week,
    case extract(dayofweek from event_timestamp)
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end as day_name

from {{ ref('stg_meter_readings') }}
