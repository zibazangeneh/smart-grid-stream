{{ config(materialized='view') }}

select
    meter_id,
    grid_zone,
    meter_type,
    date_trunc('hour', event_timestamp)         as consumption_hour,
    sum(kwh_reading)                            as total_kwh,
    avg(kwh_reading)                            as avg_kwh,
    max(kwh_reading)                            as max_kwh,
    count(*)                                    as reading_count,
    sum(case when is_anomaly then 1 else 0 end) as anomaly_count

from {{ ref('stg_meter_readings') }}

group by 1, 2, 3, 4
