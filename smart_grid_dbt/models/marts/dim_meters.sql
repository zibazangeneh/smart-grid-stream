{{ config(materialized='table') }}

select
    meter_id,
    any_value(grid_zone)  as grid_zone,
    any_value(meter_type) as meter_type,
    min(event_timestamp)  as first_seen_at,
    max(event_timestamp)  as last_seen_at,
    count(*)              as total_readings

from {{ ref('stg_meter_readings') }}

group by 1
