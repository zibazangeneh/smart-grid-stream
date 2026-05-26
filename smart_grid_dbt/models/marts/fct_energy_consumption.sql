-- fct_energy_consumption.sql
-- Fact table: energy consumption aggregated per meter per hour
-- Week 3 TODO: complete this model

{{ config(
    materialized='incremental',
    unique_key='consumption_id'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['meter_id', 'date_trunc("hour", event_timestamp)']) }} as consumption_id,
    meter_id,
    date_trunc('hour', event_timestamp)  as consumption_hour,
    grid_zone,
    meter_type,
    sum(kwh_reading)                     as total_kwh,
    avg(kwh_reading)                     as avg_kwh,
    max(kwh_reading)                     as max_kwh,
    count(*)                             as reading_count,
    sum(case when is_anomaly then 1 else 0 end) as anomaly_count

from {{ ref('stg_meter_readings') }}

{% if is_incremental() %}
    where event_timestamp > (select max(consumption_hour) from {{ this }})
{% endif %}

group by 1, 2, 3, 4, 5
