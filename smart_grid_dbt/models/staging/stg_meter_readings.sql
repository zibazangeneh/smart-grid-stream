-- stg_meter_readings.sql
-- Staging model: clean and rename raw meter readings from Snowflake
-- Week 3 TODO: complete this model

{{ config(materialized='view') }}

select
    meter_id,
    cast(timestamp as timestamp)  as event_timestamp,
    kwh_reading,
    voltage,
    frequency,
    grid_zone,
    meter_type,
    is_anomaly,
    ingest_timestamp

from {{ source('raw', 'meter_readings') }}

where kwh_reading >= 0
  and voltage between 200 and 260
  and frequency between 49 and 51
