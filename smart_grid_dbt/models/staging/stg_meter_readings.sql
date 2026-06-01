{{ config(materialized='view') }}
-- Validated smart meter readings from Snowflake RAW layer

select
    meter_id,
    event_timestamp,
    kwh_reading,
    voltage,
    frequency,
    grid_zone,
    meter_type,
    is_anomaly,
    ingest_timestamp,
    processed_timestamp

from {{ source('raw', 'meter_readings') }}

where meter_id        is not null
  and kwh_reading     is not null
  and kwh_reading     >= 0
  and voltage         between 200 and 260
  and frequency       between 49 and 51
