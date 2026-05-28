{{ config(materialized='view') }}

select
    meter_id,
    event_timestamp,
    kwh_reading,
    grid_zone,
    meter_type,
    ingest_timestamp

from {{ ref('stg_meter_readings') }}

where is_anomaly = true
