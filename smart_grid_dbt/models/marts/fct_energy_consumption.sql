{{ config(
    materialized='incremental',
    unique_key='consumption_id'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['meter_id', 'consumption_hour']) }} as consumption_id,
    meter_id,
    consumption_hour,
    grid_zone,
    meter_type,
    total_kwh,
    avg_kwh,
    max_kwh,
    reading_count,
    anomaly_count

from {{ ref('int_consumption_windowed') }}

{% if is_incremental() %}
    where consumption_hour > (select max(consumption_hour) from {{ this }})
{% endif %}
