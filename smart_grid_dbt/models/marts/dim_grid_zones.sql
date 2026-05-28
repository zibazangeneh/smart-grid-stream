{{ config(materialized='table') }}

select
    grid_zone,
    case grid_zone
        when 'NRW-North' then 'North Rhine-Westphalia North'
        when 'NRW-South' then 'North Rhine-Westphalia South'
        when 'Bavaria'   then 'Bavaria'
        when 'Saxony'    then 'Saxony'
        when 'Hamburg'   then 'Hamburg'
    end as grid_zone_name,
    case grid_zone
        when 'NRW-North' then 'NRW'
        when 'NRW-South' then 'NRW'
        when 'Bavaria'   then 'Bavaria'
        when 'Saxony'    then 'Saxony'
        when 'Hamburg'   then 'Hamburg'
    end as federal_state,
    count(distinct meter_id) as meter_count

from {{ ref('stg_meter_readings') }}

group by 1, 2, 3
