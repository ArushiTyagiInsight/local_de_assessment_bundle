{{
  config(
    materialized='table'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('bronze', 'sensors') }}
),

renamed AS (
  SELECT
    sensor_ts,
    store_id,
    shelf_id,
    temperature_c,
    humidity_pct,
    battery_mv
  FROM source
)

SELECT
  sensor_ts,
  store_id,
  shelf_id,
  temperature_c,
  humidity_pct,
  battery_mv,
  {{ current_timestamp() }} as dbt_created_ts
FROM renamed