{{
    config(
        materialized='incremental',
        unique_key=['store_id', 'sensor_ts'],
        on_schema_change='append_new_columns'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_sensors') }}
),

cleaned AS (
    SELECT 
        sensor_ts,
        store_id,
        shelf_id,
        -- Convert battery_mv from varchar to integer if it's not null
        CAST(NULLIF(battery_mv, '') AS INTEGER) as battery_mv,
        temperature_c,
        humidity_pct,
        dbt_created_ts
    FROM source
),

validated AS (
    SELECT 
        *,
        -- Add quality flags
        CASE 
            WHEN temperature_c < -30 OR temperature_c > 50 THEN FALSE
            WHEN humidity_pct < 0 OR humidity_pct > 100 THEN FALSE
            WHEN battery_mv < 2000 OR battery_mv > 3600 THEN FALSE
            ELSE TRUE 
        END as is_valid_reading
    FROM cleaned
)

SELECT 
    sensor_ts,
    store_id,
    shelf_id,
    temperature_c,
    humidity_pct,
    battery_mv,
    is_valid_reading,
    dbt_created_ts,
    CAST(current_localtimestamp() AS TIMESTAMPTZ) as silver_created_ts
FROM validated