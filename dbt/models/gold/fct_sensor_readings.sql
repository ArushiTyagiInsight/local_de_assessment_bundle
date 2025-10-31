{{
    config(
        materialized='table',
        on_schema_change='append_new_columns'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('fact_sensors') }}
    WHERE is_valid_reading = TRUE
),

hourly_stats AS (
    SELECT
        DATE_TRUNC('hour', sensor_ts) as reading_hour,
        store_id,
        shelf_id,
        -- Basic aggregations
        COUNT(*) as readings_count,
        CAST(AVG(temperature_c) AS DECIMAL(5,2)) as avg_temperature_c,
        CAST(AVG(humidity_pct) AS DECIMAL(5,2)) as avg_humidity_pct,
        CAST(AVG(battery_mv) AS DECIMAL(10,2)) as avg_battery_mv,
        CAST(MIN(temperature_c) AS DECIMAL(5,2)) as min_temperature_c,
        CAST(MAX(temperature_c) AS DECIMAL(5,2)) as max_temperature_c,
        CAST(MIN(humidity_pct) AS DECIMAL(5,2)) as min_humidity_pct,
        CAST(MAX(humidity_pct) AS DECIMAL(5,2)) as max_humidity_pct,
        -- Standard deviation for anomaly detection
        STDDEV(temperature_c) as stddev_temperature_c,
        STDDEV(humidity_pct) as stddev_humidity_pct
    FROM source
    GROUP BY 1, 2, 3
),

with_rolling_stats AS (
    SELECT 
        *,
        -- 24-hour rolling averages
        CAST(AVG(avg_temperature_c) OVER (
            PARTITION BY store_id, shelf_id 
            ORDER BY reading_hour 
            RANGE BETWEEN INTERVAL 24 HOURS PRECEDING AND CURRENT ROW
        ) AS DECIMAL(5,2)) as rolling_24h_avg_temp,
        CAST(AVG(avg_humidity_pct) OVER (
            PARTITION BY store_id, shelf_id 
            ORDER BY reading_hour 
            RANGE BETWEEN INTERVAL 24 HOURS PRECEDING AND CURRENT ROW
        ) AS DECIMAL(5,2)) as rolling_24h_avg_humidity,
        -- Rolling standard deviations
        AVG(stddev_temperature_c) OVER (
            PARTITION BY store_id, shelf_id 
            ORDER BY reading_hour 
            RANGE BETWEEN INTERVAL 24 HOURS PRECEDING AND CURRENT ROW
        ) as rolling_24h_stddev_temp,
        AVG(stddev_humidity_pct) OVER (
            PARTITION BY store_id, shelf_id 
            ORDER BY reading_hour 
            RANGE BETWEEN INTERVAL 24 HOURS PRECEDING AND CURRENT ROW
        ) as rolling_24h_stddev_humidity
    FROM hourly_stats
),

final AS (
    SELECT 
        reading_hour,
        store_id,
        shelf_id,
        readings_count,
        avg_temperature_c,
        avg_humidity_pct,
        avg_battery_mv,
        min_temperature_c,
        max_temperature_c,
        min_humidity_pct,
        max_humidity_pct,
        rolling_24h_avg_temp,
        rolling_24h_avg_humidity,
        -- Anomaly flags using 3-sigma rule
        CASE 
            WHEN ABS(avg_temperature_c - rolling_24h_avg_temp) > (3 * rolling_24h_stddev_temp) THEN TRUE
            ELSE FALSE 
        END as is_temperature_anomaly,
        CASE 
            WHEN ABS(avg_humidity_pct - rolling_24h_avg_humidity) > (3 * rolling_24h_stddev_humidity) THEN TRUE
            ELSE FALSE 
        END as is_humidity_anomaly,
        -- Business rules for environmental conditions
        CASE 
            WHEN avg_temperature_c > 25 THEN 'High'
            WHEN avg_temperature_c < 15 THEN 'Low'
            ELSE 'Normal'
        END as temperature_status,
        CASE 
            WHEN avg_humidity_pct > 70 THEN 'High'
            WHEN avg_humidity_pct < 30 THEN 'Low'
            ELSE 'Normal'
        END as humidity_status,
        CAST(current_localtimestamp() AS TIMESTAMPTZ) as gold_created_ts
    FROM with_rolling_stats
)

SELECT * FROM final