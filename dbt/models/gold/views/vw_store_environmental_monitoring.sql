{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH sensor_data AS (
    SELECT 
        sr.reading_hour,
        sr.store_id,
        sr.avg_temperature_c,
        sr.avg_humidity_pct,
        sr.min_temperature_c,
        sr.max_temperature_c,
        sr.min_humidity_pct,
        sr.max_humidity_pct,
        sr.avg_battery_mv,
        sr.readings_count,
        sr.rolling_24h_avg_temp,
        sr.rolling_24h_avg_humidity,
        sr.is_temperature_anomaly,
        sr.is_humidity_anomaly,
        sr.temperature_status,
        sr.humidity_status,
        s.store_name,
        s.region as store_region,
        s.channel as store_type,
        s.store_operational_status as store_status
    FROM {{ ref('fct_sensor_readings') }} sr
    LEFT JOIN {{ ref('dim_store') }} s ON sr.store_id = s.store_id
)

SELECT
    -- Time Dimension
    reading_hour as measurement_time,
    DATE_TRUNC('day', reading_hour) as measurement_date,
    EXTRACT(HOUR FROM reading_hour) as hour_of_day,
    
    -- Store Details
    store_name,
    store_region,
    store_type,
    
    -- Environmental Readings
    readings_count as number_of_readings,
    ROUND(avg_temperature_c, 1) as average_temperature,
    ROUND(avg_humidity_pct, 1) as average_humidity,
    ROUND(min_temperature_c, 1) as minimum_temperature,
    ROUND(max_temperature_c, 1) as maximum_temperature,
    ROUND(min_humidity_pct, 1) as minimum_humidity,
    ROUND(max_humidity_pct, 1) as maximum_humidity,
    
    -- Status Indicators
    temperature_status as temperature_condition,
    humidity_status as humidity_condition,
    CASE 
        WHEN is_temperature_anomaly AND is_humidity_anomaly THEN 'Critical'
        WHEN is_temperature_anomaly OR is_humidity_anomaly THEN 'Warning'
        ELSE 'Normal'
    END as environmental_status,
    
    -- Trend Analysis
    ROUND(rolling_24h_avg_temp, 1) as temperature_24h_average,
    ROUND(rolling_24h_avg_humidity, 1) as humidity_24h_average,
    ROUND(avg_battery_mv/1000.0, 2) as battery_voltage -- Convert to volts for readability

FROM sensor_data
WHERE store_status = 'Active' -- Only show active stores by default