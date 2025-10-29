{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH stg_stores AS (
    SELECT * FROM {{ ref('stg_stores') }}
)

SELECT
    store_id,
    store_code,
    name,
    channel,
    region,
    state,
    latitude,
    longitude,
    open_dt,
    close_dt,
    -- Add derived fields
    CASE 
        WHEN close_dt IS NOT NULL THEN 'Closed'
        ELSE 'Active'
    END as store_status,
    DATEDIFF('day', open_dt, COALESCE(close_dt, CURRENT_DATE)) as days_operating,
    -- Calculate store age in years
    DATEDIFF('year', open_dt, COALESCE(close_dt, CURRENT_DATE)) as store_age_years,
    quality_checks
FROM stg_stores