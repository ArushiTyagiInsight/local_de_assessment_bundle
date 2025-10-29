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
    quality_checks
FROM stg_stores