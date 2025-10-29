{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH stg_suppliers AS (
    SELECT * FROM {{ ref('stg_supplier') }}
)

SELECT
    supplier_id,
    supplier_code,
    name,
    country_code,
    lead_time_days,
    preferred,
    -- Add derived fields
    CASE
        WHEN preferred THEN 'Preferred'
        ELSE 'Standard'
    END as supplier_tier,
    quality_checks
FROM stg_suppliers