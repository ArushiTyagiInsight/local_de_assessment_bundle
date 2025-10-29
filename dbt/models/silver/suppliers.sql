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
    CASE
        WHEN lead_time_days <= 7 THEN 'Fast'
        WHEN lead_time_days <= 14 THEN 'Medium'
        ELSE 'Slow'
    END as lead_time_category,
    quality_checks
FROM stg_suppliers