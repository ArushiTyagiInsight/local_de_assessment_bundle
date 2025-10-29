{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id,
    sku,
    name,
    category,
    subcategory,
    current_price,
    currency,
    introduced_dt,
    discontinued_dt,
    is_discontinued,
    -- Add derived fields
    CASE 
        WHEN is_discontinued THEN 'Discontinued'
        WHEN current_price = 0 THEN 'Out of Stock'
        ELSE 'Active'
    END as product_status,
    DATEDIFF('day', introduced_dt, COALESCE(discontinued_dt, CURRENT_DATE)) as days_in_catalog,
    quality_checks
FROM stg_products