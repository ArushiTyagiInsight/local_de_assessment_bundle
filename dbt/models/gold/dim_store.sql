{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH silver_stores AS (
    -- Reference silver layer model
    SELECT * FROM {{ ref('stores') }}
),

stores_enhanced AS (
    SELECT
        store_id,
        store_code,
        name as store_name,
        channel,
        region,
        state,
        latitude,
        longitude,
        open_dt as opening_date,
        close_dt as closing_date,
        -- Store status and metrics
        CASE 
            WHEN close_dt IS NOT NULL THEN 'Closed'
            ELSE 'Active'
        END as store_operational_status,
        DATEDIFF('day', open_dt, COALESCE(close_dt, CURRENT_DATE)) as store_age_days,
        DATEDIFF('year', open_dt, COALESCE(close_dt, CURRENT_DATE)) as store_age_years,
        -- Size categorization ## NO FIELD AVAILABLE TO CALCULATE
        --CASE
        --    WHEN floor_space_sqm < 1000 THEN 'Small'
        --    WHEN floor_space_sqm < 2500 THEN 'Medium'
        --    ELSE 'Large'
        --END as store_size_category,
        current_timestamp AS dbt_updated_at,
        'dim_store' AS dbt_model
    FROM silver_stores
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS store_sk,
    *
FROM stores_enhanced