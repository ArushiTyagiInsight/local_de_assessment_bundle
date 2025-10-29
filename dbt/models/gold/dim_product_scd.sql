{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH product_snapshot AS (
    -- Reference product snapshot table (SCD Type 2)
    SELECT * FROM {{ ref('products_snapshot') }}
),

current_products AS (
    -- Get the current version of each product
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY dbt_valid_from DESC) as rn
    FROM product_snapshot
    WHERE dbt_valid_to IS NULL
),

product_history AS (
    -- Calculate price changes and other historical metrics
    SELECT 
        p.*,
        LAG(current_price) OVER (PARTITION BY product_id ORDER BY dbt_valid_from) as prev_price,
        LAG(dbt_valid_from) OVER (PARTITION BY product_id ORDER BY dbt_valid_from) as prev_valid_from,
        LEAD(dbt_valid_from) OVER (PARTITION BY product_id ORDER BY dbt_valid_from) as next_valid_from
    FROM product_snapshot p
),

products_enhanced AS (
    SELECT
        ph.product_id,
        ph.sku,
        ph.name as product_name,
        ph.category,
        ph.subcategory,
        ph.current_price as unit_price,
        ph.dbt_valid_from as effective_from,
        COALESCE(ph.dbt_valid_to, '9999-12-31'::timestamp) as effective_to,
        -- SCD Type 2 flags and indicators
        CASE 
            WHEN ph.dbt_valid_to IS NULL THEN True 
            ELSE False 
        END as is_current,
        -- Price change tracking
        CASE 
            WHEN ph.current_price != COALESCE(ph.prev_price, ph.current_price) THEN True 
            ELSE False 
        END as price_change_flag,
        CASE
            WHEN ph.prev_price IS NOT NULL THEN 
                ROUND(((ph.current_price - ph.prev_price) / ph.prev_price) * 100, 2)
            ELSE 0
        END as price_change_pct,
        -- Version tracking
        DATEDIFF('day', ph.introduced_dt, CURRENT_DATE) as days_in_catalog,
        DATEDIFF('day', 
            COALESCE(ph.prev_valid_from, ph.introduced_dt), 
            COALESCE(ph.next_valid_from, CURRENT_DATE)
        ) as version_days,
        -- Status indicators
        CASE
            WHEN cp.rn = 1 AND cp.is_discontinued = False THEN 'Active'
            WHEN cp.is_discontinued = True THEN 'Discontinued'
            ELSE 'Out of Stock'
        END as product_status,
        ph.currency,
        ph.is_discontinued,
        ph.introduced_dt,
        ph.discontinued_dt,
        current_timestamp AS dbt_updated_at,
        'dim_product_scd' AS dbt_model
    FROM product_history ph
    LEFT JOIN current_products cp ON ph.product_id = cp.product_id
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['product_id', 'effective_from']) }} AS product_sk,
    *
FROM products_enhanced