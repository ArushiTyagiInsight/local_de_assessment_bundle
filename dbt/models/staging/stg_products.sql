{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/products/*.parquet')
),
typed as (
    SELECT
        cast(product_id as bigint) as product_id,
        trim(sku) as sku,
        trim(name) as name,
        trim(category) as category,
        trim(subcategory) as subcategory,
        cast(current_price as decimal(12,4)) as current_price,
        trim(currency) as currency,
        cast(introduced_dt as date) as introduced_dt,
        cast(discontinued_dt as date) as discontinued_dt,
        cast(is_discontinued as boolean) as is_discontinued
    FROM src
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN sku !~ '^SKU-[A-Z0-9]{6}$' THEN 'Invalid SKU format'
            ELSE NULL 
        END as sku_check,
        CASE 
            WHEN current_price <= 0 OR current_price IS NULL THEN 'Invalid or missing price'
            ELSE NULL 
        END as price_check,
        CASE 
            WHEN is_discontinued AND discontinued_dt IS NULL THEN 'Missing discontinuation date'
            WHEN NOT is_discontinued AND discontinued_dt IS NOT NULL THEN 'Discontinuation date present for active product'
            ELSE NULL 
        END as discontinued_check
    FROM typed
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
    CASE 
        WHEN sku_check IS NOT NULL OR price_check IS NOT NULL OR discontinued_check IS NOT NULL 
        THEN concat_ws(', ',
            NULLIF(sku_check, ''),
            NULLIF(price_check, ''),
            NULLIF(discontinued_check, '')
        )
        ELSE NULL
    END as quality_checks
from validated