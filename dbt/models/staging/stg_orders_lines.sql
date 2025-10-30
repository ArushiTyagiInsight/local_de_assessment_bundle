{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/orders_lines/*.parquet')
),
typed as (
    SELECT
        cast(order_id as bigint) as order_id,
        cast(line_number as bigint) as line_number,
        cast(product_id as bigint) as product_id,
        cast(qty as integer) as quantity,
        cast(unit_price as decimal(10,2)) as unit_price_usd,
        cast(line_discount_pct as decimal(10,2)) as line_discount_pct,
        cast(tax_pct as decimal(10,2)) as tax_pct,
        -- Calculate derived fields
        cast(
            unit_price * cast(qty as decimal(10,2)) * (cast(line_discount_pct as decimal(10,2)) / 100.0)
            as decimal(10,2)
        ) as discount_amount_usd,
        cast(
            unit_price * cast(qty as decimal(10,2)) * (1 - cast(line_discount_pct as decimal(10,2)) / 100.0) * (1 + cast(tax_pct as decimal(10,2)) / 100.0)
            as decimal(10,2)
        ) as line_total_usd
    FROM src
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN quantity <= 0 THEN 'Invalid quantity'
            ELSE NULL 
        END as quantity_check,
        CASE 
            WHEN unit_price_usd < 0 THEN 'Negative unit price'
            ELSE NULL 
        END as price_check,
        CASE 
            WHEN line_discount_pct < 0 OR line_discount_pct > 100 
            THEN 'Discount percentage must be between 0 and 100'
            ELSE NULL
        END as discount_pct_check,
        CASE 
            WHEN tax_pct < 0 OR tax_pct > 100 
            THEN 'Tax percentage must be between 0 and 100'
            ELSE NULL
        END as tax_pct_check,
        CASE 
            WHEN discount_amount_usd < 0 
            THEN 'Negative discount amount'
            ELSE NULL 
        END as discount_check,
        CASE 
            WHEN line_total_usd < 0 
            THEN 'Negative line total'
            ELSE NULL 
        END as total_check
    FROM typed
)

SELECT 
    order_id,
    line_number,
    product_id,
    quantity,
    unit_price_usd,
    line_discount_pct,
    tax_pct,
    discount_amount_usd,
    line_total_usd,
    CASE 
        WHEN quantity_check IS NOT NULL 
        OR price_check IS NOT NULL 
        OR discount_pct_check IS NOT NULL 
        OR tax_pct_check IS NOT NULL
        OR discount_check IS NOT NULL
        OR total_check IS NOT NULL
        THEN concat_ws(', ',
            NULLIF(quantity_check, ''),
            NULLIF(price_check, ''),
            NULLIF(discount_pct_check, ''),
            NULLIF(tax_pct_check, ''),
            NULLIF(discount_check, ''),
            NULLIF(total_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated