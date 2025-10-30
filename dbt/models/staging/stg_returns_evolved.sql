{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/returns_evolved/*.parquet')
),
typed as (
    SELECT
        cast(return_id as bigint) as return_id,
        cast(order_id as bigint) as order_id,
        cast(product_id as bigint) as product_id,
        cast(return_ts as timestamp) as return_ts,
        cast(qty as integer) as qty,
        trim(reason) as reason,
        trim(return_reason_code) as return_reason_code
    FROM src
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN qty <= 0 THEN 'Invalid quantity'
            ELSE NULL 
        END as qty_check,
        CASE 
            WHEN return_ts > current_timestamp THEN 'Future return timestamp'
            ELSE NULL 
        END as timestamp_check,
        CASE 
            WHEN reason IS NULL AND return_reason_code IS NULL THEN 'Missing return reason'
            ELSE NULL 
        END as reason_check
    FROM typed
)

SELECT 
    return_id,
    order_id,
    product_id,
    return_ts,
    qty,
    reason,
    return_reason_code,
    CASE 
        WHEN qty_check IS NOT NULL OR timestamp_check IS NOT NULL OR reason_check IS NOT NULL
        THEN concat_ws(', ',
            NULLIF(qty_check, ''),
            NULLIF(timestamp_check, ''),
            NULLIF(reason_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated