{{ config(materialized='table') }}

with returns_base as (
    select * from {{ ref('stg_returns_base') }}
),

enriched as (
    SELECT
        rb.*,
        -- Add time-based dimensions
        date_trunc('day', return_ts) as return_date,
        extract('year' from return_ts) as return_year,
        extract('month' from return_ts) as return_month,
        -- Calculate return value based on the original line item
        ol.line_total_usd * (rb.qty::float / ol.quantity) as estimated_return_value_usd
    FROM returns_base rb
    LEFT JOIN {{ ref('fact_orders') }} ol 
        ON rb.order_id = ol.order_id 
        AND rb.product_id = ol.product_id
) 

SELECT
    -- Identity columns
    return_id,
    order_id,
    product_id,
    
    -- Time dimensions
    return_ts,
    return_date,
    return_year,
    return_month,
    
    -- Return details
    qty,
    reason,
    estimated_return_value_usd,
    
    -- Add metadata
    current_localtimestamp() as processed_ts
FROM enriched