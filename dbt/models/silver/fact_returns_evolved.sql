{{ config(materialized='table') }}

with returns_evolved as (
    select * from {{ ref('stg_returns_evolved') }}
),

returns_enriched as (
    SELECT
        re.*,
        -- Add time-based dimensions
        date_trunc('day', return_ts) as return_date,
        extract('year' from return_ts) as return_year,
        extract('month' from return_ts) as return_month,
        -- Add return value based on order line details
        ol.line_total_usd * (re.qty::float / ol.quantity) as estimated_return_value_usd,
        -- Add reason categorization
        CASE
            WHEN return_reason_code = 'DEF' THEN 'Defective'
            WHEN return_reason_code = 'SIZE' THEN 'Wrong Size'
            WHEN return_reason_code = 'STYLE' THEN 'Style/Preference'
            WHEN return_reason_code = 'LATE' THEN 'Late Delivery'
            WHEN return_reason_code = 'OTHER' THEN 'Other'
            ELSE 'Unknown'
        END as reason_category,
        -- Add order context
        ol.order_ts_utc,
        ol.order_dt_local,
        ol.channel,
        ol.customer_id,
        ol.store_id
    FROM returns_evolved re
    LEFT JOIN {{ ref('fact_orders') }} ol 
        ON re.order_id = ol.order_id 
        AND re.product_id = ol.product_id
)

SELECT
    -- Identity columns
    return_id,
    order_id,
    product_id,
    customer_id,
    store_id,
    
    -- Order context
    order_ts_utc,
    order_dt_local,
    channel,
    
    -- Return time dimensions
    return_ts,
    return_date,
    return_year,
    return_month,
    
    -- Return details
    qty,
    reason,
    return_reason_code,
    reason_category,
    estimated_return_value_usd,
    
    -- Quality information
    quality_checks,
    
    -- Add metadata
    current_localtimestamp() as processed_ts
FROM returns_enriched