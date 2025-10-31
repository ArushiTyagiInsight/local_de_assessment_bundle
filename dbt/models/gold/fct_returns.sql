{{ config(materialized='table') }}

with fact_returns as (
    select * from {{ ref('fact_returns_evolved') }}
),

calculations as (
    SELECT
        -- Identity columns
        return_id,
        order_id as order_number,
        product_id,
        customer_id,
        store_id,
        
        -- Timestamps and dates
        return_ts,
        return_date,
        return_year,
        return_month,
        
        -- Return details
        qty as return_quantity,
        reason,
        return_reason_code,
        reason_category,
        
        -- Original order context
        order_ts_utc,
        order_dt_local,
        channel as order_channel,
        
        -- Financial impact
        estimated_return_value_usd as return_value,
        quality_checks,
        
        -- Add some calculated metrics
        datediff('day', order_dt_local, return_date) as days_to_return,
        
        -- Add data quality indicators
        CASE 
            WHEN quality_checks IS NOT NULL THEN True
            ELSE False
        END as has_data_issues
    FROM fact_returns
)

SELECT 
    -- Identity columns
    return_id,
    order_number,
    product_id,
    customer_id,
    store_id,
    
    -- Timestamps
    return_ts,
    return_date,
    return_year,
    return_month,
    order_ts_utc,
    order_dt_local,
    
    -- Return details
    return_quantity,
    reason,
    return_reason_code,
    reason_category,
    order_channel,
    
    -- Calculated metrics
    round(return_value, 2) as return_value,
    days_to_return,
    
    -- Status and classification
    CASE 
        WHEN days_to_return <= 7 THEN 'Within 7 days'
        WHEN days_to_return <= 14 THEN 'Within 14 days'
        WHEN days_to_return <= 30 THEN 'Within 30 days'
        ELSE 'Over 30 days'
    END as return_timeframe,
    
    -- Quality flags
    has_data_issues,
    quality_checks as quality_details

FROM calculations