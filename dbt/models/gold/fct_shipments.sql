{{ config(materialized='table') }}

with fact_orders as (
    select * from {{ ref('fact_orders') }}
),
-- Get distinct orders since shipping is at order level
distinct_orders as (
    SELECT DISTINCT
        order_id,
        customer_id,
        store_id,
        order_ts_utc,
        order_dt_local,
        channel,
        shipping_fee_usd,
        has_quality_issues,
        all_quality_checks
    FROM fact_orders
),
-- Calculate SLA metrics
sla_calcs as (
    SELECT
        order_id,
        customer_id,
        store_id,
        order_ts_utc,
        order_dt_local,
        channel,
        shipping_fee_usd as shipping_cost,
        -- Calculate delivery days based on channel
        CASE 
            WHEN channel = 'store' THEN 0  -- Immediate delivery for in-store
            WHEN channel = 'web' THEN 3    -- Standard 3-day delivery for web
            WHEN channel = 'mobile' THEN 2  -- Express 2-day delivery for mobile
            ELSE 3                         -- Default to standard delivery
        END as expected_delivery_days,
        -- Actual delivery days would come from a shipping system
        -- For now, using a random number between 0-5 for demonstration
        abs(floor(random() * 6)) as actual_delivery_days,
        has_quality_issues,
        all_quality_checks
    FROM distinct_orders
),
-- Add SLA flags and measures
final as (
    SELECT
        -- Keys
        order_id as shipment_id,  -- Using order_id as shipment_id for this model
        customer_id,
        store_id,
        
        -- Timestamps
        order_ts_utc as shipment_ts_utc,
        order_dt_local as shipment_dt_local,
        
        -- Extract date parts for analysis
        extract(year from order_dt_local) as shipment_year,
        extract(month from order_dt_local) as shipment_month,
        extract(dow from order_dt_local) as shipment_day_of_week,
        
        -- Delivery channel
        channel as delivery_channel,
        
        -- Measures
        shipping_cost,
        actual_delivery_days as delivery_days,
        expected_delivery_days,
        
        -- SLA flags
        CASE 
            WHEN actual_delivery_days <= expected_delivery_days THEN 'Y'
            ELSE 'N'
        END as on_time_flag,
        
        -- SLA compliance calculations
        CASE
            WHEN actual_delivery_days <= expected_delivery_days THEN 1
            ELSE 0
        END as sla_compliant,
        
        CASE
            WHEN actual_delivery_days > expected_delivery_days 
            THEN actual_delivery_days - expected_delivery_days
            ELSE 0
        END as days_overdue,
        
        -- Quality tracking
        has_quality_issues,
        all_quality_checks,
        
        -- Metadata
        current_localtimestamp() as processed_ts
    FROM sla_calcs
)

SELECT * FROM final