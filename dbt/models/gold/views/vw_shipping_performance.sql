{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH shipment_details AS (
    SELECT 
        sh.*,
        s.store_name,
        s.region as store_region,
        s.channel as store_type,
        s.channel as order_channel,  -- Since this is from the store dimension
        c.customer_segment
    FROM {{ ref('fct_shipments') }} sh
    LEFT JOIN {{ ref('dim_store') }} s ON sh.store_id = s.store_id
    LEFT JOIN {{ ref('dim_customer') }} c ON sh.customer_id = c.customer_id
)

SELECT
    -- Time Dimension
    shipment_ts_utc as shipment_date,
    shipment_dt_local as local_ship_date,
    delivery_days as days_to_deliver,
    
    -- Location & Channel
    store_name,
    store_region,
    store_type,
    delivery_channel,
    delivery_channel as shipping_method,
    
    -- Customer Segment
    customer_segment,
    
    -- Shipment Status
    CASE 
        WHEN on_time_flag = 'Y' THEN 'On Time'
        WHEN days_overdue > 0 THEN 'Late'
        ELSE 'In Progress'
    END as delivery_status,
    
    CASE 
        WHEN on_time_flag = 'N' THEN TRUE 
        ELSE FALSE 
    END as is_delayed,
    
    -- SLA Metrics
    sla_compliant as met_sla,
    days_overdue,
    expected_delivery_days as sla_target_days,
    
    -- Cost Metrics
    ROUND(shipping_cost, 2) as shipping_cost_usd,
    
    -- Performance Metrics
    delivery_days,
    
    -- Quality Metrics
    CASE 
        WHEN has_quality_issues = 'Y' THEN TRUE 
        ELSE FALSE 
    END as has_issues,
    all_quality_checks as quality_notes

FROM shipment_details