{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        on_schema_change='merge'
    )
}}

WITH source_data AS (
    SELECT 
        cast(shipment_id as bigint) as shipment_id,
        cast(order_id as bigint) as order_id,
        trim(carrier) as carrier,
        cast(shipped_at as timestamp) as shipped_at,
        cast(delivered_at as timestamp) as delivered_at,
        cast(ship_cost as decimal(12,2)) as ship_cost,
        cast(ingestion_ts as timestamp) as ingestion_ts,
        CASE 
            WHEN delivered_at < shipped_at THEN 'Delivery before shipment'
            ELSE NULL 
        END as delivery_check
    FROM {{ source('bronze', 'shipments') }}
)
SELECT 
    s.shipment_id,
    s.order_id,
    s.carrier,
    s.shipped_at,
    s.delivered_at,
    s.ship_cost,
    s.ingestion_ts,
    -- Calculate delivery duration in days
    CASE 
        WHEN s.delivered_at IS NOT NULL 
        THEN date_diff('day', s.shipped_at, s.delivered_at)
    END as delivery_duration_days,
    -- Flag late deliveries (assuming SLA of 7 days)
    CASE 
        WHEN s.delivered_at IS NOT NULL AND 
             date_diff('day', s.shipped_at, s.delivered_at) > 7 
        THEN TRUE 
        ELSE FALSE 
    END as is_late_delivery,
    -- Status based on timestamps
    CASE 
        WHEN s.delivered_at IS NOT NULL THEN 'DELIVERED'
        WHEN s.shipped_at IS NOT NULL THEN 'IN_TRANSIT'
        ELSE 'UNKNOWN'
    END as shipment_status,
    s.delivery_check as data_quality_check
FROM source_data s

{% if is_incremental() %}
WHERE ingestion_ts > (SELECT max(ingestion_ts) FROM {{ this }})
{% endif %}