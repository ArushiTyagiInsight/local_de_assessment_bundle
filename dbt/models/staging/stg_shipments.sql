{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/shipments/*.parquet')
),
typed as (
    SELECT
        cast(shipment_id as bigint) as shipment_id,
        cast(order_id as bigint) as order_id,
        trim(carrier) as carrier,
        cast(shipped_at as timestamp) as shipped_at,
        cast(delivered_at as timestamp) as delivered_at,
        cast(ship_cost as decimal(12,2)) as ship_cost
    FROM src
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN ship_cost < 0 THEN 'Negative shipping cost'
            ELSE NULL 
        END as cost_check,
        CASE 
            WHEN shipped_at > current_timestamp THEN 'Future ship date'
            WHEN delivered_at > current_timestamp THEN 'Future delivery date'
            WHEN delivered_at < shipped_at THEN 'Delivered before shipped'
            ELSE NULL 
        END as date_check
    FROM typed
)

SELECT 
    shipment_id,
    order_id,
    carrier,
    shipped_at,
    delivered_at,
    ship_cost,
    CASE 
        WHEN cost_check IS NOT NULL OR date_check IS NOT NULL
        THEN concat_ws(', ',
            NULLIF(cost_check, ''),
            NULLIF(date_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated