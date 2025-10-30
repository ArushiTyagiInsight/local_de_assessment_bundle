{{ config(materialized='table') }}

with fact_orders as (
    select * from {{ ref('fact_orders') }}
),
calculations as (
    SELECT
        -- Foreign keys to dimensions
        order_id,
        line_number,
        customer_id,
        product_id,
        store_id,

        -- Date/time attributes
        order_ts_utc,
        order_dt_local,
        
        -- Channel and payment attributes
        channel,
        payment_method,
        
        -- Order attributes
        coupon_code,
        currency_code,

        -- Quantity measure
        quantity,
        
        -- Price measures
        unit_price_usd,
        line_discount_pct,
        tax_pct,
        
        -- Calculated measures
        unit_price_usd * quantity as gross_amount,
        discount_amount_usd as discount_amount,
        line_total_usd - (unit_price_usd * quantity * (1 - line_discount_pct / 100.0)) as tax_amount,
        unit_price_usd * quantity * (1 - line_discount_pct / 100.0) as net_amount,
        line_total_usd as line_total,
        
        -- Quality flags
        has_quality_issues,
        all_quality_checks
    FROM fact_orders
)

SELECT 
    -- Degenerate dimensions
    order_id as order_number,
    line_number as line_item_number,

    -- Foreign keys
    customer_id,
    product_id,
    store_id,

    -- Date/time attributes
    order_ts_utc,
    order_dt_local,
    extract(year from order_dt_local) as order_year,
    extract(month from order_dt_local) as order_month,
    extract(day from order_dt_local) as order_day,
    extract(dow from order_dt_local) as order_day_of_week,
    
    -- Channel and payment attributes
    channel,
    payment_method,
    
    -- Order attributes
    coupon_code,
    currency_code,

    -- Measures
    quantity,
    round(unit_price_usd, 2) as unit_price,
    round(gross_amount, 2) as gross_amount,
    round(discount_amount, 2) as discount_amount,
    round(tax_amount, 2) as tax_amount,
    round(net_amount, 2) as net_amount,
    round(line_total, 2) as line_total,
    
    -- Additional attributes
    line_discount_pct as discount_percentage,
    tax_pct as tax_percentage,
    
    -- Quality flags
    has_quality_issues,
    all_quality_checks

FROM calculations