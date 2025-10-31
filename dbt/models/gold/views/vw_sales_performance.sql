{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH order_details AS (
    SELECT 
        ol.*,
        s.store_name,
        s.region as store_region,
        s.channel as store_type,
        c.customer_segment,
        c.customer_lifetime_days,
        c.customer_age,
        c.vip_status,
        p.product_name,
        p.category,
        p.subcategory,
        p.unit_price as base_price,
        CASE 
            WHEN p.price_change_flag THEN TRUE 
            ELSE FALSE 
        END as had_price_change,
        p.price_change_pct,
        p.days_in_catalog
    FROM {{ ref('fct_sales') }} ol
    LEFT JOIN {{ ref('dim_store') }} s ON ol.store_id = s.store_id
    LEFT JOIN {{ ref('dim_customer') }} c ON ol.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_product_scd') }} p ON ol.product_id = p.product_id 
        AND ol.order_dt_local >= p.effective_from 
        AND ol.order_dt_local < p.effective_to
)

SELECT
    -- Time Dimension
    order_dt_local as order_date,
    EXTRACT(YEAR FROM order_dt_local) as order_year,
    EXTRACT(MONTH FROM order_dt_local) as order_month,
    
    -- Location & Channel
    store_name,
    store_region,
    store_type,
    channel as sales_channel,
    
    -- Customer Information
    customer_segment,
    customer_lifetime_days,
    customer_age,
    vip_status,
    
    -- Product Details
    product_name,
    category as product_category,
    subcategory as product_subcategory,
    base_price as original_price,
    had_price_change,
    ROUND(price_change_pct, 2) as price_change_percent,
    days_in_catalog,
    
    -- Order Metrics
    quantity as units_sold,
    ROUND(unit_price, 2) as selling_price,
    ROUND(gross_amount, 2) as gross_amount,
    discount_percentage,
    ROUND(discount_amount, 2) as discount_amount,
    ROUND(net_amount, 2) as net_amount,
    ROUND(tax_amount, 2) as tax_amount,
    tax_percentage,
    
    -- Payment & Fees
    payment_method,
    currency_code,
    
    -- Quality Indicators
    CASE 
        WHEN has_quality_issues = 'Y' THEN TRUE
        ELSE FALSE 
    END as has_issues,
    all_quality_checks as quality_notes

FROM order_details