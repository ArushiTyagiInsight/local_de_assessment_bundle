{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH sales_metrics AS (
    SELECT 
        product_id,
        COUNT(DISTINCT order_number) as total_orders,
        SUM(quantity) as total_quantity_sold,
        SUM(line_total) as total_revenue,
        COUNT(DISTINCT customer_id) as unique_customers,
        MAX(order_dt_local) as last_order_date
    FROM {{ ref('fct_sales') }}
    GROUP BY product_id
),

return_metrics AS (
    SELECT
        product_id,
        COUNT(DISTINCT return_id) as total_returns,
        SUM(return_quantity) as returned_quantity,
        SUM(return_value) as total_return_value
    FROM {{ ref('fct_returns') }}
    GROUP BY product_id
),

product_stats AS (
    SELECT 
        -- Product Details
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory as sub_category,
        
        -- Sales Metrics
        COALESCE(s.total_orders, 0) as total_orders,
        COALESCE(s.total_quantity_sold, 0) as total_quantity_sold,
        COALESCE(s.total_revenue, 0) as total_revenue,
        COALESCE(s.unique_customers, 0) as unique_customers,
        s.last_order_date,
        
        -- Return Metrics
        COALESCE(r.total_returns, 0) as total_returns,
        COALESCE(r.returned_quantity, 0) as returned_quantity,
        COALESCE(r.total_return_value, 0) as total_return_value
    FROM {{ ref('dim_product_scd') }} p
    LEFT JOIN sales_metrics s ON p.product_id = s.product_id
    LEFT JOIN return_metrics r ON p.product_id = r.product_id
    WHERE p.is_current = true
)

SELECT
    -- Product Details
    product_id,
    product_name,
    category,
    sub_category,
    
    -- Sales Volume Metrics
    total_orders,
    total_quantity_sold,
    ROUND(total_revenue, 2) as total_revenue,
    unique_customers,
    
    -- Per Order Metrics
    ROUND(total_revenue::FLOAT / NULLIF(total_orders, 0), 2) as avg_revenue_per_order,
    ROUND(total_quantity_sold::FLOAT / NULLIF(total_orders, 0), 2) as avg_quantity_per_order,
    
    -- Return Metrics
    total_returns,
    returned_quantity,
    ROUND(total_return_value, 2) as total_return_value,
    ROUND(returned_quantity::FLOAT * 100 / NULLIF(total_quantity_sold, 0), 2) as return_rate_pct,
    ROUND(total_return_value::FLOAT * 100 / NULLIF(total_revenue, 0), 2) as return_value_pct,
    
    -- Performance Indicators
    CASE
        WHEN total_revenue > 100000 THEN 'A'
        WHEN total_revenue > 10000 THEN 'B'
        ELSE 'C'
    END as revenue_category,
    
    CASE
        WHEN return_rate_pct > 10 THEN 'High'
        WHEN return_rate_pct > 5 THEN 'Medium'
        ELSE 'Low'
    END as return_risk,
    
    CASE
        WHEN total_orders > 0 AND DATEDIFF('day', last_order_date, CURRENT_DATE) <= 30 THEN 'Active'
        WHEN total_orders > 0 AND DATEDIFF('day', last_order_date, CURRENT_DATE) <= 90 THEN 'Slowing'
        ELSE 'Inactive'
    END as product_status

FROM product_stats