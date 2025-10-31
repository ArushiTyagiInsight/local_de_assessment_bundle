{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH customer_base AS (
    SELECT
        customer_id,
        customer_name,
        customer_segment,
        customer_lifetime_days,
        customer_age,
        join_date,
        city,
        state,
        country_code
    FROM {{ ref('dim_customer') }}
),

recent_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT CASE 
            WHEN DATE_TRUNC('month', order_dt_local) = DATE_TRUNC('month', CURRENT_DATE) 
            THEN order_number
        END) as orders_this_month
    FROM {{ ref('fct_sales') }}
    GROUP BY customer_id
),

customer_sales AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_number) as total_orders,
        COUNT(DISTINCT DATE_TRUNC('month', order_dt_local)) as active_months,
        SUM(line_total) as lifetime_spend,
        MIN(order_dt_local) as first_order_date,
        MAX(order_dt_local) as last_order_date
    FROM {{ ref('fct_sales') }}
    GROUP BY customer_id
),

final AS (
    SELECT
        -- Customer Details
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        c.customer_lifetime_days,
        c.customer_age,
        
        -- Geographic Information
        c.city,
        c.state,
        c.country_code,
        
        -- Activity Metrics
        COALESCE(s.total_orders, 0) as total_orders,
        COALESCE(s.active_months, 0) as active_months,
        ROUND(COALESCE(s.total_orders, 0)::FLOAT / NULLIF(s.active_months, 0), 2) as avg_orders_per_active_month,
        COALESCE(r.orders_this_month, 0) as orders_this_month,
        
        -- Value Metrics
        ROUND(COALESCE(s.lifetime_spend, 0), 2) as total_lifetime_spend,
        ROUND(COALESCE(s.lifetime_spend, 0)::FLOAT / NULLIF(s.total_orders, 0), 2) as avg_order_value,
        
        -- Temporal Metrics
        s.first_order_date,
        s.last_order_date,
        DATEDIFF('day', s.last_order_date, CURRENT_DATE) as days_since_last_order,
        
        -- Status Indicators
        CASE 
            WHEN r.orders_this_month > 0 THEN 'Active'
            WHEN DATEDIFF('day', s.last_order_date, CURRENT_DATE) <= 90 THEN 'Recent'
            WHEN DATEDIFF('day', s.last_order_date, CURRENT_DATE) <= 180 THEN 'At Risk'
            ELSE 'Churned'
        END as customer_status,
        
        CASE
            WHEN s.total_orders = 1 THEN 'New'
            WHEN s.total_orders > 1 AND r.orders_this_month > 0 THEN 'Returning Active'
            WHEN s.total_orders > 1 THEN 'Returning Inactive'
            ELSE 'Inactive'
        END as customer_type,
        
        -- Value Segmentation
        CASE
            WHEN s.lifetime_spend >= 10000 THEN 'High Value'
            WHEN s.lifetime_spend >= 1000 THEN 'Medium Value'
            ELSE 'Low Value'
        END as value_segment
    FROM customer_base c
    LEFT JOIN customer_sales s ON c.customer_id = s.customer_id
    LEFT JOIN recent_orders r ON c.customer_id = r.customer_id
)

SELECT * FROM final