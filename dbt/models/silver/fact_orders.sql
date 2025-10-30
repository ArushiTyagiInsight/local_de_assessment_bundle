{{ config(materialized='table') }}

with stg_orders_header as (
    select * from {{ ref('stg_orders_header') }}
),
stg_order_lines as (
    select * from {{ ref('stg_orders_lines') }}
),
joined as (
    SELECT
        -- Order Line Keys
        ol.order_id,
        ol.line_number,
        ol.product_id,
        oh.customer_id,
        oh.store_id,

        -- Order Header Details
        oh.order_ts_utc,
        oh.order_dt_local,
        oh.channel,
        oh.payment_method,
        oh.coupon_code,
        oh.currency_code,

        -- Line Item Details
        ol.quantity,
        ol.unit_price_usd,
        ol.line_discount_pct,
        ol.tax_pct,

        -- Monetary Values
        ol.discount_amount_usd,
        ol.line_total_usd,
        oh.shipping_fee_usd,

        -- Quality Checks
        oh.quality_checks as header_quality_checks,
        ol.quality_checks as line_quality_checks
    FROM stg_order_lines ol
    INNER JOIN stg_orders_header oh 
        ON ol.order_id = oh.order_id
),
validations as (
    SELECT
        *,
        -- Identify records with quality issues
        CASE 
            WHEN header_quality_checks IS NOT NULL 
            OR line_quality_checks IS NOT NULL 
            THEN 'Y' 
            ELSE 'N' 
        END as has_quality_issues,
        -- Combine all quality checks
        NULLIF(concat_ws(', ',
            NULLIF(header_quality_checks, ''),
            NULLIF(line_quality_checks, '')
        ), '') as all_quality_checks
    FROM joined
)

SELECT 
    -- Order Line Keys
    order_id,
    line_number,
    product_id,
    customer_id,
    store_id,

    -- Order Header Details
    order_ts_utc,
    order_dt_local,
    channel,
    payment_method,
    coupon_code,
    currency_code,

    -- Line Item Details
    quantity,
    unit_price_usd,
    line_discount_pct,
    tax_pct,

    -- Monetary Values
    discount_amount_usd,
    line_total_usd,
    shipping_fee_usd,

    -- Quality Information
    has_quality_issues,
    all_quality_checks
FROM validations