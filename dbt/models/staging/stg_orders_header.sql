{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/orders_header/*.parquet')
),
typed as (
    SELECT
        cast(order_id as bigint) as order_id,
        cast(order_ts as timestamp) as order_ts_utc,
        cast(order_dt_local as date) as order_dt_local,
        cast(customer_id as bigint) as customer_id,
        cast(store_id as bigint) as store_id,
        trim(channel) as channel,
        trim(payment_method) as payment_method,
        trim(coupon_code) as coupon_code,
        cast(shipping_fee as decimal(12,2)) as shipping_fee_usd,
        upper(trim(currency)) as currency_code,
        -- Add row number to handle duplicates
        ROW_NUMBER() OVER (
            PARTITION BY cast(order_id as bigint)
            ORDER BY cast(order_ts as timestamp) DESC -- Take the latest record
        ) as row_num
    FROM src
),
deduped as (
    SELECT * FROM typed WHERE row_num = 1  -- Take only the first record for each order_id
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN shipping_fee_usd < 0 THEN 'Negative shipping fee'
            ELSE NULL 
        END as shipping_fee_check,
        CASE 
            WHEN currency_code NOT IN ('USD', 'AUD', 'EUR', 'GBP') THEN 'Invalid currency'
            ELSE NULL 
        END as currency_check,
        CASE 
            WHEN channel NOT IN ('web', 'mobile', 'store') THEN 'Invalid channel'
            ELSE NULL 
        END as channel_check
    FROM deduped
)

SELECT 
    order_id,
    order_ts_utc,
    order_dt_local,
    customer_id,
    store_id,
    channel,
    payment_method,
    coupon_code,
    shipping_fee_usd,
    currency_code,
    CASE 
        WHEN shipping_fee_check IS NOT NULL 
        OR currency_check IS NOT NULL 
        OR channel_check IS NOT NULL 
        THEN concat_ws(', ',
            NULLIF(shipping_fee_check, ''),
            NULLIF(currency_check, ''),
            NULLIF(channel_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated