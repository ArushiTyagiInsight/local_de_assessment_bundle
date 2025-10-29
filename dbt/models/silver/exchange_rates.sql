{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH stg_exchange_rates AS (
    SELECT * FROM {{ ref('stg_exchange_rates') }}
),
-- Add moving averages for trend analysis using window functions
moving_avgs AS (
    SELECT
        date,
        currency,
        rate_to_aud,
        -- 7-day moving average
        AVG(rate_to_aud) OVER (
            PARTITION BY currency 
            ORDER BY date ASC
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
        ) as moving_avg_7d,
        -- 30-day moving average
        AVG(rate_to_aud) OVER (
            PARTITION BY currency 
            ORDER BY date ASC
            RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW
        ) as moving_avg_30d,
        quality_checks,
        -- Get previous day's rate for percentage change calculation
        LAG(rate_to_aud, 1) OVER (
            PARTITION BY currency 
            ORDER BY date ASC
        ) as prev_day_rate
    FROM stg_exchange_rates
)

SELECT
    date,
    currency,
    rate_to_aud,
    ROUND(moving_avg_7d, 4) as moving_avg_7d,
    ROUND(moving_avg_30d, 4) as moving_avg_30d,
    -- Calculate daily percentage change
    CASE 
        WHEN prev_day_rate = 0 OR prev_day_rate IS NULL THEN NULL
        ELSE ROUND(((rate_to_aud - prev_day_rate) / prev_day_rate * 100), 2)
    END as daily_change_pct,
    quality_checks
FROM moving_avgs