{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/exchange_rates/*.parquet')
),
typed as (
    SELECT
        cast(date as date) as date,
        trim(currency) as currency,
        cast(rate_to_aud as decimal(18,8)) as rate_to_aud
    FROM src
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN rate_to_aud <= 0 THEN 'Invalid exchange rate (less than or equal to 0)'
            ELSE NULL 
        END as rate_check,
        CASE 
            WHEN currency !~ '^[A-Z]{3}$' THEN 'Invalid currency code (must be 3 uppercase letters)'
            ELSE NULL 
        END as currency_check
    FROM typed
)

SELECT 
    date,
    currency,
    rate_to_aud,
    CASE 
        WHEN rate_check IS NOT NULL OR currency_check IS NOT NULL 
        THEN concat_ws(', ',
            NULLIF(rate_check, ''),
            NULLIF(currency_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated