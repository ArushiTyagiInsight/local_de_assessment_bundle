{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/suppliers/*.parquet')
),
typed as (
    SELECT
        cast(supplier_id as bigint) as supplier_id,
        trim(supplier_code) as supplier_code,
        trim(name) as name,
        trim(country_code) as country_code,
        try_cast(lead_time_days as integer) as lead_time_days,
        cast(preferred as boolean) as preferred,
        -- Store original value for validation
        lead_time_days as lead_time_days_raw
    FROM src
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN country_code !~ '^[A-Z]{2}$' THEN 'Invalid country code'
            ELSE NULL 
        END as country_code_check,
        CASE 
            WHEN lead_time_days IS NULL AND lead_time_days_raw IS NOT NULL THEN 'Invalid lead time format'
            WHEN lead_time_days < 0 THEN 'Negative lead time'
            ELSE NULL 
        END as lead_time_check
    FROM typed
)

SELECT 
    supplier_id,
    supplier_code,
    name,
    country_code,
    lead_time_days,
    preferred,
    CASE 
        WHEN country_code_check IS NOT NULL OR lead_time_check IS NOT NULL 
        THEN concat_ws(', ',
            NULLIF(country_code_check, ''),
            NULLIF(lead_time_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated