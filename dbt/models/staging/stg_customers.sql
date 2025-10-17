{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/customers/*.parquet')
),
typed as (
  SELECT
        cast(customer_id as bigint) as customer_id,
        natural_key,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        phone,--regexp_replace(phone, '[^0-9+]', '') as phone,
        trim(address_line1) as address_line1,
        trim(address_line2) as address_line2,
        trim(city) as city,--initcap(trim(city)) as city,
        trim(state_region) as state_region,
        trim(postcode) as postcode,
        upper(trim(country_code)) as country_code,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(birth_date as date) as birth_date,
        cast(join_ts as timestamp) as join_ts_utc,
        cast(is_vip as boolean) as is_vip,
        cast(gdpr_consent as boolean) as gdpr_consent
    FROM src
),
validated AS (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$' THEN 'Invalid email format'
            ELSE NULL 
        END as email_check,
        CASE 
            WHEN birth_date > current_date THEN 'Future date of birth'
            WHEN birth_date < '1900-01-01' THEN 'Date too old'
            ELSE NULL 
        END as birth_date_check,
        CASE 
            WHEN country_code !~ '^[A-Z]{2}$' THEN 'Invalid country code'
            ELSE NULL 
        END as country_code_check
    FROM typed
)

SELECT 
    customer_id,
    natural_key,
    email,
    first_name,
    last_name,
    phone,
    address_line1,
    address_line2,
    city,
    state_region,
    postcode,
    country_code,
    latitude,
    longitude,
    birth_date,
    join_ts_utc,
    is_vip,
    gdpr_consent,
    CASE 
        WHEN email_check IS NOT NULL 
        OR birth_date_check IS NOT NULL 
        OR country_code_check IS NOT NULL 
        THEN concat_ws(', ',
            NULLIF(email_check, ''),
            NULLIF(birth_date_check, ''),
            NULLIF(country_code_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated