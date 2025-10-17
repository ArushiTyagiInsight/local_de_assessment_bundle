{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH stg_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
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
    join_ts_utc as join_date,
    is_vip,
    gdpr_consent,
    quality_checks
FROM stg_customers