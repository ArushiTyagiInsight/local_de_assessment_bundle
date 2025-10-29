{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH silver_customers AS (
    -- Reference silver layer model
    SELECT * FROM {{ ref('customers') }}
),

customers_enhanced AS (
    SELECT
        customer_id,
        gdpr_consent,
        true AS is_active,
        is_vip AS vip_status,
        DATE(join_date) AS join_date,
        birth_date AS date_of_birth,
        -- Apply GDPR masking
        CASE 
            WHEN gdpr_consent = false THEN 'MASKED'
            ELSE first_name || ' ' || last_name
        END AS customer_name,
        CASE 
            WHEN gdpr_consent = false THEN MD5(email)
            ELSE email 
        END AS email,
        CASE 
            WHEN gdpr_consent = false THEN REGEXP_REPLACE(phone, '\\d', 'X')
            ELSE phone 
        END AS phone,
        CASE 
            WHEN gdpr_consent = false THEN city || ', ' || state_region || ', ' || country_code
            ELSE COALESCE(address_line1 || CASE WHEN address_line2 IS NOT NULL THEN ', ' || address_line2 ELSE '' END, '')
        END AS address,
        city,
        state_region AS state,
        country_code,
        CASE 
            WHEN gdpr_consent = false THEN 'XXXXX'
            ELSE postcode 
        END AS postal_code,
        -- Add derived attributes
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth)) AS customer_age,
        DATEDIFF('day', join_date, CURRENT_DATE) AS customer_lifetime_days,
        CASE
            WHEN vip_status = True AND DATEDIFF('day', join_date, CURRENT_DATE) > 365 THEN 'Premium'
            WHEN vip_status = True THEN 'New VIP'
            WHEN DATEDIFF('day', join_date, CURRENT_DATE) > 365 THEN 'Loyal'
            WHEN DATEDIFF('day', join_date, CURRENT_DATE) > 90 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment,
        current_timestamp AS dbt_updated_at,
        'dim_customer' AS dbt_model
    FROM silver_customers

)

SELECT 
   {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_sk,
    *
FROM customers_enhanced