{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH silver_suppliers AS (
    -- Reference silver layer model
    SELECT * FROM {{ ref('suppliers') }}
),

suppliers_enhanced AS (
    SELECT
        s.supplier_id,
        s.name as supplier_name,
        s.supplier_code,
        s.country_code as country,
        -- Add geographical region mapping
        CASE 
            WHEN s.country_code IN ('AU', 'NZ') THEN 'Oceania'
            WHEN s.country_code IN ('US', 'CA', 'MX') THEN 'North America'
            WHEN s.country_code IN ('BR', 'AR', 'CL') THEN 'South America'
            WHEN s.country_code IN ('GB', 'DE', 'FR', 'IT', 'ES') THEN 'Europe'
            WHEN s.country_code IN ('CN', 'JP', 'IN', 'KR', 'SG') THEN 'Asia'
            ELSE 'Other'
        END as region,
        -- Add supplier tier based on lead time and preferred status
        CASE
            WHEN s.preferred = true AND s.lead_time_days <= 5 THEN 'Tier 1'
            WHEN s.preferred = true OR s.lead_time_days <= 7 THEN 'Tier 2'
            ELSE 'Tier 3'
        END as supplier_tier,
        -- Lead time classification
        CASE
            WHEN s.lead_time_days <= 3 THEN 'Express'
            WHEN s.lead_time_days <= 7 THEN 'Fast'
            WHEN s.lead_time_days <= 14 THEN 'Standard'
            ELSE 'Extended'
        END as lead_time_category,
        s.lead_time_days,
        s.preferred,
        current_timestamp AS dbt_updated_at,
        'dim_supplier' AS dbt_model
    FROM silver_suppliers s
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} AS supplier_sk,
    *
FROM suppliers_enhanced