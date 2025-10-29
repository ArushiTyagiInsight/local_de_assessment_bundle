{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH date_dimension AS (
    -- Generate date series from 2020 to 2030
    SELECT date::DATE as date_id
    FROM GENERATE_SERIES(
        '2020-01-01'::DATE,
        '2030-12-31'::DATE,
        INTERVAL '1 day'
    ) AS date
),

date_attributes AS (
    SELECT
        -- Primary date identifiers
        date_id,
        EXTRACT(YEAR FROM date_id) AS year,
        EXTRACT(MONTH FROM date_id) AS month,
        EXTRACT(DAY FROM date_id) AS day,
        
        -- Date formats
        TO_CHAR(date_id, 'YYYY-MM-DD') AS date_string,
        TO_CHAR(date_id, 'Month') AS month_name,
        TO_CHAR(date_id, 'Mon') AS month_short_name,
        TO_CHAR(date_id, 'Day') AS day_name,
        TO_CHAR(date_id, 'Dy') AS day_short_name,
        
        -- ISO week information
        EXTRACT(ISOYEAR FROM date_id) AS iso_year,
        EXTRACT(WEEK FROM date_id) AS iso_week_number,
        TO_CHAR(date_id, 'IYYY-IW') AS iso_week_id,
        
        -- Quarter information
        EXTRACT(QUARTER FROM date_id) AS quarter_number,
        'Q' || EXTRACT(QUARTER FROM date_id) || ' ' || EXTRACT(YEAR FROM date_id) AS quarter_name,
        
        -- Month end indicators
        CASE 
            WHEN EXTRACT(DAY FROM (date_id + INTERVAL '1 day')) = 1 THEN TRUE 
            ELSE FALSE 
        END AS is_month_end,
        
        -- Quarter end indicators
        CASE 
            WHEN EXTRACT(DAY FROM (date_id + INTERVAL '1 day')) = 1 
                AND EXTRACT(MONTH FROM date_id) IN (3,6,9,12) THEN TRUE 
            ELSE FALSE 
        END AS is_quarter_end,
        
        -- Year end indicator
        CASE 
            WHEN EXTRACT(MONTH FROM date_id) = 12 
                AND EXTRACT(DAY FROM date_id) = 31 THEN TRUE 
            ELSE FALSE 
        END AS is_year_end,
        
        -- Weekend indicator
        CASE 
            WHEN EXTRACT(DOW FROM date_id) IN (0,6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Fiscal Year (assuming July-June fiscal year)
        CASE 
            WHEN EXTRACT(MONTH FROM date_id) >= 7 
            THEN EXTRACT(YEAR FROM date_id) + 1 
            ELSE EXTRACT(YEAR FROM date_id) 
        END AS fiscal_year,
        
        -- Fiscal Quarter
        'FQ' || TO_CHAR(
            CASE 
                WHEN EXTRACT(MONTH FROM date_id) >= 7 
                THEN EXTRACT(MONTH FROM date_id) - 6 
                ELSE EXTRACT(MONTH FROM date_id) + 6 
            END / 3.0, 'FM1') || ' ' || 
        CASE 
            WHEN EXTRACT(MONTH FROM date_id) >= 7 
            THEN EXTRACT(YEAR FROM date_id) + 1 
            ELSE EXTRACT(YEAR FROM date_id) 
        END AS fiscal_quarter,
        
        -- Fiscal Period (Month)
        CASE 
            WHEN EXTRACT(MONTH FROM date_id) >= 7 
            THEN EXTRACT(MONTH FROM date_id) - 6 
            ELSE EXTRACT(MONTH FROM date_id) + 6 
        END AS fiscal_period,
        
        -- Holiday indicators
        CASE
            -- Australian Holidays
            WHEN TO_CHAR(date_id, 'MM-DD') = '01-26' THEN 'AU: Australia Day'
            WHEN TO_CHAR(date_id, 'MM-DD') = '04-25' THEN 'AU: ANZAC Day'
            WHEN TO_CHAR(date_id, 'MM-DD') = '12-25' THEN 'AU/UK/US: Christmas Day'
            WHEN TO_CHAR(date_id, 'MM-DD') = '12-26' THEN 'AU/UK: Boxing Day'
            
            -- UK Holidays
            WHEN TO_CHAR(date_id, 'MM-DD') = '01-01' THEN 'UK/US: New Year''s Day'
            WHEN TO_CHAR(date_id, 'MM-DD') = '05-01' THEN 'UK: Early May Bank Holiday'
            WHEN TO_CHAR(date_id, 'MM-DD') = '08-28' THEN 'UK: Summer Bank Holiday'
            
            -- US Holidays
            WHEN TO_CHAR(date_id, 'MM-DD') = '07-04' THEN 'US: Independence Day'
            WHEN TO_CHAR(date_id, 'MM-DD') = '11-11' THEN 'US: Veterans Day'
            WHEN TO_CHAR(date_id, 'MM-DD') = '11-24' THEN 'US: Thanksgiving'
            
            ELSE NULL
        END AS holiday_name,
        
        CASE
            WHEN TO_CHAR(date_id, 'MM-DD') IN ('01-26', '04-25', '12-25', '12-26') THEN TRUE
            ELSE FALSE
        END AS is_holiday_au,
        
        CASE
            WHEN TO_CHAR(date_id, 'MM-DD') IN ('01-01', '05-01', '08-28', '12-25', '12-26') THEN TRUE
            ELSE FALSE
        END AS is_holiday_uk,
        
        CASE
            WHEN TO_CHAR(date_id, 'MM-DD') IN ('01-01', '07-04', '11-11', '11-24', '12-25') THEN TRUE
            ELSE FALSE
        END AS is_holiday_us,
        
        -- Current date indicators
        CASE 
            WHEN date_id = CURRENT_DATE THEN TRUE 
            ELSE FALSE 
        END AS is_current_day,
        
        CASE 
            WHEN EXTRACT(YEAR FROM date_id) = EXTRACT(YEAR FROM CURRENT_DATE) 
                AND EXTRACT(MONTH FROM date_id) = EXTRACT(MONTH FROM CURRENT_DATE) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current_month,
        
        CASE 
            WHEN EXTRACT(YEAR FROM date_id) = EXTRACT(YEAR FROM CURRENT_DATE) 
                AND EXTRACT(QUARTER FROM date_id) = EXTRACT(QUARTER FROM CURRENT_DATE) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current_quarter,
        
        CASE 
            WHEN EXTRACT(YEAR FROM date_id) = EXTRACT(YEAR FROM CURRENT_DATE) 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current_year,
        
        -- Previous period indicators
        CASE 
            WHEN date_id >= (CURRENT_DATE - INTERVAL '90 days') 
                AND date_id < CURRENT_DATE 
            THEN TRUE 
            ELSE FALSE 
        END AS is_rolling_90_days
        
    FROM date_dimension
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['date_id']) }} AS date_sk,
    *,
    current_timestamp AS dbt_updated_at,
    'dim_date' AS dbt_model
FROM date_attributes
ORDER BY date_id