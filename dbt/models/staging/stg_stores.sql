{{ config(materialized='table', contract={'enforced': true}) }}

with src as (
    select * from read_parquet('../lake/bronze/parquet/bronze/stores/*.parquet')
),
typed as (
  SELECT
        cast(store_id as bigint) as store_id,
        trim(store_code) as store_code,
        trim(name) as name,
        trim(channel) as channel,
        trim(region) as region,
        trim(state) as state,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        cast(open_dt as timestamp) as open_dt,
        cast(close_dt as timestamp) as close_dt,
        trim(close_dt__v_text) as close_dt__v_text
  FROM src
),
validated as (
    SELECT 
        *,
        -- Add data quality check columns
        CASE 
            WHEN store_code !~ '^[A-Z]{2}$' THEN 'Invalid store code'
            ELSE NULL 
        END as store_code_check
    FROM typed
)

SELECT 
    store_id,
    store_code,
    name,
    channel,
    region,
    state,
    latitude,
    longitude,
    open_dt,
    close_dt,
    close_dt__v_text,
    CASE 
        WHEN store_code_check IS NOT NULL  
        THEN concat_ws(', ',
            NULLIF(store_code_check, '')
        )
        ELSE NULL
    END as quality_checks
FROM validated
