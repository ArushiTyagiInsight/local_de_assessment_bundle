{{
    config(
        materialized='incremental',
        unique_key='return_id',
        on_schema_change='merge'
    )
}}

WITH source_data AS (
    SELECT 
        cast(return_id as bigint) as return_id,
        cast(order_id as bigint) as order_id,
        cast(product_id as bigint) as product_id,
        cast(return_ts as timestamp) as return_ts,
        cast(qty as integer) as qty,
        trim(reason) as reason,
        trim(return_reason_code) as return_reason_code,  -- Added in v2
        cast(ingestion_ts as timestamp) as ingestion_ts,
        CASE 
            WHEN qty <= 0 THEN 'Invalid return quantity'
            ELSE NULL 
        END as qty_check
    FROM {{ source('bronze', 'returns') }}
)
SELECT 
    r.return_id,
    r.order_id,
    r.product_id,
    r.return_ts,
    r.qty,
    r.reason,
    r.return_reason_code,
    r.ingestion_ts,
    -- Extract date components for analysis
    date_trunc('day', r.return_ts) as return_date,
    extract('month' from r.return_ts) as return_month,
    extract('year' from r.return_ts) as return_year,
    -- Flag high quantity returns
    CASE 
        WHEN r.qty > 10 THEN TRUE 
        ELSE FALSE 
    END as is_bulk_return,
    r.qty_check as data_quality_check
FROM source_data r

{% if is_incremental() %}
WHERE ingestion_ts > (SELECT max(ingestion_ts) FROM {{ this }})
{% endif %}