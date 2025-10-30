{{ config(materialized='table') }}

with source as (
    select * from read_parquet('../lake/bronze/parquet/bronze/returns_base/*.parquet')
),

typed as (
    SELECT
        cast(return_id as bigint) as return_id,
        cast(order_id as bigint) as order_id,
        cast(product_id as bigint) as product_id,
        cast(return_ts as timestamp) as return_ts,
        cast(qty as integer) as qty,
        trim(reason) as reason
    FROM source
)

SELECT 
    return_id,
    order_id,
    product_id,
    return_ts,
    qty,
    reason
FROM typed