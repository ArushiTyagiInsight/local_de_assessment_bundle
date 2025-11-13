{{ config(materialized='table') }}

with src as (
    select * from {{ ref('stg_data_quality_log') }}
    where table_name is not null
),

final as (
    select
        table_name,
        rows_processed,
        rows_rejected,
        processing_time_seconds,
        file_size_bytes,
        source_file,
        -- Add any silver-level transformations or aggregations here
        case when rows_rejected > 0 then 'Issues detected' else 'OK' end as quality_status
    from src
)

select * from final
