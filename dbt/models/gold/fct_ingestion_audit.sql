{{ config(materialized='table') }}

with src as (
    select * from {{ ref('data_quality_log') }}
    where table_name is not null
),

fct_audit as (
    select
        table_name,
        sum(rows_processed) as total_rows_processed,
        sum(rows_rejected) as total_rows_rejected,
        avg(processing_time_seconds) as avg_processing_time_seconds,
        avg(file_size_bytes) as avg_file_size_bytes,
        count(*) as ingestion_runs,
        max(quality_status) as latest_quality_status
    from src
    group by table_name
)

select * from fct_audit
