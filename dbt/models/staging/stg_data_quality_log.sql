
{{ config(materialized='table') }}



with raw_log as (
    select * from read_csv_auto('../bronze_data_quality.log', header=false, strict_mode=false)
),
filtered as (
    select column0 as log_line
    from raw_log
    where column0 like '%Rows Processed%' and column0 like '%Rows Rejected%'
),
parsed as (
    select
        regexp_extract(log_line, 'Table: ([^|]+)', 1) as table_name,
        cast(regexp_extract(log_line, 'Rows Processed: ([0-9]+)', 1) as integer) as rows_processed,
        cast(regexp_extract(log_line, 'Rows Rejected: ([0-9]+)', 1) as integer) as rows_rejected,
        case when regexp_extract(log_line, 'Processing Time \(s\): ([0-9.]+)', 1) != ''
            then cast(regexp_extract(log_line, 'Processing Time \(s\): ([0-9.]+)', 1) as double)
            else NULL end as processing_time_seconds,
        case when regexp_extract(log_line, 'File Size \(bytes\): ([0-9]+)', 1) != ''
            then cast(regexp_extract(log_line, 'File Size \(bytes\): ([0-9]+)', 1) as integer)
            else NULL end as file_size_bytes,
        regexp_extract(log_line, 'Source: ([^|]+)$', 1) as source_file
    from filtered
)
select * from parsed
