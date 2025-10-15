-- External views pointing at Bronze Parquet/Delta. Adjust path if needed.
{% set lake_root = '../lake/bronze' %}

create or replace view bronze_customers_parquet as
select * from read_parquet('{{ lake_root }}/parquet/bronze/customers/*.parquet');

create or replace view bronze_customers_delta as
select * from delta_scan('{{ lake_root }}/delta/customers');
