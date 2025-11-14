# Data Engineering Assessment - Post Mortem

## Architecture Decisions

### Chose DLT over Option A
I decided to use DLT over the python based manual approach due to the reasons mentioned below :
- DLT has automatic schema inference and evolution
- built-in incremental loading
- data type detation and normalisatoan
- overall far more advanced as compared to the standard python approach

### Design Patterns I Implemented
**Schema-First Approach**: I have used PyArrow schemas from `schemas/schemas.py` as the single source of truth. This helps in detecting mismatches early on.

**Stage based implementation**: I approached at implementing a data load stage at a time and full proofing it then later moving to the next level. Once all the stages are implemented a final run was done for whatever was missing to be implemented in Power BI report.

**Audit**: I have added fields like dbt_updated_at and model_name to each table in the gold layer to facilitate the auditing functionality. There is also a separate log file created from each load that loads the information about the table read, number of records written and rejected, bytes read and time to read the information.

### Implementation Structure
There are separate functions created to load different kinds of files:
- load_csv() - for the CSV files
- load_jsonl() - for the JSONL event data
- load_xlsx() - for the exchange rates Excel file
- load_parquet() - for the shipments data

For storage, I used two functions `run_parquet_pipeline()` and `run_bronze_pipeline()`.

## Challenges

### Writing parquet files
It was quite challenging to generate and load data into parquet format as even when I explecitly defined the parquet format for the load, the files were still loading as .jsonl

### Memory Management
The orders and orders_lines table are generated and stored as partitioned files by date as a single file with everything will not work. The files are read one by one, combined and loaded into single table eventually.

### DBT
I have had some experience with DBT in the past so learning it and then dellivering was a bit easier. But DBT was definitely a new skill that was needed to deliver this project.
A standard data structure architecture was delivered as below:
- **Staging Layer**: staging models in `models/staging/`
- **Silver Layer**: Business logic transformations in `models/silver/`
- **Gold Layer**: Final aggregations optimized for analytics in `models/gold/`

### Duckdb
Learning to write data to duckdb and then getting to know a tool to read it was definitely a learning curve. DBeaver seemed to be a good tool to be able to access the data from the warehouse in a format that is familier (sql). However it was always challenging to write to the warehouse when the DBeaver connection was still connected so had to disconnect and then approach writing.

### Actual Delivery
- 12 bronze tables
- 2 snapshots
- 6 dims and 5 fact tables in Silver layer
- 5 dims and 5 fact tables in Gold layer
- Power BI report connected to the final data warehouse

I have skipped the delivery of delta lake format of data load as that was more of a value add rather than actual delivery expectation.

### Overall Assessment Thoughts

I am quite happy to attempt this assessment and it involved an end to end data engineering task - starting from generating raw data with anomalies to loading it through different stages of data flow and developing reports on top of it. The versitality in data source formats provides good exposure to handling different data sources and bringing them together into the same format. Partitining of source data is another good angle provided in this assessment as that structre is commonly seen while working with a client.

This definitely teaches to work iteratively and try to create the final reports in the first go. Once done going back and forth with any modifications needed in the steps to achieve the final goal.

However, I find there are many gaps in the assessment instructions that lead to a lot of decisions along the way that were confusing or contradicting to what was expected.

But overall, iterative approch of delivery worked quite well for me.