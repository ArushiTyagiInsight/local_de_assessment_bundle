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

