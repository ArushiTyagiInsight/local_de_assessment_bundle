# scripts/bronze_dlt_pipeline.py
import dlt
from dlt.sources.filesystem import filesystem
import pyarrow as pa
import csv
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from schemas.schemas import *
from datetime import datetime

# Configure destinations
duckdb_dest = dlt.destinations.duckdb(
    credentials="duckdb/warehouse.duckdb"
)

parquet_dest = dlt.destinations.filesystem(
    bucket_url="lake/bronze/parquet",
    file_format="parquet"
)

@dlt.source(name="retail_bronze")
def retail_source(raw_path: str = "data_raw"):
    
    @dlt.resource(
        name="customers",
        write_disposition="replace",
        #columns=customers_schema  # Use PyArrow schema
    )
    def load_customers():
        # Read CSV and yield data
        # DLT handles schema validation automatically
        file_path = os.path.join(raw_path, "customers.csv")
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row
        pass
    
    @dlt.transformer(
        data_from=load_customers,
        write_disposition="replace"
    )
    def add_audit_columns(record):
        # Add ingestion_ts, src_filename, etc.
        return {
            **record,
            "ingestion_ts": datetime.utcnow(),
            "src_filename": dlt.current.source_state().get("file", "unknown")
        }
    
    return [
        add_audit_columns()
        # ... other resources
    ]

pipeline = dlt.pipeline(
    pipeline_name="retail_bronze_pipeline",
    destination=parquet_dest,
    dataset_name="retail_bronze"
)

pipeline.run(retail_source())