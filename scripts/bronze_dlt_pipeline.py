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

def create_customer_columns():
    """
    Create DLT column definitions for customers table by reading from schemas.py
    Returns a dictionary of column definitions with data types and constraints
    """
    dlt_columns = {}
    
    # Map PyArrow types to DLT types
    type_mapping = {
        pa.int64(): "bigint",
        pa.string(): "text",
        pa.float64(): "double",
        pa.date32(): "date",
        pa.bool_(): "bool"
    }
    
    # Process each field in the customers schema
    for field in customers_schema:
        col_def = {
            "data_type": type_mapping.get(field.type, "text")  # Default to text if type not found
        }
        
        # Special handling for timestamp type
        if isinstance(field.type, pa.TimestampType):
            col_def["data_type"] = "timestamp"
        
        # Add constraints based on business rules
        if field.name == "customer_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["natural_key", "first_name", "last_name", "email"]:
            col_def["nullable"] = False
        
        dlt_columns[field.name] = col_def
    
    return dlt_columns

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
        columns=create_customer_columns()
    )
    def load_customers():
        # Read CSV and yield data
        # DLT handles schema validation automatically
        #added code for counting rows
        file_path = os.path.join(raw_path, "customers.csv")
        count = 0
        with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                count += 1
                yield row
        print(f"Loaded {count} rows from customers.csv")

    @dlt.transformer(
        data_from=load_customers,
        write_disposition="replace"
    )
    def add_audit_columns(record):
        return {
            **record,
            "ingestion_ts": datetime.utcnow(),
            "src_filename": "customers.csv"
        }
    return [add_audit_columns,load_customers]
def run_bronze_pipeline():
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="retail_bronze",
        destination=duckdb_dest,
        dataset_name="bronze",
        #schema_contract_settings={
        #    "data_type": "evolve",  # Allow schema evolution
        #    "columns": "complete"   # But require all defined columns
        #}
    )
    # Load to DuckDB
    load_info = pipeline.run(retail_source())
    print("[DLT] DuckDB load_info:")
    print(load_info)
    # Also write to Parquet
    pipeline.destination = parquet_dest
    pipeline.run(retail_source())
    # Handle Delta format separately
    #write_to_delta(pipeline.last_trace.last_extract_info)

def run_parquet_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="retail_bronze",
        destination=parquet_dest,
        dataset_name="bronze",
        #schema_contract_settings={
        #    "data_type": "evolve",  # Allow schema evolution
        #    "columns": "complete"   # But require all defined columns
        #}
    )
    load_info = pipeline.run(retail_source())
    print("[DLT] Parquet load_info:")
    print(load_info)

if __name__ == "__main__":
    run_bronze_pipeline()
    run_parquet_pipeline()