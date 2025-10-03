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
        columns={
            "customer_id": {"data_type": "bigint", "unique": True},
            "natural_key": {"data_type": "text", "nullable": False},
            "first_name": {"data_type": "text", "nullable": False},
            "last_name": {"data_type": "text", "nullable": False},
            "email": {"data_type": "text", "nullable": False},
            "phone": {"data_type": "text"},
            "address_line1": {"data_type": "text"},
            "address_line2": {"data_type": "text"},
            "city": {"data_type": "text"},
            "state_region": {"data_type": "text"},
            "postcode": {"data_type": "text"},
            "country_code": {"data_type": "text"},
            "latitude": {"data_type": "double"},
            "longitude": {"data_type": "double"},
            "birth_date": {"data_type": "date"},
            "join_ts": {"data_type": "timestamp"},
            "is_vip": {"data_type": "bool"},
            "gdpr_consent": {"data_type": "bool"}
        },
        schema_contract_settings={"data_type": "evolve", "columns": "complete"}
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

        def make_csv_resource(name, filename):
            @dlt.resource(name=name, write_disposition="replace")
            def loader():
                file_path = os.path.join(raw_path, filename)
                with open(file_path, newline='') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        yield row
            @dlt.transformer(data_from=loader, write_disposition="replace")
            def add_audit_columns(record):
                return {
                    **record,
                    "ingestion_ts": datetime.utcnow(),
                    "src_filename": filename
                }
            return add_audit_columns()

        # DLT resources with data quality checks for all files
        
        def validated_customers():
            file_path = os.path.join(raw_path, "customers.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_customers_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "customers.csv"}
        customers_with_audit = map(add_customers_audit, validated_customers())

        return [
            customers_with_audit
        ]
def run_bronze_pipeline():
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="retail_bronze",
        destination=duckdb_dest,
        dataset_name="bronze"
    )
    # Load to DuckDB
    load_info = pipeline.run(retail_source())
    # Also write to Parquet
    pipeline.destination = parquet_dest
    pipeline.run(retail_source())
    # Handle Delta format separately
    #write_to_delta(pipeline.last_trace.last_extract_info)
 