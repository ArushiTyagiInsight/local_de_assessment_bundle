# scripts/bronze_dlt_pipeline.py
import dlt
from dlt.sources.filesystem import filesystem
import pyarrow as pa
import csv
import os
import sys
import logging

logging.basicConfig(filename='bronze_data_quality.log', level=logging.INFO)
import json
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from schemas.schemas import *
from datetime import datetime, timezone

def ensure_dir(path):
    """Ensure directory exists"""
    if not os.path.exists(path):
        os.makedirs(path)

def get_dlt_type(pa_type):
    """
    Convert PyArrow type to DLT type
    """
    # Map PyArrow types to DLT types
    type_mapping = {
        pa.int64(): "bigint",
        pa.string(): "text",
        pa.float64(): "double",
        pa.date32(): "date",
        pa.bool_(): "bool"
    }
    
    # First try the direct mapping
    if pa_type in type_mapping:
        return type_mapping[pa_type]
    
    # Handle special cases
    if isinstance(pa_type, pa.TimestampType):
        return "timestamp"
    elif isinstance(pa_type, pa.Decimal128Type):
        return "decimal"
    else:
        return "text"  # Default fallback

def create_customer_columns():
    """
    Create DLT column definitions for customers table by reading from schemas.py
    """
    dlt_columns = {}
    
   
    # Process each field in the customers schema
    for field in customers_schema:
        col_def = {
            "data_type": get_dlt_type(field.type)
        }
        
        # Add constraints based on business rules
        if field.name == "customer_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["natural_key", "first_name", "last_name", "email"]:
            col_def["nullable"] = False
        
        dlt_columns[field.name] = col_def
    
    return dlt_columns

def create_product_columns():
    """Create DLT column definitions for products table"""
    dlt_columns = {}
    for field in products_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name == "product_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["sku", "name", "category"]:
            col_def["nullable"] = False
        if isinstance(field.type, pa.Decimal128Type):
            col_def["precision"] = field.type.precision
            col_def["scale"] = field.type.scale
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_supplier_columns():
    """Create DLT column definitions for suppliers table"""
    dlt_columns = {}
    for field in suppliers_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name == "supplier_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["supplier_code", "name"]:
            col_def["nullable"] = False
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_store_columns():
    """Create DLT column definitions for stores table"""
    dlt_columns = {}
    for field in stores_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name == "store_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["store_code", "name", "channel"]:
            col_def["nullable"] = False
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_order_header_columns():
    """Create DLT column definitions for order_header table"""
    dlt_columns = {}
    for field in orders_header_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name == "order_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["order_ts", "customer_id", "store_id"]:
            col_def["nullable"] = False
        if isinstance(field.type, pa.Decimal128Type):
            col_def["precision"] = field.type.precision
            col_def["scale"] = field.type.scale
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_order_lines_columns():
    """Create DLT column definitions for order_lines table"""
    dlt_columns = {}
    for field in orders_lines_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name in ["order_id", "line_number"]:
            col_def["nullable"] = False
        if isinstance(field.type, pa.Decimal128Type):
            col_def["precision"] = field.type.precision
            col_def["scale"] = field.type.scale
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_exchange_rate_columns():
    """Create DLT column definitions for exchange_rates table"""
    dlt_columns = {}
    for field in exchange_rates_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name in ["date", "currency"]:
            col_def["nullable"] = False
            if field.name == "date":
                col_def["unique"] = True  # Assuming date+currency is unique
        if isinstance(field.type, pa.Decimal128Type):
            col_def["precision"] = field.type.precision
            col_def["scale"] = field.type.scale
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_returns_base_columns():
    """Create DLT column definitions for base returns table"""
    dlt_columns = {}
    # Use day1 schema as base
    for field in returns_day1_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name == "return_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["order_id", "product_id", "return_ts"]:
            col_def["nullable"] = False
        if isinstance(field.type, pa.Decimal128Type):
            col_def["precision"] = field.type.precision
            col_def["scale"] = field.type.scale
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_returns_evolved_columns():
    """Create DLT column definitions for evolved returns table with return_reason_code"""
    dlt_columns = {}
    # Start with base schema
    dlt_columns.update(create_returns_base_columns())
    # Add return_reason_code column
    dlt_columns["return_reason_code"] = {
        "data_type": "text",
        "nullable": True  # Allow nullable for backward compatibility
    }
    return dlt_columns

def create_shipments_columns():
    """Create DLT column definitions for shipments table"""
    dlt_columns = {}
    for field in shipments_schema:
        col_def = {"data_type": get_dlt_type(field.type)}
        if field.name == "shipment_id":
            col_def["unique"] = True
            col_def["nullable"] = False
        elif field.name in ["order_id", "carrier", "shipped_at"]:
            col_def["nullable"] = False
        if isinstance(field.type, pa.Decimal128Type):
            col_def["precision"] = field.type.precision
            col_def["scale"] = field.type.scale
        dlt_columns[field.name] = col_def
    return dlt_columns

def create_sensor_columns():
    """Create DLT column definitions for sensors table"""
    dlt_columns = {}
    for field in sensors_schema:
        col_def = {
            "data_type": get_dlt_type(field.type),
            "nullable": field.nullable  # Use the nullable property from PyArrow schema
        }
        if isinstance(field.type, pa.Decimal128Type):
            col_def["precision"] = field.type.precision
            col_def["scale"] = field.type.scale
        if field.name == "sensor_ts":
            col_def["data_type"] = "timestamp"
    return dlt_columns


# Configure destinations
duckdb_dest = dlt.destinations.duckdb(
    credentials="duckdb/warehouse.duckdb"
)

# Configure Parquet destination with explicit settings
parquet_dest = dlt.destinations.filesystem(
    bucket_url=os.path.abspath("lake/bronze/parquet"),
    file_format="parquet",
    file_options={"compression": "snappy"}
)

@dlt.source(name="retail_bronze")
def retail_source(raw_path: str = "data_raw"):
    def load_csv(filename, resource_name, schema_func, options=None):
        """Helper function to load CSV files with consistent audit columns"""
        resource_config = {
            "name": resource_name,
            "write_disposition": "replace",
            "file_format": "parquet"  # Default to parquet
        }
        
        # Handle schema function - could be a function or lambda
        if callable(schema_func):
            columns = schema_func()
            resource_config["columns"] = columns
            
        # Add any additional options
        if options:
            resource_config.update(options)
        
        @dlt.resource(**resource_config)
        def load_data():
            file_path = os.path.join(raw_path, filename)
            count = 0
            rejected = 0
            start_time = datetime.now()
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Convert empty strings to None for nullable fields
                    processed_row = {}
                    for key, value in row.items():
                        if value == '':
                            processed_row[key] = None
                        elif key == 'sensor_ts' and resource_name == 'sensors':
                            # Special handling for sensor_ts timestamps
                            try:
                                processed_row[key] = datetime.fromisoformat(value.rstrip('Z')).replace(tzinfo=timezone.utc)
                            except (ValueError, AttributeError):
                                # Use far future date for NULL sensor timestamps
                                processed_row[key] = datetime(9999, 12, 31, 23, 59, 59).replace(tzinfo=timezone.utc)
                        else:
                            processed_row[key] = value
                    
                    # Example rejection logic: count missing required fields
                    if any(v is None for k, v in processed_row.items() if k != 'sensor_ts'):
                        rejected += 1
                    count += 1
                    yield {**processed_row, 
                          "ingestion_ts": datetime.now(timezone.utc),
                          "src_filename": filename}
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            print(f"Loaded {count} rows from {filename}")
            logging.info(f"Table: {resource_name} | Rows Processed: {count} | Rows Rejected: {rejected} | Processing Time (s): {processing_time} | File Size (bytes): {file_size} | Source: {filename}")
        return load_data

    def load_jsonl(filename, resource_name, schema_func, options=None):
        """Helper function to load JSONL files with consistent audit columns"""
        resource_config = {
            "name": resource_name,
            "write_disposition": "replace",
            "columns": schema_func()
        }
        if options:
            resource_config.update(options)
        
        @dlt.resource(**resource_config)
        def load_data():
            file_path = os.path.join(raw_path, filename)
            count = 0
            rejected = 0
            start_time = datetime.now()
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line)
                    # Example rejection logic: count missing required fields
                    if any(v is None for v in data.values()):
                        rejected += 1
                    count += 1
                    yield {**data,
                          "ingestion_ts": datetime.now(timezone.utc),
                          "src_filename": filename}
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            print(f"Loaded {count} rows from {filename}")
            logging.info(f"Table: {resource_name} | Rows Processed: {count} | Rows Rejected: {rejected} | Processing Time (s): {processing_time} | File Size (bytes): {file_size} | Source: {filename}")
        return load_data

    def load_xlsx(filename, resource_name, schema_func, options=None, sheet_name=0):
        """Helper function to load Excel files with consistent audit columns"""
        resource_config = {
            "name": resource_name,
            "write_disposition": "replace",
            "file_format": "parquet",
            "columns": schema_func()
        }
        if options:
            resource_config.update(options)
        
        @dlt.resource(**resource_config)
        def load_data():
            file_path = os.path.join(raw_path, filename)
            start_time = datetime.now()
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            # Read Excel file into pandas DataFrame
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            # For exchange rates, pivot the data to get currency as a column value
            if resource_name == "exchange_rates":
                df_melted = df.melt(id_vars=['date'], var_name='currency', value_name='rate')
                df = df_melted
            count = 0
            rejected = 0
            # Convert to dict records and yield
            for record in df.to_dict('records'):
                # Example rejection logic: count missing required fields
                if any(v is None for v in record.values()):
                    rejected += 1
                count += 1
                yield {**record,
                      "ingestion_ts": datetime.now(timezone.utc),
                      "src_filename": filename}
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            print(f"Loaded {count} rows from {filename}")
            logging.info(f"Table: {resource_name} | Rows Processed: {count} | Rows Rejected: {rejected} | Processing Time (s): {processing_time} | File Size (bytes): {file_size} | Source: {filename}")
        return load_data

    def load_parquet(filename, resource_name, schema_func, options=None):
        """Helper function to load Parquet files with consistent audit columns"""
        resource_config = {
            "name": resource_name,
            "write_disposition": "replace",
            "columns": schema_func(),
            "file_format": "parquet"  # Default to parquet for parquet files
        }
        if options:
            resource_config.update(options)
        
        @dlt.resource(**resource_config)
        def load_data():
            file_path = os.path.join(raw_path, filename)
            start_time = datetime.now()
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            # Read Parquet file into pandas DataFrame
            df = pd.read_parquet(file_path)
            count = 0
            rejected = 0
            # Convert to dict records and yield
            for record in df.to_dict('records'):
                # Example rejection logic: count missing required fields
                if any(v is None for v in record.values()):
                    rejected += 1
                count += 1
                yield {**record,
                      "ingestion_ts": datetime.now(timezone.utc),
                      "src_filename": filename}
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            print(f"Loaded {count} rows from {filename}")
            logging.info(f"Table: {resource_name} | Rows Processed: {count} | Rows Rejected: {rejected} | Processing Time (s): {processing_time} | File Size (bytes): {file_size} | Source: {filename}")
        return load_data

    # Define all resources
    resources = []
    
    # Load dimension tables (full refresh)
    resources.append(load_csv("customers.csv", "customers", create_customer_columns))
    resources.append(load_csv("products.csv", "products", create_product_columns))
    resources.append(load_csv("suppliers.csv", "suppliers", create_supplier_columns))
    resources.append(load_csv("stores.csv", "stores", create_store_columns))

    # Define incremental loading for orders
    @dlt.resource(
        name="orders_header",
        write_disposition="merge",
        primary_key="order_id",
        file_format="parquet",
        columns=create_order_header_columns()
    )
    def orders_header_incremental(updated_after=dlt.sources.incremental("order_ts")):
        """Load orders header data incrementally based on order_ts from partitioned structure"""
        orders_base_path = os.path.join(raw_path, "orders")
        if not os.path.exists(orders_base_path):
            print(f"Warning: orders directory not found in {raw_path}, skipping...")
            return
            
        # Walk through all date partitions
        for dt_folder in os.listdir(orders_base_path):
            if not dt_folder.startswith("order_dt="):
                continue
                
            partition_path = os.path.join(orders_base_path, dt_folder)
            orders_file = os.path.join(partition_path, "orders.csv")
            
            if not os.path.exists(orders_file):
                continue
                
            print(f"Processing partition: {dt_folder}")  # Add logging
            with open(orders_file, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Convert order_ts to UTC datetime for comparison
                    row_ts_str = row["order_ts"].rstrip('Z')
                    row_ts = datetime.fromisoformat(row_ts_str)
                    if not row_ts.tzinfo:
                        # If timestamp is naive, assume it's UTC
                        row_ts = row_ts.replace(tzinfo=timezone.utc)
                    
                    # Convert last_value to UTC datetime if it exists
                    if updated_after.last_value:
                        last_value = datetime.fromisoformat(updated_after.last_value)
                        if not last_value.tzinfo:
                            last_value = last_value.replace(tzinfo=timezone.utc)
                    else:
                        last_value = None
                    
                    if not last_value or row_ts > last_value:
                        # Store the original timestamp string to preserve format
                        yield {**row,
                              "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                              "src_filename": f"orders/{dt_folder}/orders.csv"}
    
    resources.append(orders_header_incremental)

    # Define incremental loading for order lines
    @dlt.resource(
        name="orders_lines",
        write_disposition="merge",
        primary_key=["order_id", "line_number"],
        file_format="parquet",
        columns=create_order_lines_columns()
    )
    def orders_lines_incremental(orders_updated_after=dlt.sources.incremental("order_id")):
        """Load order lines incrementally from partitioned structure"""
        orders_base_path = os.path.join(raw_path, "orders")
        if not os.path.exists(orders_base_path):
            print(f"Warning: orders directory not found in {raw_path}, skipping...")
            return
            
        # Walk through all date partitions
        for dt_folder in os.listdir(orders_base_path):
            if not dt_folder.startswith("order_dt="):
                continue
                
            partition_path = os.path.join(orders_base_path, dt_folder)
            order_lines_file = os.path.join(partition_path, "order_lines.csv")
            
            if not os.path.exists(order_lines_file):
                continue
                
            print(f"Processing order lines partition: {dt_folder}")  # Add logging
            with open(order_lines_file, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if not orders_updated_after.last_value or int(row["order_id"]) > int(orders_updated_after.last_value):
                        yield {**row,
                              "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                              "src_filename": f"orders/{dt_folder}/order_lines.csv"}

    resources.append(orders_lines_incremental)
    # Add new data sources if files exist
    for file_info in [
        ("sensors.csv", "sensors", lambda: {
            "sensor_ts": {"data_type": "timestamp", "nullable": True},
            "store_id": {"data_type": "bigint"},  # Required field, non-nullable
            "shelf_id": {"data_type": "text", "nullable": True},
            "temperature_c": {"data_type": "double", "nullable": True},  # Using double instead of decimal for better compatibility
            "humidity_pct": {"data_type": "double", "nullable": True},  # Using double instead of decimal for better compatibility
            "battery_mv": {"data_type": "bigint", "nullable": True},
            "ingestion_ts": {"data_type": "timestamp"},  # Required field, non-nullable
            "src_filename": {"data_type": "text"}  # Required field, non-nullable
        }, load_csv, {"file_format": "parquet"}),
        # ("events.jsonl", "events", lambda: {
        #     "json": {"data_type": "text", "nullable": True},
        #     "ingestion_ts": {"data_type": "timestamp"},
        #     "src_filename": {"data_type": "text"}
        # }, load_csv),
        ("exchange_rates.xlsx", "exchange_rates", create_exchange_rate_columns, load_xlsx),
        ("returns/returns_base.csv", "returns_base", create_returns_base_columns, load_csv),
        ("returns/returns_evolved.csv", "returns_evolved", create_returns_evolved_columns, load_csv),
        ("shipments.parquet", "shipments", create_shipments_columns, load_parquet)
    ]:
        # Unpack with optional options
        if len(file_info) == 5:
            filename, resource_name, schema_func, loader_func, options = file_info
        else:
            filename, resource_name, schema_func, loader_func = file_info
            options = None
        if os.path.exists(os.path.join(raw_path, filename)):
            print(f"Loading {resource_name} from {filename}")
            resource = loader_func(filename, resource_name, schema_func, options)
            resources.append(resource)
        else:
            print(f"Warning: {filename} not found in {raw_path}, skipping...")
    
    return resources
def write_rejects(failed_jobs):
    """Write failed records to rejects folder"""
    ensure_dir("lake/_rejects")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    for job in failed_jobs:
        resource_name = job.resource_name
        failed_items = job.items if hasattr(job, 'items') else []
        
        if failed_items:
            reject_file = f"lake/_rejects/{resource_name}_{ts}.json"
            with open(reject_file, 'w') as f:
                for item in failed_items:
                    f.write(json.dumps({
                        "data": item.data,
                        "error": str(item.error),
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }) + "\n")
            print(f"Written {len(failed_items)} failed records to {reject_file}")

def run_bronze_pipeline():

    # Ensure directories exist for both destinations
    ensure_dir("lake/bronze/parquet")
    ensure_dir("lake/bronze/dlt_storage")

    # Create DuckDB pipeline with explicit schema loading
    duckdb_pipeline = dlt.pipeline(
        pipeline_name="retail_bronze_duckdb",
        destination=duckdb_dest,
        dataset_name="bronze"
    )

    # Ensure clean state by dropping existing tables
    duckdb_pipeline.drop()
    # Run with explicit configuration to ensure schema recreation
    duckdb_info = duckdb_pipeline.run(
        retail_source(),
        write_disposition="replace"  # Force table recreation with new schema
    )
    print("[DLT] DuckDB load_info:")
    print(duckdb_info)

    # Create separate Parquet pipeline with parquet file format
    parquet_pipeline = dlt.pipeline(
        pipeline_name="retail_bronze_parquet",
        destination=parquet_dest,
        dataset_name="bronze"
    )

    # Load to Parquet
    parquet_pipeline.drop()
    # Run with explicit configuration to ensure parquet output
    parquet_info = parquet_pipeline.run(
        retail_source(),
        write_disposition="replace"  # Force table recreation with new schema
    )
    print("[DLT] Parquet load_info:")
    print(parquet_info)
    

def run_parquet_pipeline():
    # Ensure directories exist
    ensure_dir("lake/bronze/parquet")
    ensure_dir("lake/bronze/dlt_storage")
    
    # Create pipeline with parquet destination
    pipeline = dlt.pipeline(
        pipeline_name="retail_bronze",
        destination=parquet_dest,
        dataset_name="bronze"
    )
    
    # Drop existing data for clean slate
    pipeline.drop()
    
    # Run the pipeline and capture load info for Parquet
    parquet_info = pipeline.run(retail_source(), loader_file_format="parquet")
    print("[DLT] Parquet load_info:")
    print(parquet_info)
    
    # Handle failed records for Parquet pipeline
    if hasattr(parquet_info, 'load_packages'):
        for package in parquet_info.load_packages:
            if hasattr(package, 'jobs') and package.jobs.get("failed_jobs"):
                write_rejects(package.jobs["failed_jobs"])

if __name__ == "__main__":
    run_bronze_pipeline()
    run_parquet_pipeline()