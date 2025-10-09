# scripts/bronze_dlt_pipeline.py
import dlt
from dlt.sources.filesystem import filesystem
import pyarrow as pa
import csv
import os
import sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from schemas.schemas import *
from datetime import datetime

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

def create_returns_upsert_columns():
    """Create DLT column definitions for returns table with upserts and deletes"""
    # Use the same schema as evolved since structure is the same
    return create_returns_evolved_columns()

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
    def load_csv(filename, resource_name, schema_func):
        """Helper function to load CSV files with consistent audit columns"""
        @dlt.resource(
            name=resource_name,
            write_disposition="replace",
            columns=schema_func()
        )
        def load_data():
            file_path = os.path.join(raw_path, filename)
            count = 0
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    count += 1
                    yield {**row, 
                          "ingestion_ts": datetime.utcnow(),
                          "src_filename": filename}
            print(f"Loaded {count} rows from {filename}")
        return load_data

    def load_xlsx(filename, resource_name, schema_func, sheet_name=0):
        """Helper function to load Excel files with consistent audit columns"""
        @dlt.resource(
            name=resource_name,
            write_disposition="replace",
            columns=schema_func()
        )
        def load_data():
            file_path = os.path.join(raw_path, filename)
            # Read Excel file into pandas DataFrame
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            # For exchange rates, pivot the data to get currency as a column value
            if resource_name == "exchange_rates":
                df_melted = df.melt(id_vars=['date'], var_name='currency', value_name='rate')
                df = df_melted
            count = 0
            # Convert to dict records and yield
            for record in df.to_dict('records'):
                count += 1
                yield {**record,
                      "ingestion_ts": datetime.utcnow(),
                      "src_filename": filename}
            print(f"Loaded {count} rows from {filename}")
        return load_data

    def load_parquet(filename, resource_name, schema_func):
        """Helper function to load Parquet files with consistent audit columns"""
        @dlt.resource(
            name=resource_name,
            write_disposition="replace",
            columns=schema_func()
        )
        def load_data():
            file_path = os.path.join(raw_path, filename)
            # Read Parquet file into pandas DataFrame
            df = pd.read_parquet(file_path)
            count = 0
            # Convert to dict records and yield
            for record in df.to_dict('records'):
                count += 1
                yield {**record,
                      "ingestion_ts": datetime.utcnow(),
                      "src_filename": filename}
            print(f"Loaded {count} rows from {filename}")
        return load_data

    # Define all resources
    resources = []
    
    # Load each table
    resources.append(load_csv("customers.csv", "customers", create_customer_columns))
    resources.append(load_csv("products.csv", "products", create_product_columns))
    resources.append(load_csv("suppliers.csv", "suppliers", create_supplier_columns))
    resources.append(load_csv("stores.csv", "stores", create_store_columns))
    resources.append(load_csv("orders_header.csv", "orders_header", create_order_header_columns))
    resources.append(load_csv("orders_lines.csv", "orders_lines", create_order_lines_columns))
    #resources.append(load_csv("sensors.csv", "sensors", create_sensor_columns))

    # Add new data sources if files exist
    for file_info in [
        ("exchange_rates.xlsx", "exchange_rates", create_exchange_rate_columns, load_xlsx),
        ("returns_base.parquet", "returns_base", create_returns_base_columns, load_parquet),
        ("returns_evolved.parquet", "returns_evolved", create_returns_evolved_columns, load_parquet),
        ("returns_upsert_delete.parquet", "returns_upsert", create_returns_upsert_columns, load_parquet),
        ("shipments.parquet", "shipments", create_shipments_columns, load_parquet)
    ]:
        filename, resource_name, schema_func, loader_func = file_info
        if os.path.exists(os.path.join(raw_path, filename)):
            resources.append(loader_func(filename, resource_name, schema_func))
        else:
            print(f"Warning: {filename} not found in {raw_path}, skipping...")
    
    return resources
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