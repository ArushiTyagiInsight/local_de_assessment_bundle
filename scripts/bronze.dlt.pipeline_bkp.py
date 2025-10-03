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

    @dlt.resource(
        name="orders",
        write_disposition="append",
        primary_key="order_id",     # for deduplication
        merge_key="order_id"        # for incremental merge
    )
    def load_orders():
        # Incremental loading with automatic dedup
        file_path = os.path.join("data_raw", "orders.csv")
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

        def make_jsonl_resource(name, filename):
            import json
            @dlt.resource(name=name, write_disposition="replace")
            def loader():
                file_path = os.path.join(raw_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            row = json.loads(line)
                            yield row
                        except Exception:
                            continue
            @dlt.transformer(data_from=loader, write_disposition="replace")
            def add_audit_columns(record):
                return {
                    **record,
                    "ingestion_ts": datetime.utcnow(),
                    "src_filename": filename
                }
            return add_audit_columns()

        def make_xlsx_resource(name, filename):
            import pandas as pd
            @dlt.resource(name=name, write_disposition="replace")
            def loader():
                file_path = os.path.join(raw_path, filename)
                df = pd.read_excel(file_path)
                for row in df.to_dict(orient='records'):
                    yield row
            @dlt.transformer(data_from=loader, write_disposition="replace")
            def add_audit_columns(record):
                return {
                    **record,
                    "ingestion_ts": datetime.utcnow(),
                    "src_filename": filename
                }
            return add_audit_columns()

        def make_parquet_resource(name, filename):
            import pyarrow.parquet as pq
            @dlt.resource(name=name, write_disposition="replace")
            def loader():
                file_path = os.path.join(raw_path, filename)
                table = pq.read_table(file_path)
                df = table.to_pandas()
                for row in df.to_dict(orient='records'):
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
        @dlt.resource(
            name="customers",
            columns={
                "email": {"data_type": "text", "nullable": False},
                "customer_id": {"data_type": "bigint", "unique": True},
                "gdpr_consent": {"data_type": "bool"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_customers():
            file_path = os.path.join(raw_path, "customers.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_customers_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "customers.csv"}
        customers_with_audit = map(add_customers_audit, validated_customers())

        @dlt.resource(
            name="products",
            columns={
                "sku": {"data_type": "text", "unique": True},
                "product_id": {"data_type": "bigint", "unique": True},
                "current_price": {"data_type": "decimal", "nullable": True},
                "is_discontinued": {"data_type": "bool"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_products():
            file_path = os.path.join(raw_path, "products.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_products_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "products.csv"}
        products_with_audit = map(add_products_audit, validated_products())

        @dlt.resource(
            name="stores",
            columns={
                "store_code": {"data_type": "text", "nullable": False},
                "store_id": {"data_type": "bigint", "unique": True},
                "latitude": {"data_type": "float"},
                "longitude": {"data_type": "float"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_stores():
            file_path = os.path.join(raw_path, "stores.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_stores_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "stores.csv"}
        stores_with_audit = map(add_stores_audit, validated_stores())

        @dlt.resource(
            name="suppliers",
            columns={
                "supplier_code": {"data_type": "text", "unique": True},
                "supplier_id": {"data_type": "bigint", "unique": True},
                "country_code": {"data_type": "text"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_suppliers():
            file_path = os.path.join(raw_path, "suppliers.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_suppliers_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "suppliers.csv"}
        suppliers_with_audit = map(add_suppliers_audit, validated_suppliers())

        @dlt.resource(
            name="orders_header",
            columns={
                "order_id": {"data_type": "bigint", "unique": True},
                "customer_id": {"data_type": "bigint"},
                "store_id": {"data_type": "bigint"},
                "order_ts": {"data_type": "timestamp"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_orders_header():
            file_path = os.path.join(raw_path, "orders_header.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_orders_header_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "orders_header.csv"}
        orders_header_with_audit = map(add_orders_header_audit, validated_orders_header())

        @dlt.resource(
            name="orders_lines",
            columns={
                "order_id": {"data_type": "bigint"},
                "product_id": {"data_type": "bigint"},
                "qty": {"data_type": "int"},
                "unit_price": {"data_type": "decimal"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_orders_lines():
            file_path = os.path.join(raw_path, "orders_lines.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_orders_lines_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "orders_lines.csv"}
        orders_lines_with_audit = map(add_orders_lines_audit, validated_orders_lines())

        @dlt.resource(
            name="events",
            columns={
                "event_id": {"data_type": "bigint", "unique": True},
                "event_type": {"data_type": "text"},
                "event_ts": {"data_type": "timestamp"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_events():
            import json
            file_path = os.path.join(raw_path, "events.jsonl")
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        row = json.loads(line)
                        yield row
                    except Exception:
                        continue
        def add_events_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "events.jsonl"}
        events_with_audit = map(add_events_audit, validated_events())

        @dlt.resource(
            name="sensors",
            columns={
                "sensor_ts": {"data_type": "timestamp", "nullable": True},
                "store_id": {"data_type": "bigint"},
                "temperature_c": {"data_type": "float"},
                "humidity_pct": {"data_type": "float"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_sensors():
            file_path = os.path.join(raw_path, "sensors.csv")
            with open(file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    yield row
        def add_sensors_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "sensors.csv"}
        sensors_with_audit = map(add_sensors_audit, validated_sensors())

        @dlt.resource(
            name="exchange_rates",
            columns={
                "date": {"data_type": "date"},
                "currency": {"data_type": "text"},
                "rate_to_aud": {"data_type": "decimal"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_exchange_rates():
            import pandas as pd
            file_path = os.path.join(raw_path, "exchange_rates.xlsx")
            df = pd.read_excel(file_path)
            for row in df.to_dict(orient='records'):
                yield row
        def add_exchange_rates_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "exchange_rates.xlsx"}
        exchange_rates_with_audit = map(add_exchange_rates_audit, validated_exchange_rates())

        @dlt.resource(
            name="shipments",
            columns={
                "shipment_id": {"data_type": "bigint", "unique": True},
                "order_id": {"data_type": "bigint"},
                "shipped_at": {"data_type": "timestamp"},
                "delivered_at": {"data_type": "timestamp", "nullable": True}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_shipments():
            import pyarrow.parquet as pq
            file_path = os.path.join(raw_path, "shipments.parquet")
            table = pq.read_table(file_path)
            df = table.to_pandas()
            for row in df.to_dict(orient='records'):
                yield row
        def add_shipments_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "shipments.parquet"}
        shipments_with_audit = map(add_shipments_audit, validated_shipments())

        @dlt.resource(
            name="returns_base",
            columns={
                "return_id": {"data_type": "bigint", "unique": True},
                "order_id": {"data_type": "bigint"},
                "return_ts": {"data_type": "timestamp"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_returns_base():
            import pyarrow.parquet as pq
            file_path = os.path.join(raw_path, "returns_base.parquet")
            table = pq.read_table(file_path)
            df = table.to_pandas()
            for row in df.to_dict(orient='records'):
                yield row
        def add_returns_base_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "returns_base.parquet"}
        returns_base_with_audit = map(add_returns_base_audit, validated_returns_base())

        @dlt.resource(
            name="returns_evolved",
            columns={
                "return_id": {"data_type": "bigint", "unique": True},
                "order_id": {"data_type": "bigint"},
                "return_ts": {"data_type": "timestamp"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_returns_evolved():
            import pyarrow.parquet as pq
            file_path = os.path.join(raw_path, "returns_evolved.parquet")
            table = pq.read_table(file_path)
            df = table.to_pandas()
            for row in df.to_dict(orient='records'):
                yield row
        def add_returns_evolved_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "returns_evolved.parquet"}
        returns_evolved_with_audit = map(add_returns_evolved_audit, validated_returns_evolved())

        @dlt.resource(
            name="returns_upsert_delete",
            columns={
                "return_id": {"data_type": "bigint", "unique": True},
                "order_id": {"data_type": "bigint"},
                "return_ts": {"data_type": "timestamp"}
            },
            schema_contract_settings={"data_type": "evolve", "columns": "complete"},
            write_disposition="replace"
        )
        def validated_returns_upsert_delete():
            import pyarrow.parquet as pq
            file_path = os.path.join(raw_path, "returns_upsert_delete.parquet")
            table = pq.read_table(file_path)
            df = table.to_pandas()
            for row in df.to_dict(orient='records'):
                yield row
        def add_returns_upsert_delete_audit(record):
            return {**record, "ingestion_ts": datetime.utcnow(), "src_filename": "returns_upsert_delete.parquet"}
        returns_upsert_delete_with_audit = map(add_returns_upsert_delete_audit, validated_returns_upsert_delete())

        return [
            customers_with_audit,
            products_with_audit,
            stores_with_audit,
            suppliers_with_audit,
            orders_header_with_audit,
            orders_lines_with_audit,
            events_with_audit,
            sensors_with_audit,
            exchange_rates_with_audit,
            shipments_with_audit,
            returns_base_with_audit,
            returns_evolved_with_audit,
            returns_upsert_delete_with_audit
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
    # Handle Delta format separately (stub)
    if hasattr(pipeline, 'last_trace') and hasattr(pipeline.last_trace, 'last_extract_info'):
        write_to_delta(pipeline.last_trace.last_extract_info)

# Stub for Delta writing - implement as needed
def write_to_delta(extract_info):
    print("[INFO] Delta Lake writing not yet implemented. Received extract_info:")
    print(extract_info)
 