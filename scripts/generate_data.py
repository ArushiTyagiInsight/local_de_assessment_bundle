# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import argparse, os, pathlib, random
from datetime import datetime, timedelta, date
import numpy as np
from faker import Faker
from mimesis import Person, Address
import rstr
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--out', type=str, default='data_raw')
    ap.add_argument('--scale', type=float, default=0.01, help='scale for testing (0.01 = 1%)') ##added scale
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)
    fake = Faker('en_AU')

    # Remove existing output files to ensure new data is generated each run
    for fname in [
        'customers.csv', 'products.csv', 'stores.csv', 'suppliers.csv', 'orders_header.csv',
        'orders_lines.csv', 'events.jsonl', 'sensors.csv', 'exchange_rates.xlsx',
        'shipments.parquet', 'returns_base.parquet', 'returns_evolved.parquet', 'returns_upsert_delete.parquet',
    ]:
        fpath = out / fname
        if fpath.exists():
            fpath.unlink()

    # Define base table sizes (100% scale)
    BASE_STORES = 5000
    BASE_SUPPLIERS = 8000
    BASE_CUSTOMERS = 80000
    BASE_PRODUCTS = 25000
    BASE_ORDERS = 1000000
    BASE_ORDER_LINES = 4000000
    BASE_EVENTS = 2000000
    BASE_SENSORS = 10000000
    
    # Generate stores.csv with schema and anomalies
    stores_path = out/'stores.csv'
    num_stores = int(BASE_STORES * args.scale)  # Apply scaling
    channels = ['Retail', 'Online', 'Franchise']
    regions = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']
    states = ['NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT']
    used_store_codes = set()
    # Prepare duplicate store_code indices
    num_duplicates = max(1, int(num_stores * 0.01))  # 1% duplicates
    duplicate_indices = set(random.sample(range(1, num_stores+1), num_duplicates))
    duplicate_codes = []
    # Generate a pool of codes to duplicate
    for _ in range(num_duplicates):
        code = 'STORE-' + rstr.rstr('[A-Z0-9]{5}')
        duplicate_codes.append(code)
    # Prepare impossible lat/lon indices
    num_bad_latlon = max(1, int(num_stores * 0.005))  # 0.5% impossible
    bad_latlon_indices = set(random.sample(range(1, num_stores+1), num_bad_latlon))
    with stores_path.open('w', encoding='utf-8') as f:
        f.write('store_id,store_code,name,channel,region,state,latitude,longitude,open_dt,close_dt\n')
        for i in range(1, num_stores+1):
            # Duplicate store_code logic
            if i in duplicate_indices:
                store_code = random.choice(duplicate_codes)
            else:
                while True:
                    store_code = 'STORE-' + rstr.rstr('[A-Z0-9]{5}')
                    if store_code not in used_store_codes:
                        used_store_codes.add(store_code)
                        break
            name = fake.company()
            channel = random.choice(channels)
            region = random.choice(regions)
            state = random.choice(states)
            # Impossible lat/lon logic
            if i in bad_latlon_indices:
                latitude = random.choice([-200, 200, 999, -999])
                longitude = random.choice([-200, 200, 999, -999])
            else:
                latitude = round(-44 + random.random()*10, 6)
                longitude = round(112 + random.random()*40, 6)
            open_dt = date(2000,1,1) + timedelta(days=random.randint(0, 9000))
            # 80% active (null close_dt), 20% closed
            if random.random() < 0.2:
                close_dt_val = open_dt + timedelta(days=random.randint(30, 5000))
                if close_dt_val > date.today():
                    close_dt = ''
                else:
                    close_dt = close_dt_val.isoformat()
            else:
                close_dt = ''
            f.write(f"{i},{store_code},{name},{channel},{region},{state},{latitude},{longitude},{open_dt.isoformat()},{close_dt}\n")
    
    # Generate suppliers.csv with schema
    suppliers_path = out/'suppliers.csv'
    num_suppliers = int(BASE_SUPPLIERS * args.scale)
    country_codes = ['AU', 'US', 'CN', 'IN', 'DE', 'JP', 'GB', 'FR', 'BR', 'CA']
    used_supplier_codes = set()
    with suppliers_path.open('w', encoding='utf-8') as f:
        f.write('supplier_id,supplier_code,name,country_code,lead_time_days,preferred\n')
        for i in range(1, num_suppliers+1):
            # Ensure supplier_code is unique and matches SUP-[A-Z0-9]{6}
            while True:
                supplier_code = 'SUP-' + rstr.rstr('[A-Z0-9]{6}')
                if supplier_code not in used_supplier_codes:
                    used_supplier_codes.add(supplier_code)
                    break
            name = fake.company()
            country_code = random.choice(country_codes)
            lead_time_days = random.randint(1, 60)
            preferred = random.random() < 0.2  # 20% preferred
            f.write(f"{i},{supplier_code},{name},{country_code},{lead_time_days},{str(preferred)}\n")

    # Generate customers.csv with schema and anomalies
    customers_path = out/'customers.csv'
    num_rows = int(BASE_CUSTOMERS * args.scale)
    num_malformed = random.randint(int(num_rows*0.005), int(num_rows*0.01))  # 0.5-1% malformed emails
    
    # Calculate number of unique keys and duplicates
    num_duplicates = int(num_rows*0.002)  # 0.2% will be duplicates
    num_unique_keys = num_rows - num_duplicates
    
    # Generate base unique natural_keys
    natural_keys = []
    while len(natural_keys) < num_unique_keys:
        key = 'CUST-' + rstr.rstr('[A-Z0-9]{8}')
        if key not in natural_keys:  # Ensure uniqueness
            natural_keys.append(key)
    
    # Select keys to duplicate and create exact duplicates
    keys_to_duplicate = random.sample(natural_keys, num_duplicates)
    all_keys = natural_keys + keys_to_duplicate
    
    # Validate duplicate percentage
    duplicate_count = len(all_keys) - len(set(all_keys))
    assert duplicate_count == num_duplicates, f"Expected {num_duplicates} duplicates, got {duplicate_count}"
    
    # Shuffle all keys to distribute duplicates randomly
    random.shuffle(all_keys)
    # Prepare indices for malformed emails
    malformed_indices = set(random.sample(range(num_rows), num_malformed))
    with customers_path.open('w', encoding='utf-8') as f:
        f.write('customer_id,natural_key,first_name,last_name,email,phone,address_line1,address_line2,city,state_region,postcode,country_code,latitude,longitude,birth_date,join_ts,is_vip,gdpr_consent\n')
        for i in range(1, num_rows+1):
            nk = all_keys[i-1]
            # Malformed email for selected indices
            if (i-1) in malformed_indices:
                email = 'bad_email'
            else:
                email = fake.email()
            lat = -44 + random.random()*10; lon = 112 + random.random()*40
            birth = date(1960,1,1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024,1,1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399))
            # Randomly set phone and address fields to empty (null) for some rows
            phone = fake.phone_number().replace(',',' ')
            address_line1 = fake.street_address().replace(',',' ')
            # 5% null phone, 5% null address_line1, 2% both null
            rand_null = random.random()
            if rand_null < 0.02:
                phone = ''
                address_line1 = ''
            elif rand_null < 0.07:
                phone = ''
            elif rand_null < 0.12:
                address_line1 = ''
            f.write(f"{i},{nk},{fake.first_name()},{fake.last_name()},{email},{phone},{address_line1},,{fake.city().replace(',',' ')},{fake.state_abbr()},{fake.postcode()},AU,{lat:.6f},{lon:.6f},{birth.isoformat()},{join_ts.isoformat()},{str(random.random()<0.15)},{str(random.random()>0.05)}\n")

    # Generate products.csv with the specified schema
    products_path = out/'products.csv'
    num_products = int(BASE_PRODUCTS * args.scale)
    categories = ['Electronics', 'Clothing', 'Home', 'Toys', 'Books', 'Beauty']
    subcategories = {
        'Electronics': ['Phones', 'Laptops', 'Audio', 'Cameras'],
        'Clothing': ['Men', 'Women', 'Kids'],
        'Home': ['Furniture', 'Kitchen', 'Decor'],
        'Toys': ['Outdoor', 'Educational', 'Board Games'],
        'Books': ['Fiction', 'Non-Fiction', 'Comics'],
        'Beauty': ['Skincare', 'Makeup', 'Haircare']
    }
    currencies = ['AUD', 'USD', 'EUR']
    used_skus = set()
    # Calculate anomaly indices for missing/invalid prices
    num_anomaly_prices = random.randint(int(num_products*0.001), int(num_products*0.005))
    anomaly_price_indices = set(random.sample(range(1, num_products+1), num_anomaly_prices))
    # For discontinued products with null discontinued_dt
    discontinued_null_dt_indices = set()
    for idx in range(1, num_products+1):
        if random.random() < 0.01:  # 1% of all products
            discontinued_null_dt_indices.add(idx)
    with products_path.open('w', encoding='utf-8') as f:
        f.write('product_id,sku,name,category,subcategory,current_price,currency,is_discontinued,introduced_dt,discontinued_dt\n')
        for i in range(1, num_products+1):
            # Ensure sku is unique and matches SKU-[A-Z0-9]{6}
            while True:
                sku = 'SKU-' + rstr.rstr('[A-Z0-9]{6}')
                if sku not in used_skus:
                    used_skus.add(sku)
                    break
            name = fake.word().capitalize() + ' ' + fake.word().capitalize()
            category = random.choice(categories)
            subcategory = random.choice(subcategories[category])
            # Price anomaly logic
            if i in anomaly_price_indices:
                # 50% missing, 50% invalid (e.g. negative or string)
                if random.random() < 0.5:
                    price = ''
                else:
                    price = random.choice(['-99.9999', 'not_a_price'])
            else:
                price = f"{round(random.uniform(5, 2000), 4):.4f}"
            currency = random.choice(currencies)
            is_discontinued = random.random() < 0.1
            intro_dt = date(2015,1,1) + timedelta(days=random.randint(0, 365*8))
            # Discontinued logic with some null discontinued_dt
            if is_discontinued:
                if i in discontinued_null_dt_indices:
                    disc_dt = ''
                else:
                    disc_dt_val = intro_dt + timedelta(days=random.randint(30, 2000))
                    if disc_dt_val > date.today():
                        disc_dt = ''
                    else:
                        disc_dt = disc_dt_val.isoformat()
            else:
                disc_dt = ''
            f.write(f"{i},{sku},{name},{category},{subcategory},{price},{currency},{str(is_discontinued)},{intro_dt.isoformat()},{disc_dt}\n")
    
    # Generate orders_header.csv with schema, partitioning, and anomalies
    orders_path = out/'orders_header.csv'
    num_orders = int(BASE_ORDERS * args.scale)
    payment_methods = ['Credit Card', 'PayPal', 'Gift Card', 'Afterpay', 'Cash']
    coupon_codes = [''] + [f'COUPON{str(i).zfill(3)}' for i in range(1, 51)]
    currencies = ['AUD', 'USD', 'EUR']
    # Load valid customer and store ids from generated files
    customers_ids = list(range(1, 80001))
    stores_ids = list(range(1, 5001))
    # Prepare anomaly indices
    num_fk_viol = int(num_orders * 0.01)
    fk_viol_indices = set(random.sample(range(1, num_orders+1), num_fk_viol))
    num_dupes = int(num_orders * 0.0005)
    dupe_indices = set(random.sample(range(1, num_orders+1), num_dupes))
    dupe_order_ids = random.sample(range(1, num_orders+1), num_dupes)
    # Partitioning by order_dt (simulate by sorting at the end if needed)
    order_ids = list(range(1, num_orders+1))
    # Add duplicate order_ids at random positions
    for idx, dupe_id in zip(sorted(dupe_indices), dupe_order_ids):
        order_ids[idx-1] = dupe_id
    with orders_path.open('w', encoding='utf-8') as f:
        f.write('order_id,order_ts,order_dt_local,customer_id,store_id,channel,payment_method,coupon_code,shipping_fee,currency\n')
        for i in range(1, num_orders+1):
            order_id = order_ids[i-1]
            # Partitioning: order_ts and order_dt_local
            order_dt_local = date(2023,1,1) + timedelta(days=random.randint(0, 639))
            order_ts = datetime.combine(order_dt_local, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))
            # Foreign key violation logic
            if i in fk_viol_indices:
                customer_id = random.randint(80001, 90000)
                store_id = random.randint(5001, 6000)
            else:
                customer_id = random.choice(customers_ids)
                store_id = random.choice(stores_ids)
            channel = random.choice(['Retail', 'Online', 'Franchise'])
            payment_method = random.choice(payment_methods)
            coupon_code = random.choice(coupon_codes)
            shipping_fee = round(random.uniform(0, 50), 2)
            currency = random.choice(currencies)
            f.write(f"{order_id},{order_ts.isoformat()}Z,{order_dt_local.isoformat()},{customer_id},{store_id},{channel},{payment_method},{coupon_code},{shipping_fee:.2f},{currency}\n")
    # Generate orders_lines.csv with schema, partitioning, and anomalies
    order_lines_path = out/'orders_lines.csv'
    # Load order_ids and product_ids from generated files
    order_ids = list(range(1, num_orders + 1))
    product_ids = list(range(1, num_products + 1))
    scaled_lines = int(BASE_ORDER_LINES * args.scale)
    num_lines = random.randint(int(scaled_lines * 0.75), scaled_lines)
    # 1% invalid product_ids
    num_invalid_prod = int(num_lines * 0.01)
    invalid_prod_indices = set(random.sample(range(1, num_lines+1), num_invalid_prod))
    # Rare negative quantities or zero prices
    num_neg_qty = max(1, int(num_lines * 0.0005))
    neg_qty_indices = set(random.sample(range(1, num_lines+1), num_neg_qty))
    num_zero_price = max(1, int(num_lines * 0.0005))
    zero_price_indices = set(random.sample(range(1, num_lines+1), num_zero_price))
    with order_lines_path.open('w', encoding='utf-8') as f:
        f.write('order_id,line_number,product_id,qty,unit_price,line_discount_pct,tax_pct\n')
        for i in range(1, num_lines+1):
            order_id = random.choice(order_ids)
            line_number = random.randint(1, 10)
            # Product id anomaly
            if i in invalid_prod_indices:
                product_id = random.randint(25001, 26000)
            else:
                product_id = random.choice(product_ids)
            # Quantity anomaly
            if i in neg_qty_indices:
                qty = -random.randint(1, 5)
            else:
                qty = random.randint(1, 10)
            # Price anomaly
            if i in zero_price_indices:
                unit_price = 0.0
            else:
                unit_price = round(random.uniform(5, 2000), 4)
            line_discount_pct = round(random.uniform(0, 0.5), 4)
            tax_pct = round(random.uniform(0, 0.25), 4)
            f.write(f"{order_id},{line_number},{product_id},{qty},{unit_price:.4f},{line_discount_pct:.4f},{tax_pct:.4f}\n")

    # Generate events.jsonl with schema, partitioning, and anomalies
    events_path = out/'events.jsonl'
    num_events = int(BASE_EVENTS * args.scale)
    event_types = ['click', 'view', 'purchase', 'login', 'logout', 'error']
    # Prepare anomaly indices
    num_malformed = max(1, int(num_events * 0.0005))
    malformed_indices = set(random.sample(range(1, num_events+1), num_malformed))
    num_missing_env = max(1, int(num_events * 0.0005))
    missing_env_indices = set(random.sample(range(1, num_events+1), num_missing_env))
    with events_path.open('w', encoding='utf-8') as f:
        for i in range(1, num_events+1):
            # Envelope
            envelope = {
                "event_id": i,
                "event_ts": (datetime(2023,1,1) + timedelta(seconds=random.randint(0, 60*60*24*730))).isoformat() + 'Z',
                "event_type": random.choice(event_types),
                "user_id": random.randint(1, 100000),
                "session_id": rstr.rstr('[A-Z0-9]{12}')
            }
            # Remove envelope fields for some lines
            if i in missing_env_indices:
                for k in random.sample(list(envelope.keys()), random.randint(1, 3)):
                    envelope.pop(k)
            # Payload
            payload = {}
            if envelope.get("event_type") == "click":
                payload = {"element": random.choice(["button", "link", "image"]), "x": random.randint(0, 1920), "y": random.randint(0, 1080)}
            elif envelope.get("event_type") == "view":
                payload = {"page": random.choice(["home", "product", "cart", "checkout"]), "duration": random.randint(1, 600)}
            elif envelope.get("event_type") == "purchase":
                payload = {"order_id": random.randint(1, 1000000), "amount": round(random.uniform(10, 2000), 2)}
            elif envelope.get("event_type") == "login":
                payload = {"method": random.choice(["email", "google", "facebook"])}
            elif envelope.get("event_type") == "logout":
                payload = {"reason": random.choice(["timeout", "user_action", "error"])}
            elif envelope.get("event_type") == "error":
                payload = {"code": random.randint(100, 599), "message": random.choice(["timeout", "not_found", "server_error"])}
            event_obj = {**envelope, "payload": payload}
            # Malformed JSON anomaly
            if i in malformed_indices:
                line = '{bad_json_line\n'
            else:
                import json
                line = json.dumps(event_obj, separators=(",", ":")) + "\n"
            f.write(line)

    # Generate sensors.csv with schema, partitioning, and anomalies
    sensors_path = out/'sensors.csv'
    scaled_sensors = int(BASE_SENSORS * args.scale)
    num_sensors = random.randint(int(scaled_lines * 0.5), scaled_lines)
    store_ids = list(range(1, 5001))
    shelf_ids = [f'SHELF-{rstr.rstr("[A-Z0-9]{4}")}' for _ in range(100)]
    # Out-of-range anomaly indices
    num_bad_temp = random.randint(int(num_sensors*0.001), int(num_sensors*0.005))
    bad_temp_indices = set(random.sample(range(1, num_sensors+1), num_bad_temp))
    num_bad_hum = random.randint(int(num_sensors*0.001), int(num_sensors*0.005))
    bad_hum_indices = set(random.sample(range(1, num_sensors+1), num_bad_hum))
    # Missing sensor_ts anomaly
    num_missing_ts = random.randint(int(num_sensors*0.0005), int(num_sensors*0.001))
    missing_ts_indices = set(random.sample(range(1, num_sensors+1), num_missing_ts))
    with sensors_path.open('w', encoding='utf-8') as f:
        f.write('sensor_ts,store_id,shelf_id,temperature_c,humidity_pct,battery_mv\n')
        for i in range(1, num_sensors+1):
            # Partitioning: store_id and month
            store_id = random.choice(store_ids)
            month_offset = random.randint(0, 23)
            base_date = datetime(2023, 1, 1) + timedelta(days=month_offset*30)
            ts = base_date + timedelta(minutes=random.randint(0, 43200))
            # Missing sensor_ts anomaly
            if i in missing_ts_indices:
                sensor_ts = ''
            else:
                sensor_ts = ts.isoformat() + 'Z'
            shelf_id = random.choice(shelf_ids)
            # Out-of-range temperature
            if i in bad_temp_indices:
                temperature_c = round(random.choice([-50, 100, 200]), 2)
            else:
                temperature_c = round(random.uniform(-5, 45), 2)
            # Out-of-range humidity
            if i in bad_hum_indices:
                humidity_pct = round(random.choice([-10, 150, 200]), 2)
            else:
                humidity_pct = round(random.uniform(10, 90), 2)
            battery_mv = random.randint(2500, 4200)
            f.write(f"{sensor_ts},{store_id},{shelf_id},{temperature_c},{humidity_pct},{battery_mv}\n")
    
    # Generate exchange_rates.xlsx with schema and 3 years of daily data
    exchange_rates_path = out/'exchange_rates.xlsx'
    start_date = date(2023, 1, 1)
    num_days = 366 + 365 + 365  # 3 years, including leap year
    currencies = ['AUD', 'USD', 'EUR', 'GBP', 'JPY', 'CNY', 'INR', 'NZD', 'CAD', 'SGD']
    # Generate rates for each day and currency
    rows = []
    for i in range(num_days):
        d = start_date + timedelta(days=i)
        for currency in currencies:
            if currency == 'AUD':
                rate = 1.0
            else:
                # Simulate a realistic but random walk for FX rates
                base = {
                    'USD': 0.65, 'EUR': 0.60, 'GBP': 0.53, 'JPY': 95.0, 'CNY': 4.5, 'INR': 54.0, 'NZD': 1.08, 'CAD': 0.88, 'SGD': 0.87
                }[currency]
                # Add some daily random walk
                rate = round(base + np.random.normal(0, base*0.01), 8)
            rows.append((d.isoformat(), currency, f"{rate:.8f}"))
    # Write to XLSX
    wb = xlsxwriter.Workbook(str(exchange_rates_path))
    ws = wb.add_worksheet('exchange_rates')
    ws.write_row(0, 0, ['date', 'currency', 'rate_to_aud'])
    for idx, row in enumerate(rows, 1):
        ws.write_row(idx, 0, row)
    wb.close()
    # Shipments parquet sample
    #tbl = pa.table({
     #   'shipment_id': pa.array(range(1, 10001), type=pa.int64()),
      #  'order_id': pa.array(range(1, 10001), type=pa.int64()),
       # 'carrier': pa.array(['AUSPOST']*10000, type=pa.string()),
        #'shipped_at': pa.array([datetime(2024,1,1)+timedelta(days=i%90) for i in range(10000)], type=pa.timestamp('us')),
        #'delivered_at': pa.array([datetime(2024,1,2)+timedelta(days=i%90) for i in range(10000)], type=pa.timestamp('us')),
        #'ship_cost': pa.array([1995]*10000, type=pa.int64()).cast(pa.decimal128(12,2)),
    #})
    #pq.write_table(tbl, out/'shipments.parquet', compression='snappy')


    # Generate shipments.parquet with schema and anomalies
    shipments_path = out/'shipments.parquet'
    num_shipments = int(BASE_ORDERS * args.scale)  # Same as orders
    carriers = ['AUSPOST', 'TNT', 'DHL', 'FEDEX', 'ARAMEX']
    # Prepare anomaly indices
    num_null_delivered = int(num_shipments * 0.01)
    null_delivered_indices = set(random.sample(range(1, num_shipments+1), num_null_delivered))
    order_ids = list(range(1, 1000001))
    shipment_ids = list(range(1, num_shipments+1))
    # Build order_ts_map for temporal logic
    order_ts_map = {}
    with open(out/'orders_header.csv', encoding='utf-8') as f:
        next(f)
        for line in f:
            parts = line.strip().split(',')
            oid = int(parts[0])
            ots = datetime.fromisoformat(parts[1].replace('Z',''))
            order_ts_map[oid] = ots
    shipped_ats = []
    delivered_ats = []
    ship_costs = []
    carrier_list = []
    order_id_list = []
    num_late = int(num_shipments * 0.01)
    late_indices = set(random.sample(range(1, num_shipments+1), num_late))
    for i in range(1, num_shipments+1):
        order_id = random.choice(order_ids)
        carrier = random.choice(carriers)
        min_ship_dt = order_ts_map[oid] + timedelta(hours=1)
        ship_dt = min_ship_dt + timedelta(hours=random.randint(0, 72), seconds=random.randint(0, 86399))
        if i in null_delivered_indices:
            delivered_at = None
        else:
            if i in late_indices:
                delivered_at = ship_dt + timedelta(days=random.randint(8, 30), seconds=random.randint(0, 86399))
            else:
                delivered_at = ship_dt + timedelta(days=random.randint(1, 7), seconds=random.randint(0, 86399))
        ship_cost = round(random.uniform(5, 200), 2)
        shipped_ats.append(ship_dt)
        delivered_ats.append(delivered_at)
        ship_costs.append(ship_cost)
        carrier_list.append(carrier)
        order_id_list.append(order_id)
    tbl = pa.table({
        'shipment_id': pa.array(shipment_ids, type=pa.int64()),
        'order_id': pa.array(order_id_list, type=pa.int64()),
        'carrier': pa.array(carrier_list, type=pa.string()),
        'shipped_at': pa.array(shipped_ats, type=pa.timestamp('us')),
        'delivered_at': pa.array(delivered_ats, type=pa.timestamp('us')),
        'ship_cost': pa.array(ship_costs).cast(pa.decimal128(12,2)),
    })
    pq.write_table(tbl, shipments_path, compression='snappy')

        # Generate returns Delta format with schema evolution
    import pandas as pd
    try:
        import deltalake as dl
    except ImportError:
        dl = None
    returns_base_path = out/'returns_base.parquet'
    returns_evolved_path = out/'returns_evolved.parquet'
    num_returns = int(BASE_ORDERS * 0.1 * args.scale)  # 10% of orders
    order_ids = list(range(1, 1000001))
    product_ids = list(range(1, 25001))
    reasons = ['damaged', 'wrong_item', 'not_needed', 'late', 'other']
    # Base version
    # Build order_ts_map for returns
    order_ts_map_returns = {}
    with open(out/'orders_header.csv', encoding='utf-8') as f:
        next(f)
        for line in f:
            parts = line.strip().split(',')
            oid = int(parts[0])
            ots = datetime.fromisoformat(parts[1].replace('Z',''))
            order_ts_map_returns[oid] = ots
    base_data = []
    for i in range(1, num_returns+1):
        order_id = random.choice(order_ids)
        product_id = random.choice(product_ids)
        # Return date must be after order_ts
        min_return_ts = order_ts_map_returns[oid] + timedelta(days=2)
        return_ts = min_return_ts + timedelta(days=random.randint(0, 30), seconds=random.randint(0, 86399))
        qty = random.randint(1, 5)
        reason = random.choice(reasons)
        base_data.append((i, order_id, product_id, return_ts, qty, reason))
    df_base = pd.DataFrame(base_data, columns=['return_id','order_id','product_id','return_ts','qty','reason'])
    df_base.to_parquet(returns_base_path, index=False)
    # Evolved version: add return_reason_code
    reason_codes = {'damaged': 'D', 'wrong_item': 'W', 'not_needed': 'N', 'late': 'L', 'other': 'O'}
    df_evolved = df_base.copy()
    df_evolved['return_reason_code'] = df_evolved['reason'].map(reason_codes)
    df_evolved.to_parquet(returns_evolved_path, index=False)
    # Demonstrate UPSERT (update some reasons) and DELETE (remove some rows)
    # UPSERT: update 1% of reasons
    upsert_indices = random.sample(range(num_returns), int(num_returns*0.01))
    for idx in upsert_indices:
        df_evolved.at[idx, 'reason'] = 'updated_reason'
        df_evolved.at[idx, 'return_reason_code'] = 'U'
    # DELETE: remove 0.5% of rows
    delete_indices = set(random.sample(range(num_returns), int(num_returns*0.005)))
    df_evolved = df_evolved.drop(df_evolved.index[list(delete_indices)])
    # Save as a new version
    returns_upsert_path = out/'returns_upsert_delete.parquet'
    df_evolved.to_parquet(returns_upsert_path, index=False)

    print(f"✅ Raw data written to {out}. Expand to required volumes per /docs.")
if __name__ == '__main__':
    main()
