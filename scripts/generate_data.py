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
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)
    fake = Faker('en_AU')

    # Generate stores.csv with schema and anomalies
    stores_path = out/'stores.csv'
    num_stores = 5000
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
    num_suppliers = 8000
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
    num_rows = 80000
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
    num_products = 25000
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
    num_orders = 1000000
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

    print(f"✅ Sample raw written to {out}. Expand to required volumes per /docs.")
if __name__ == '__main__':
    main()
