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

    # Minimal sample generation (expand to full volumes per docs)
    fake = Faker('en_AU')
    customers_path = out/'customers.csv'
    num_rows = 80000
    num_malformed = random.randint(int(num_rows*0.005), int(num_rows*0.01))  # 0.5-1% malformed emails
    
    # Calculate number of unique keys and duplicates
    num_duplicates = int(num_rows*0.002)  # 0.2% will be duplicates
    num_unique_keys = num_rows - num_duplicates
    
    # Generate base unique natural_keys
    natural_keys = []
    while len(natural_keys) < num_unique_keys:
        key = 'CUST-' + rstr.rstr('A-Z0-9', 8)
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
    with products_path.open('w', encoding='utf-8') as f:
        f.write('product_id,sku,name,category,subcategory,current_price,currency,is_discontinued,introduced_dt,discontinued_dt\n')
        for i in range(1, num_products+1):
            # Ensure sku is unique and matches SKU-[A-Z0-9]{6}
            while True:
                sku = 'SKU-' + rstr.rstr('A-Z0-9', 6)
                if sku not in used_skus:
                    used_skus.add(sku)
                    break
            name = fake.word().capitalize() + ' ' + fake.word().capitalize()
            category = random.choice(categories)
            subcategory = random.choice(subcategories[category])
            price = round(random.uniform(5, 2000), 4)
            currency = random.choice(currencies)
            is_discontinued = random.random() < 0.1
            intro_dt = date(2015,1,1) + timedelta(days=random.randint(0, 365*8))
            if is_discontinued:
                disc_dt = intro_dt + timedelta(days=random.randint(30, 2000))
                if disc_dt > date.today():
                    disc_dt = ''
                else:
                    disc_dt = disc_dt.isoformat()
            else:
                disc_dt = ''
            f.write(f"{i},{sku},{name},{category},{subcategory},{price:.4f},{currency},{str(is_discontinued)},{intro_dt.isoformat()},{disc_dt}\n")


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
