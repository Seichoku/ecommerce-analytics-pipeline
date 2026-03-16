"""
Generate realistic e-commerce datasets for the analytics pipeline.
Produces: orders, customers, products, order_items CSVs.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

np.random.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.dirname(__file__)

# ── Config ────────────────────────────────────────────────────────────────────
N_CUSTOMERS   = 5_000
N_PRODUCTS    = 200
N_ORDERS      = 20_000
START_DATE    = datetime(2022, 1, 1)
END_DATE      = datetime(2024, 12, 31)

# ── Lookup data ───────────────────────────────────────────────────────────────
CATEGORIES = {
    "Electronics":   {"brands": ["Samsung","Sony","Apple","LG","Bose"],       "price_range": (49,  1499)},
    "Clothing":      {"brands": ["Nike","Adidas","Zara","H&M","Levi's"],      "price_range": (15,  250)},
    "Home & Garden": {"brands": ["IKEA","Dyson","Cuisinart","KitchenAid"],    "price_range": (20,  800)},
    "Sports":        {"brands": ["Nike","Under Armour","Garmin","Titleist"],  "price_range": (10,  600)},
    "Books":         {"brands": ["Penguin","HarperCollins","OReilly"],        "price_range": (8,   60)},
    "Beauty":        {"brands": ["L'Oréal","Clinique","Fenty","Olaplex"],     "price_range": (12,  200)},
    "Toys":          {"brands": ["LEGO","Hasbro","Mattel","Fisher-Price"],    "price_range": (10,  300)},
    "Food & Drink":  {"brands": ["Nespresso","Vitamix","Keurig","Instant Pot"],   "price_range": (15,  450)},
}
CHANNELS    = ["organic_search","paid_search","email","social_media","direct","referral"]
REGIONS     = ["West","East","Central","South","North"]
COUNTRIES   = ["Canada","Canada","Canada","USA","USA","UK"]
STATUSES    = ["completed","completed","completed","completed","returned","cancelled"]
FIRST_NAMES = ["James","Olivia","Liam","Emma","Noah","Ava","William","Sophia","Benjamin","Isabella",
               "Lucas","Mia","Henry","Charlotte","Alexander","Amelia","Mason","Harper","Ethan","Evelyn",
               "Emeka","Chioma","Kwame","Amara","Kofi","Fatima","Yusuf","Aisha","Tariq","Nadia"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Moore",
               "Ichoku","Okafor","Mensah","Diallo","Abubakar","Chen","Kim","Patel","Singh","Nguyen"]

# ── 1. Products ───────────────────────────────────────────────────────────────
print("Generating products...")
products = []
prod_id = 1
for category, info in CATEGORIES.items():
    count = N_PRODUCTS // len(CATEGORIES)
    for _ in range(count):
        brand    = random.choice(info["brands"])
        lo, hi   = info["price_range"]
        cost     = round(np.random.uniform(lo * 0.4, hi * 0.6), 2)
        price    = round(cost * np.random.uniform(1.4, 2.8), 2)
        products.append({
            "product_id":   f"PROD-{prod_id:04d}",
            "product_name": f"{brand} {category} Item {prod_id}",
            "category":     category,
            "brand":        brand,
            "cost_price":   cost,
            "selling_price": price,
            "is_active":    np.random.choice([True, False], p=[0.92, 0.08]),
        })
        prod_id += 1

df_products = pd.DataFrame(products)
df_products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
print(f"  ✓ {len(df_products)} products")

# ── 2. Customers ──────────────────────────────────────────────────────────────
print("Generating customers...")
reg_dates = [START_DATE + timedelta(days=int(d))
             for d in np.random.exponential(400, N_CUSTOMERS)]
reg_dates = [min(d, END_DATE) for d in reg_dates]

customers = []
for i in range(N_CUSTOMERS):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    customers.append({
        "customer_id":        f"CUST-{i+1:05d}",
        "first_name":         fn,
        "last_name":          ln,
        "email":              f"{fn.lower()}.{ln.lower()}{i}@email.com",
        "country":            random.choice(COUNTRIES),
        "region":             random.choice(REGIONS),
        "acquisition_channel":random.choice(CHANNELS),
        "registration_date":  reg_dates[i].date(),
        "is_loyalty_member":  np.random.choice([True, False], p=[0.35, 0.65]),
        "age_group":          np.random.choice(["18-24","25-34","35-44","45-54","55+"],
                                               p=[0.15,0.30,0.25,0.18,0.12]),
    })

df_customers = pd.DataFrame(customers)
df_customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
print(f"  ✓ {len(df_customers)} customers")

# ── 3. Orders ─────────────────────────────────────────────────────────────────
print("Generating orders...")
date_range_days = (END_DATE - START_DATE).days

# Seasonal weights (Q4 heavier)
def seasonal_weight(d):
    m = d.month
    if m in [11, 12]: return 2.5
    if m in [6, 7, 8]: return 1.3
    return 1.0

all_dates = [START_DATE + timedelta(days=int(d))
             for d in np.random.uniform(0, date_range_days, N_ORDERS * 3)]
weights   = [seasonal_weight(d) for d in all_dates]
weights   = np.array(weights) / sum(weights)
chosen_idx = np.random.choice(len(all_dates), size=N_ORDERS, replace=False, p=weights)
order_dates = sorted([all_dates[i] for i in chosen_idx])

# Repeat customer behaviour
cust_ids    = df_customers["customer_id"].tolist()
repeat_custs= random.choices(cust_ids, k=int(N_ORDERS * 0.45))
new_custs   = random.choices(cust_ids, k=N_ORDERS - len(repeat_custs))
all_custs   = repeat_custs + new_custs
random.shuffle(all_custs)

orders = []
for i, (dt, cust) in enumerate(zip(order_dates, all_custs)):
    status    = random.choices(STATUSES, weights=[70,70,70,70,8,12], k=1)[0]
    disc_pct  = np.random.choice([0,0,0,5,10,15,20], p=[0.50,0.10,0.10,0.12,0.10,0.05,0.03])
    ship_days = np.random.randint(1, 8) if status != "cancelled" else None
    orders.append({
        "order_id":        f"ORD-{i+1:06d}",
        "customer_id":     cust,
        "order_date":      dt.date(),
        "order_status":    status,
        "channel":         random.choice(CHANNELS),
        "discount_pct":    disc_pct,
        "shipping_days":   ship_days,
        "is_first_order":  np.random.choice([True, False], p=[0.38, 0.62]),
    })

df_orders = pd.DataFrame(orders)
df_orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
print(f"  ✓ {len(df_orders)} orders")

# ── 4. Order Items ────────────────────────────────────────────────────────────
print("Generating order items...")
prod_ids    = df_products["product_id"].tolist()
prod_prices = dict(zip(df_products["product_id"], df_products["selling_price"]))
prod_costs  = dict(zip(df_products["product_id"], df_products["cost_price"]))

items = []
item_id = 1
for _, order in df_orders.iterrows():
    n_items = np.random.choice([1,2,3,4,5], p=[0.50,0.25,0.13,0.07,0.05])
    chosen_prods = random.sample(prod_ids, min(n_items, len(prod_ids)))
    for prod in chosen_prods:
        qty      = np.random.randint(1, 4)
        price    = prod_prices[prod]
        cost     = prod_costs[prod]
        disc_amt = round(price * order["discount_pct"] / 100, 2)
        items.append({
            "order_item_id":   f"ITEM-{item_id:07d}",
            "order_id":        order["order_id"],
            "product_id":      prod,
            "quantity":        qty,
            "unit_price":      price,
            "unit_cost":       cost,
            "discount_amount": disc_amt,
            "line_total":      round((price - disc_amt) * qty, 2),
            "line_profit":     round(((price - disc_amt) - cost) * qty, 2),
        })
        item_id += 1

df_items = pd.DataFrame(items)
df_items.to_csv(f"{OUTPUT_DIR}/order_items.csv", index=False)
print(f"  ✓ {len(df_items)} order items")
print("\nAll datasets generated successfully.")
print(f"  Total revenue (gross): ${df_items['line_total'].sum():,.2f}")
print(f"  Total profit:          ${df_items['line_profit'].sum():,.2f}")
