"""
Database Creation Script for TechStore Data Warehouse
Creates Star Schema in SQLite from transformed and extracted data
Author: Database Architect - TechStore BI Project
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import re

print("="*70)
print("TECHSTORE DATA WAREHOUSE - DATABASE CREATION")
print("="*70)

# ============================================
# STEP 1: LOAD ALL DATA
# ============================================
print("\n[1/6] Loading data from CSV and Excel files...")

# Load transformed data
dim_product_raw = pd.read_csv('../Data/transformed/dim_product.csv')
fact_sales_raw = pd.read_csv('../Data/transformed/fact_sales_final.csv')

# Load extracted data
stores = pd.read_csv('../Data/extracted/stores.csv')
customers = pd.read_csv('../Data/extracted/customers.csv')
cities = pd.read_csv('../Data/extracted/cities.csv')
categories = pd.read_csv('../Data/extracted/categories.csv')
subcategories = pd.read_csv('../Data/extracted/subcategories.csv')

# Load flat files
monthly_targets = pd.read_excel('../Data/flat_files/monthly_targets.xlsx')
marketing_expenses = pd.read_excel('../Data/flat_files/marketing_expenses.xlsx')
shipping_rates = pd.read_excel('../Data/flat_files/shipping_rates.xlsx')

# Try to load competitor prices if exists
try:
    competitor_prices = pd.read_csv('../Data/extracted/competitor_prices.csv')
    has_competitor_data = True
    print("  ✓ Competitor prices loaded")
except:
    has_competitor_data = False
    print("  ⚠ Competitor prices not found (will be NULL in database)")

print(f"  ✓ Loaded {len(dim_product_raw)} products")
print(f"  ✓ Loaded {len(fact_sales_raw)} sales transactions")
print(f"  ✓ Loaded {len(stores)} stores")
print(f"  ✓ Loaded {len(customers)} customers")

# ============================================
# STEP 2: BUILD DIMENSION TABLES
# ============================================
print("\n[2/6] Building Dimension Tables...")

# --- Dim_Product ---
print("  Building Dim_Product...")

# Join with subcategories and categories to get names
dim_product = dim_product_raw.copy()
dim_product = dim_product.merge(
    subcategories[['SubCat_ID', 'SubCat_Name', 'Category_ID']], 
    left_on='subcat_id', 
    right_on='SubCat_ID', 
    how='left'
)
dim_product = dim_product.merge(
    categories[['Category_ID', 'Category_Name']], 
    on='Category_ID', 
    how='left'
)

# Add competitor prices if available
if has_competitor_data:
    dim_product = dim_product.merge(
        competitor_prices[['product_id', 'competitor_price']], 
        on='product_id', 
        how='left'
    )
else:
    dim_product['competitor_price'] = None

# Select and rename final columns
dim_product = dim_product[[
    'product_id', 'product_name', 'SubCat_Name', 'Category_Name',
    'unit_cost', 'sentiment_score', 'competitor_price'
]].rename(columns={
    'product_id': 'Product_ID',
    'product_name': 'Product_Name',
    'SubCat_Name': 'Subcategory_Name',
    'Category_Name': 'Category_Name',
    'unit_cost': 'Unit_Cost',
    'sentiment_score': 'Sentiment_Score',
    'competitor_price': 'Competitor_Price'
})

print(f"    ✓ Dim_Product: {len(dim_product)} rows")

# --- Dim_Store ---
print("  Building Dim_Store...")

# Merge stores with cities
dim_store = stores.merge(cities, on='City_ID', how='left')

# Fix Store_ID in monthly_targets - handle multiple formats
def clean_store_id(store_id):
    """Extract numeric ID from various formats: 'S1', 'Store_5', '3', etc."""
    store_id_str = str(store_id).strip()
    # Remove any non-numeric characters and extract the number
    numeric_part = ''.join(filter(str.isdigit, store_id_str))
    return int(numeric_part) if numeric_part else None

print(f"    Cleaning Store_ID format in monthly_targets...")
print(f"    Sample original Store_IDs: {monthly_targets['Store_ID'].head().tolist()}")

monthly_targets['Store_ID_Clean'] = monthly_targets['Store_ID'].apply(clean_store_id)

# Remove any rows where we couldn't extract a valid Store_ID
before_clean = len(monthly_targets)
monthly_targets = monthly_targets.dropna(subset=['Store_ID_Clean'])
monthly_targets['Store_ID_Clean'] = monthly_targets['Store_ID_Clean'].astype(int)
after_clean = len(monthly_targets)

if before_clean != after_clean:
    print(f"    ⚠ Removed {before_clean - after_clean} rows with invalid Store_ID")

print(f"    Sample cleaned Store_IDs: {monthly_targets['Store_ID_Clean'].head().tolist()}")

# Convert Target_Revenue to numeric (it might be stored as string)
monthly_targets['Target_Revenue_Numeric'] = pd.to_numeric(
    monthly_targets['Target_Revenue'], 
    errors='coerce'
)

# Calculate average target per store (since we have monthly targets)
avg_targets = monthly_targets.groupby('Store_ID_Clean')['Target_Revenue_Numeric'].mean().reset_index()
avg_targets.columns = ['Store_ID', 'Monthly_Target']

print(f"    Calculated targets for {len(avg_targets)} stores")

# Merge with stores
dim_store = dim_store.merge(avg_targets, on='Store_ID', how='left')

# Select final columns
dim_store = dim_store[[
    'Store_ID', 'Store_Name', 'City_Name', 'Region', 'Monthly_Target'
]]

print(f"    ✓ Dim_Store: {len(dim_store)} rows")

# --- Dim_Customer ---
print("  Building Dim_Customer...")

dim_customer = customers.merge(cities, on='City_ID', how='left')
dim_customer = dim_customer[[
    'Customer_ID', 'Full_Name', 'City_Name', 'Region'
]].rename(columns={'Full_Name': 'Customer_Name'})

print(f"    ✓ Dim_Customer: {len(dim_customer)} rows")

# --- Dim_Date ---
print("  Building Dim_Date...")

# Extract unique dates from fact_sales
fact_sales_raw['date_parsed'] = pd.to_datetime(fact_sales_raw['date'])
unique_dates = fact_sales_raw['date_parsed'].dt.date.unique()

# Create date dimension
dim_date = pd.DataFrame({'Full_Date': pd.to_datetime(unique_dates)})
dim_date['Date_ID'] = dim_date['Full_Date'].dt.strftime('%Y%m%d').astype(int)
dim_date['Day'] = dim_date['Full_Date'].dt.day
dim_date['Month'] = dim_date['Full_Date'].dt.month
dim_date['Year'] = dim_date['Full_Date'].dt.year
dim_date['Quarter'] = dim_date['Full_Date'].dt.quarter

dim_date = dim_date[['Date_ID', 'Full_Date', 'Day', 'Month', 'Year', 'Quarter']]
dim_date = dim_date.sort_values('Date_ID').reset_index(drop=True)

print(f"    ✓ Dim_Date: {len(dim_date)} rows")
print(f"    Date range: {dim_date['Full_Date'].min()} to {dim_date['Full_Date'].max()}")

# ============================================
# STEP 3: BUILD FACT TABLE
# ============================================
print("\n[3/6] Building Fact Table...")

# Prepare fact_sales
fact_sales = fact_sales_raw.copy()

# Add Date_ID
fact_sales['date_parsed'] = pd.to_datetime(fact_sales['date'])
fact_sales['Date_ID'] = fact_sales['date_parsed'].dt.strftime('%Y%m%d').astype(int)

# Rename columns to match schema
fact_sales_final = fact_sales[[
    'trans_id', 'Date_ID', 'product_id', 'store_id', 'customer_id',
    'quantity', 'total_revenue', 'product_total_cost', 
    'shipping_cost', 'allocated_marketing_dzd', 'net_profit'
]].rename(columns={
    'trans_id': 'Sale_ID',
    'product_id': 'Product_ID',
    'store_id': 'Store_ID',
    'customer_id': 'Customer_ID',
    'quantity': 'Quantity',
    'total_revenue': 'Total_Revenue',
    'product_total_cost': 'Product_Cost',
    'shipping_cost': 'Shipping_Cost',
    'allocated_marketing_dzd': 'Marketing_Cost',
    'net_profit': 'Net_Profit'
})

print(f"  ✓ Fact_Sales: {len(fact_sales_final)} rows")

# ============================================
# STEP 4: CREATE SQLITE DATABASE
# ============================================
print("\n[4/6] Creating SQLite Database...")

db_path = '../database/techstore_dw.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print(f"  ✓ Connected to: {db_path}")

# Create tables
print("\n  Creating tables...")

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Date (
    Date_ID INTEGER PRIMARY KEY,
    Full_Date DATE NOT NULL,
    Day INTEGER,
    Month INTEGER,
    Year INTEGER,
    Quarter INTEGER
)
''')
print("    ✓ Dim_Date table created")

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Product (
    Product_ID TEXT PRIMARY KEY,
    Product_Name TEXT NOT NULL,
    Subcategory_Name TEXT,
    Category_Name TEXT,
    Unit_Cost REAL,
    Sentiment_Score REAL,
    Competitor_Price REAL
)
''')
print("    ✓ Dim_Product table created")

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Store (
    Store_ID INTEGER PRIMARY KEY,
    Store_Name TEXT NOT NULL,
    City_Name TEXT,
    Region TEXT,
    Monthly_Target REAL
)
''')
print("    ✓ Dim_Store table created")

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Customer (
    Customer_ID TEXT PRIMARY KEY,
    Customer_Name TEXT NOT NULL,
    City_Name TEXT,
    Region TEXT
)
''')
print("    ✓ Dim_Customer table created")

cursor.execute('''
CREATE TABLE IF NOT EXISTS Fact_Sales (
    Sale_ID INTEGER PRIMARY KEY,
    Date_ID INTEGER NOT NULL,
    Product_ID TEXT NOT NULL,
    Store_ID INTEGER NOT NULL,
    Customer_ID TEXT NOT NULL,
    Quantity INTEGER,
    Total_Revenue REAL,
    Product_Cost REAL,
    Shipping_Cost REAL,
    Marketing_Cost REAL,
    Net_Profit REAL,
    FOREIGN KEY (Date_ID) REFERENCES Dim_Date(Date_ID),
    FOREIGN KEY (Product_ID) REFERENCES Dim_Product(Product_ID),
    FOREIGN KEY (Store_ID) REFERENCES Dim_Store(Store_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Dim_Customer(Customer_ID)
)
''')
print("    ✓ Fact_Sales table created")

conn.commit()

# ============================================
# STEP 5: LOAD DATA INTO TABLES
# ============================================
print("\n[5/6] Loading data into database...")

# Load dimensions first (for foreign key integrity)
dim_date.to_sql('Dim_Date', conn, if_exists='replace', index=False)
print(f"  ✓ Dim_Date: {len(dim_date)} rows loaded")

dim_product.to_sql('Dim_Product', conn, if_exists='replace', index=False)
print(f"  ✓ Dim_Product: {len(dim_product)} rows loaded")

dim_store.to_sql('Dim_Store', conn, if_exists='replace', index=False)
print(f"  ✓ Dim_Store: {len(dim_store)} rows loaded")

dim_customer.to_sql('Dim_Customer', conn, if_exists='replace', index=False)
print(f"  ✓ Dim_Customer: {len(dim_customer)} rows loaded")

# Load fact table last
fact_sales_final.to_sql('Fact_Sales', conn, if_exists='replace', index=False)
print(f"  ✓ Fact_Sales: {len(fact_sales_final)} rows loaded")

# ============================================
# STEP 6: VERIFY DATABASE
# ============================================
print("\n[6/6] Verifying database integrity...")

tables = ['Dim_Date', 'Dim_Product', 'Dim_Store', 'Dim_Customer', 'Fact_Sales']
verification_results = {}

for table in tables:
    count_query = f"SELECT COUNT(*) as count FROM {table}"
    result = pd.read_sql(count_query, conn)
    count = result['count'][0]
    verification_results[table] = count
    print(f"  ✓ {table}: {count:,} rows")

# Run a test join query
print("\n  Running test query (Top 5 Sales with full details)...")
test_query = """
SELECT 
    fs.Sale_ID,
    dd.Full_Date,
    dp.Product_Name,
    dp.Category_Name,
    ds.Store_Name,
    dc.Customer_Name,
    fs.Quantity,
    ROUND(fs.Total_Revenue, 2) as Revenue,
    ROUND(fs.Net_Profit, 2) as Profit
FROM Fact_Sales fs
JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
JOIN Dim_Customer dc ON fs.Customer_ID = dc.Customer_ID
ORDER BY fs.Total_Revenue DESC
LIMIT 5
"""

test_result = pd.read_sql(test_query, conn)
print("\n" + test_result.to_string(index=False))

# Calculate some quick stats
stats_query = """
SELECT 
    COUNT(DISTINCT Product_ID) as Total_Products,
    COUNT(DISTINCT Store_ID) as Total_Stores,
    COUNT(DISTINCT Customer_ID) as Total_Customers,
    COUNT(*) as Total_Transactions,
    ROUND(SUM(Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(Net_Profit), 2) as Total_Net_Profit,
    ROUND(AVG(Total_Revenue), 2) as Avg_Transaction_Value
FROM Fact_Sales
"""

stats = pd.read_sql(stats_query, conn)
print("\n" + "="*70)
print("DATABASE STATISTICS")
print("="*70)
print(stats.to_string(index=False))

# Close connection
conn.close()

print("\n" + "="*70)
print("✓ DATABASE CREATION COMPLETED SUCCESSFULLY!")
print("="*70)
print(f"\nDatabase saved to: {db_path}")
print(f"Total size: {len(fact_sales_final):,} sales transactions")
print(f"Date range: {fact_sales_raw['date_parsed'].min()} to {fact_sales_raw['date_parsed'].max()}")
print("\nYou can now:")
print("  1. Use this database in your dashboard_app.py")
print("  2. Run SQL queries for analytics")
print("  3. Share with your Dashboard Developer teammate")
print("="*70)