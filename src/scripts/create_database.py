"""
Database Creation Script for TechStore Data Warehouse
Loads pre-transformed Star Schema tables into SQLite
Author: Database Architect - TechStore BI Project
Date: January 2025
"""

import sqlite3
import pandas as pd
from datetime import datetime

print("="*70)
print("TECHSTORE DATA WAREHOUSE - DATABASE CREATION")
print("Star Schema: 1 Fact Table + 4 Dimension Tables")
print("="*70)

# ============================================
# STEP 1: LOAD TRANSFORMED STAR SCHEMA FILES
# ============================================
print("\n[1/4] Loading pre-transformed Star Schema files...")

try:
    # Load all dimension tables
    dim_customer_raw = pd.read_csv('../Data/transformed/Dim_Customer.csv')
    dim_date_raw = pd.read_csv('../Data/transformed/Dim_Date.csv')
    dim_product_raw = pd.read_csv('../Data/transformed/Dim_Product.csv')
    dim_store_raw = pd.read_csv('../Data/transformed/Dim_Store.csv')
    
    # Load fact table
    fact_sales_raw = pd.read_csv('../Data/transformed/Fact_Sales.csv')
    
    print(f"  ✓ Dim_Customer: {len(dim_customer_raw):,} rows loaded")
    print(f"  ✓ Dim_Date: {len(dim_date_raw):,} rows loaded")
    print(f"  ✓ Dim_Product: {len(dim_product_raw):,} rows loaded")
    print(f"  ✓ Dim_Store: {len(dim_store_raw):,} rows loaded")
    print(f"  ✓ Fact_Sales: {len(fact_sales_raw):,} rows loaded")
    
except FileNotFoundError as e:
    print(f"\n  ✗ ERROR: Could not find transformed files!")
    print(f"  {e}")
    print("\n  Please ensure the Star Schema files exist in:")
    print("  ../Data/transformed/")
    exit(1)

# ============================================
# STEP 2: TRANSFORM TO MATCH SCHEMA
# ============================================
print("\n[2/4] Preparing data for Star Schema...")

# --- Dim_Customer ---
print("  Preparing Dim_Customer...")
Dim_Customer = dim_customer_raw[[
    'customer_id', 'full_name', 'city_name', 'region'
]].rename(columns={
    'customer_id': 'Customer_ID',
    'full_name': 'Customer_Name',
    'city_name': 'City_Name',
    'region': 'Region'
})
print(f"    ✓ {len(Dim_Customer):,} customers ready")

# --- Dim_Product ---
print("  Preparing Dim_Product...")
Dim_Product = dim_product_raw[[
    'product_id', 'product_name', 'subcat_name', 'category_name',
    'unit_cost', 'avg_sentiment', 'competitor_price'
]].rename(columns={
    'product_id': 'Product_ID',
    'product_name': 'Product_Name',
    'subcat_name': 'Subcategory_Name',
    'category_name': 'Category_Name',
    'unit_cost': 'Unit_Cost',
    'avg_sentiment': 'Sentiment_Score',
    'competitor_price': 'Competitor_Price'
})
print(f"    ✓ {len(Dim_Product):,} products ready")

# --- Dim_Store ---
print("  Preparing Dim_Store...")
# Note: target_revenue appears to be ANNUAL (very large numbers)
# We'll store as-is and calculate monthly in queries if needed
Dim_Store = dim_store_raw[[
    'store_id', 'store_name', 'city_name', 'region', 'target_revenue'
]].rename(columns={
    'store_id': 'Store_ID',
    'store_name': 'Store_Name',
    'city_name': 'City_Name',
    'region': 'Region',
    'target_revenue': 'Annual_Target'
})
# Add Monthly_Target for consistency with queries
Dim_Store['Monthly_Target'] = Dim_Store['Annual_Target'] / 12
print(f"    ✓ {len(Dim_Store):,} stores ready")

# --- Dim_Date ---
print("  Preparing Dim_Date...")
# Parse dates and create Date_ID
dim_date_raw['date_parsed'] = pd.to_datetime(dim_date_raw['date'])
dim_date_raw['Date_ID'] = dim_date_raw['date_parsed'].dt.strftime('%Y%m%d').astype(int)

# Remove duplicate Date_IDs (keep first occurrence)
dim_date_unique = dim_date_raw.drop_duplicates(subset=['Date_ID'], keep='first')

Dim_Date = pd.DataFrame({
    'Date_ID': dim_date_unique['Date_ID'],
    'Full_Date': dim_date_unique['date_parsed'].dt.date,
    'Day': dim_date_unique['day'],
    'Month': dim_date_unique['month'],
    'Year': dim_date_unique['year'],
    'Quarter': dim_date_unique['quarter']
}).sort_values('Date_ID').reset_index(drop=True)

print(f"    ✓ {len(Dim_Date):,} unique dates ready")
print(f"    Date range: {Dim_Date['Full_Date'].min()} to {Dim_Date['Full_Date'].max()}")

# --- Fact_Sales ---
print("  Preparing Fact_Sales...")
# Parse date and create Date_ID
fact_sales_raw['date_parsed'] = pd.to_datetime(fact_sales_raw['date'])
fact_sales_raw['Date_ID'] = fact_sales_raw['date_parsed'].dt.strftime('%Y%m%d').astype(int)

Fact_Sales = fact_sales_raw[[
    'trans_id', 'Date_ID', 'product_id', 'store_id', 'customer_id',
    'quantity', 'total_revenue', 'cost', 
    'shipping_cost_total', 'allocated_marketing_dzd', 'net_profit'
]].rename(columns={
    'trans_id': 'Sale_ID',
    'product_id': 'Product_ID',
    'store_id': 'Store_ID',
    'customer_id': 'Customer_ID',
    'quantity': 'Quantity',
    'total_revenue': 'Total_Revenue',
    'cost': 'Product_Cost',
    'shipping_cost_total': 'Shipping_Cost',
    'allocated_marketing_dzd': 'Marketing_Cost',
    'net_profit': 'Net_Profit'
})

print(f"    ✓ {len(Fact_Sales):,} transactions ready")

# ============================================
# STEP 3: CREATE SQLITE DATABASE
# ============================================
print("\n[3/4] Creating SQLite Data Warehouse...")

db_path = '../database/techstore_dw.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print(f"  ✓ Connected to: {db_path}")

# Drop existing tables for clean rebuild
print("\n  Dropping existing tables (if any)...")
tables_to_drop = ['Fact_Sales', 'Dim_Date', 'Dim_Product', 'Dim_Store', 'Dim_Customer']
for table in tables_to_drop:
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
conn.commit()
print("  ✓ Old tables dropped")

# Create Star Schema tables
print("\n  Creating Star Schema tables...")

cursor.execute('''
CREATE TABLE Dim_Date (
    Date_ID INTEGER PRIMARY KEY,
    Full_Date DATE NOT NULL,
    Day INTEGER,
    Month INTEGER,
    Year INTEGER,
    Quarter INTEGER
)
''')
print("    ✓ Dim_Date created")

cursor.execute('''
CREATE TABLE Dim_Product (
    Product_ID TEXT PRIMARY KEY,
    Product_Name TEXT NOT NULL,
    Subcategory_Name TEXT,
    Category_Name TEXT,
    Unit_Cost REAL,
    Sentiment_Score REAL,
    Competitor_Price REAL
)
''')
print("    ✓ Dim_Product created")

cursor.execute('''
CREATE TABLE Dim_Store (
    Store_ID INTEGER PRIMARY KEY,
    Store_Name TEXT NOT NULL,
    City_Name TEXT,
    Region TEXT,
    Annual_Target REAL,
    Monthly_Target REAL
)
''')
print("    ✓ Dim_Store created")

cursor.execute('''
CREATE TABLE Dim_Customer (
    Customer_ID TEXT PRIMARY KEY,
    Customer_Name TEXT NOT NULL,
    City_Name TEXT,
    Region TEXT
)
''')
print("    ✓ Dim_Customer created")

cursor.execute('''
CREATE TABLE Fact_Sales (
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
print("    ✓ Fact_Sales created")

conn.commit()

# Load data into tables (dimensions first, fact last)
print("\n  Loading data into tables...")
print("    Loading dimensions first (for referential integrity)...")

Dim_Date.to_sql('Dim_Date', conn, if_exists='append', index=False)
print(f"      ✓ Dim_Date: {len(Dim_Date):,} rows inserted")

Dim_Product.to_sql('Dim_Product', conn, if_exists='append', index=False)
print(f"      ✓ Dim_Product: {len(Dim_Product):,} rows inserted")

Dim_Store.to_sql('Dim_Store', conn, if_exists='append', index=False)
print(f"      ✓ Dim_Store: {len(Dim_Store):,} rows inserted")

Dim_Customer.to_sql('Dim_Customer', conn, if_exists='append', index=False)
print(f"      ✓ Dim_Customer: {len(Dim_Customer):,} rows inserted")

print("    Loading fact table...")
Fact_Sales.to_sql('Fact_Sales', conn, if_exists='append', index=False)
print(f"      ✓ Fact_Sales: {len(Fact_Sales):,} rows inserted")

conn.commit()
print("\n  ✓ All data loaded successfully!")

# ============================================
# STEP 4: VERIFY DATABASE INTEGRITY
# ============================================
print("\n[4/4] Verifying database integrity...")

# Row counts
print("\n  Table Row Counts:")
print("  " + "-"*60)
for table in ['Dim_Date', 'Dim_Product', 'Dim_Store', 'Dim_Customer', 'Fact_Sales']:
    count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", conn)['cnt'][0]
    print(f"    {table:20} {count:>10,} rows")
print("  " + "-"*60)

# Test Star Schema joins
print("\n  Testing Star Schema joins (Top 5 Sales by Revenue)...")
print("  " + "-"*60)

test_query = """
SELECT 
    fs.Sale_ID,
    dd.Full_Date,
    dp.Product_Name,
    dp.Category_Name,
    ds.Store_Name,
    ds.City_Name as Store_City,
    ds.Region,
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

try:
    test_result = pd.read_sql(test_query, conn)
    print(test_result.to_string(index=False))
    print("\n  ✓ Star Schema joins working correctly!")
except Exception as e:
    print(f"  ⚠ Join test failed: {e}")

# Business metrics summary
print("\n  Business Metrics Summary:")
print("  " + "-"*60)

summary_query = """
SELECT 
    COUNT(DISTINCT Product_ID) as Unique_Products,
    COUNT(DISTINCT Store_ID) as Unique_Stores,
    COUNT(DISTINCT Customer_ID) as Unique_Customers,
    COUNT(*) as Total_Transactions,
    ROUND(SUM(Total_Revenue)/1000000, 2) as Total_Revenue_M_DZD,
    ROUND(SUM(Net_Profit)/1000000, 2) as Total_Profit_M_DZD,
    ROUND(SUM(Net_Profit) * 100.0 / SUM(Total_Revenue), 2) as Profit_Margin_Pct,
    ROUND(AVG(Total_Revenue), 2) as Avg_Transaction_Value
FROM Fact_Sales
"""

summary = pd.read_sql(summary_query, conn)
print(summary.to_string(index=False))

# Date range
date_range_query = "SELECT MIN(Full_Date) as Start_Date, MAX(Full_Date) as End_Date FROM Dim_Date"
date_range = pd.read_sql(date_range_query, conn)
print(f"\n  Date Range: {date_range['Start_Date'][0]} to {date_range['End_Date'][0]}")

# Category breakdown
print("\n  Revenue by Category:")
print("  " + "-"*60)
category_query = """
SELECT 
    dp.Category_Name,
    COUNT(*) as Transactions,
    ROUND(SUM(fs.Total_Revenue)/1000000, 2) as Revenue_M_DZD,
    ROUND(SUM(fs.Net_Profit)/1000000, 2) as Profit_M_DZD,
    ROUND(SUM(fs.Net_Profit)*100.0/SUM(fs.Total_Revenue), 2) as Margin_Pct
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
GROUP BY dp.Category_Name
ORDER BY Revenue_M_DZD DESC
"""
category_breakdown = pd.read_sql(category_query, conn)
print(category_breakdown.to_string(index=False))

# Store performance
print("\n  Top 3 Stores by Revenue:")
print("  " + "-"*60)
store_query = """
SELECT 
    ds.Store_Name,
    ds.Region,
    COUNT(*) as Transactions,
    ROUND(SUM(fs.Total_Revenue)/1000000, 2) as Revenue_M_DZD,
    ROUND(SUM(fs.Net_Profit)/1000000, 2) as Profit_M_DZD
FROM Fact_Sales fs
JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
GROUP BY ds.Store_ID, ds.Store_Name, ds.Region
ORDER BY Revenue_M_DZD DESC
LIMIT 3
"""
top_stores = pd.read_sql(store_query, conn)
print(top_stores.to_string(index=False))

# Close connection
conn.close()

# ============================================
# FINAL SUMMARY
# ============================================
print("\n" + "="*70)
print("✓ DATA WAREHOUSE CREATION COMPLETED SUCCESSFULLY!")
print("="*70)
print(f"\n📊 Database File: {db_path}")
print(f"📅 Date Range: {date_range['Start_Date'][0]} to {date_range['End_Date'][0]}")
print(f"💰 Total Revenue: {summary['Total_Revenue_M_DZD'][0]:,.2f} Million DZD")
print(f"💵 Total Profit: {summary['Total_Profit_M_DZD'][0]:,.2f} Million DZD")
print(f"📈 Profit Margin: {summary['Profit_Margin_Pct'][0]:.2f}%")
print(f"🏪 Stores: {summary['Unique_Stores'][0]}")
print(f"📦 Products: {summary['Unique_Products'][0]}")
print(f"👥 Customers: {summary['Unique_Customers'][0]}")
print(f"🧾 Transactions: {summary['Total_Transactions'][0]:,}")

print("\n" + "="*70)
print("NEXT STEPS")
print("="*70)
print("✅ 1. Database is ready for dashboard development (Member 4)")
print("✅ 2. Test your SQL queries: python test_queries.py")
print("✅ 3. Take screenshots for your report")
print("✅ 4. Share techstore_dw.db with your Dashboard Developer")
print("="*70)