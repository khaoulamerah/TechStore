import sqlite3
import pandas as pd
import os
from pathlib import Path

print("="*70)
print("TECHSTORE DATA WAREHOUSE - DATABASE LOADING")
print("Star Schema: 1 Fact Table + 4 Dimension Tables")
print("="*70)

base_dir = Path(__file__).parent if '__file__' in globals() else Path.cwd()

if base_dir.name == 'scripts':
    project_root = base_dir.parent.parent
elif base_dir.name == 'src':
    project_root = base_dir.parent
else:
    project_root = base_dir

transformed_dir = project_root / 'src' / 'Data' / 'transformed'
database_dir = project_root / 'src' / 'database'
database_dir.mkdir(parents=True, exist_ok=True)
db_path = database_dir / 'techstore_dw.db'

print("\n[1/4] Loading transformed Star Schema files...")

try:
    dim_customer_raw = pd.read_csv(transformed_dir / 'Dim_Customer.csv')
    dim_date_raw = pd.read_csv(transformed_dir / 'Dim_Date.csv')
    dim_product_raw = pd.read_csv(transformed_dir / 'Dim_Product.csv')
    dim_store_raw = pd.read_csv(transformed_dir / 'Dim_Store.csv')
    fact_sales_raw = pd.read_csv(transformed_dir / 'Fact_Sales.csv')
    
    print(f"  Dim_Customer: {len(dim_customer_raw):,} rows loaded")
    print(f"  Dim_Date: {len(dim_date_raw):,} rows loaded")
    print(f"  Dim_Product: {len(dim_product_raw):,} rows loaded")
    print(f"  Dim_Store: {len(dim_store_raw):,} rows loaded")
    print(f"  Fact_Sales: {len(fact_sales_raw):,} rows loaded")
    
except FileNotFoundError as e:
    print(f"\n  ERROR: Missing transformed file!")
    print(f"  {e}")
    print(f"  Ensure all files are in: {transformed_dir}")
    exit(1)

print("\n[2/4] Standardizing column names for database...")

def standardize_columns(df):
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

dim_customer_raw = standardize_columns(dim_customer_raw)
dim_date_raw = standardize_columns(dim_date_raw)
dim_product_raw = standardize_columns(dim_product_raw)
dim_store_raw = standardize_columns(dim_store_raw)
fact_sales_raw = standardize_columns(fact_sales_raw)

Dim_Customer = dim_customer_raw.rename(columns={
    'customer_id': 'Customer_ID',
    'full_name': 'Customer_Name',
    'city_name': 'City_Name',
    'region': 'Region'
})[[  'Customer_ID', 'Customer_Name', 'City_Name', 'Region'
]]

Dim_Product = dim_product_raw.rename(columns={
    'product_id': 'Product_ID',
    'product_name': 'Product_Name',
    'subcategory_name': 'Subcategory_Name',
    'category_name': 'Category_Name',
    'unit_cost': 'Unit_Cost',
    'avg_sentiment': 'Sentiment_Score',
    'competitor_price': 'Competitor_Price'
})[[
    'Product_ID', 'Product_Name', 'Subcategory_Name', 'Category_Name',
    'Unit_Cost', 'Sentiment_Score', 'Competitor_Price'
]]

Dim_Store = dim_store_raw.rename(columns={
    'store_id': 'Store_ID',
    'store_name': 'Store_Name',
    'city_name': 'City_Name',
    'region': 'Region',
    'monthly_target': 'Monthly_Target',
    'annual_target': 'Annual_Target'
})[[
    'Store_ID', 'Store_Name', 'City_Name', 'Region',
    'Monthly_Target', 'Annual_Target'
]]

Dim_Date = dim_date_raw.rename(columns={
    'date_id': 'Date_ID',
    'date': 'Full_Date',
    'year': 'Year',
    'quarter': 'Quarter',
    'month': 'Month',
    'month_name': 'Month_Name',
    'day': 'Day',
    'day_of_week': 'Day_Of_Week',
    'day_name': 'Day_Name',
    'week_of_year': 'Week_Of_Year'
})[[
    'Date_ID', 'Full_Date', 'Year', 'Quarter', 'Month', 'Month_Name',
    'Day', 'Day_Of_Week', 'Day_Name', 'Week_Of_Year'
]]

fact_sales_raw['date'] = pd.to_datetime(fact_sales_raw['date'], errors='coerce')
fact_sales_raw['date_id'] = fact_sales_raw['date'].dt.strftime('%Y%m%d').astype(int)

Fact_Sales = fact_sales_raw.rename(columns={
    'trans_id': 'Sale_ID',
    'date_id': 'Date_ID',
    'product_id': 'Product_ID',
    'store_id': 'Store_ID',
    'customer_id': 'Customer_ID',
    'quantity': 'Quantity',
    'total_revenue': 'Total_Revenue',
    'cost': 'Product_Cost',
    'shipping_cost': 'Shipping_Cost',
    'marketing_cost': 'Marketing_Cost',
    'net_profit': 'Net_Profit'
})[[
    'Sale_ID', 'Date_ID', 'Product_ID', 'Store_ID', 'Customer_ID',
    'Quantity', 'Total_Revenue', 'Product_Cost', 'Shipping_Cost',
    'Marketing_Cost', 'Net_Profit'
]]

print("  Column mapping completed")

print("\n[3/4] Creating SQLite Data Warehouse...")

if db_path.exists():
    db_path.unlink()
    print("  Old database deleted")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
print(f"  Connected to: {db_path}")

print("\n  Creating Star Schema tables...")

cursor.execute('''
CREATE TABLE Dim_Date (
    Date_ID INTEGER PRIMARY KEY,
    Full_Date DATE NOT NULL,
    Year INTEGER,
    Quarter INTEGER,
    Month INTEGER,
    Month_Name TEXT,
    Day INTEGER,
    Day_Of_Week INTEGER,
    Day_Name TEXT,
    Week_Of_Year INTEGER
)
''')
print("    Dim_Date created")

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
print("    Dim_Product created")

cursor.execute('''
CREATE TABLE Dim_Store (
    Store_ID INTEGER PRIMARY KEY,
    Store_Name TEXT NOT NULL,
    City_Name TEXT,
    Region TEXT,
    Monthly_Target REAL,
    Annual_Target REAL
)
''')
print("    Dim_Store created")

cursor.execute('''
CREATE TABLE Dim_Customer (
    Customer_ID TEXT PRIMARY KEY,
    Customer_Name TEXT NOT NULL,
    City_Name TEXT,
    Region TEXT
)
''')
print("    Dim_Customer created")

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
print("    Fact_Sales created")

conn.commit()

print("\n[4/4] Loading data into tables...")
print("  Loading dimensions first (for referential integrity)...")

Dim_Date.to_sql('Dim_Date', conn, if_exists='append', index=False)
print(f"    Dim_Date: {len(Dim_Date):,} rows inserted")

Dim_Product.to_sql('Dim_Product', conn, if_exists='append', index=False)
print(f"    Dim_Product: {len(Dim_Product):,} rows inserted")

Dim_Store.to_sql('Dim_Store', conn, if_exists='append', index=False)
print(f"    Dim_Store: {len(Dim_Store):,} rows inserted")

Dim_Customer.to_sql('Dim_Customer', conn, if_exists='append', index=False)
print(f"    Dim_Customer: {len(Dim_Customer):,} rows inserted")

print("  Loading fact table...")
Fact_Sales.to_sql('Fact_Sales', conn, if_exists='append', index=False)
print(f"    Fact_Sales: {len(Fact_Sales):,} rows inserted")

conn.commit()
print("\n  All data loaded successfully!")

print("\n[Verification] Checking database integrity...")

print("\n  Table Row Counts:")
print("  " + "-"*60)
all_tables = ['Dim_Date', 'Dim_Product', 'Dim_Store', 'Dim_Customer', 'Fact_Sales']

for table in all_tables:
    count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", conn)['cnt'][0]
    print(f"    {table:20} {count:>10,} rows")
print("  " + "-"*60)

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
    print("\n  Star Schema joins working correctly!")
except Exception as e:
    print(f"  Join test failed: {e}")

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

date_range_query = "SELECT MIN(Full_Date) as Start_Date, MAX(Full_Date) as End_Date FROM Dim_Date"
date_range = pd.read_sql(date_range_query, conn)
print(f"\n  Date Range: {date_range['Start_Date'][0]} to {date_range['End_Date'][0]}")

conn.close()

print("\n" + "="*70)
print("DATA WAREHOUSE LOADING COMPLETED SUCCESSFULLY!")
print("="*70)
print(f"\nDatabase File: {db_path}")
print(f"Date Range: {date_range['Start_Date'][0]} to {date_range['End_Date'][0]}")
print(f"Total Revenue: {summary['Total_Revenue_M_DZD'][0]:,.2f} Million DZD")
print(f"Total Profit: {summary['Total_Profit_M_DZD'][0]:,.2f} Million DZD")
print(f"Profit Margin: {summary['Profit_Margin_Pct'][0]:.2f}%")
print(f"Stores: {summary['Unique_Stores'][0]}")
print(f"Products: {summary['Unique_Products'][0]}")
print(f"Customers: {summary['Unique_Customers'][0]}")
print(f"Transactions: {summary['Total_Transactions'][0]:,}")

print("\n" + "="*70)
print("STAR SCHEMA TABLES (5)")
print("="*70)
print("1. Fact_Sales (Fact Table)")
print("2. Dim_Date (Time Dimension)")
print("3. Dim_Product (Product Dimension)")
print("4. Dim_Store (Store Dimension)")
print("5. Dim_Customer (Customer Dimension)")
print("="*70)