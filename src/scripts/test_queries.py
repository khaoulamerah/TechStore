import sqlite3
import pandas as pd
from sql_queries import *

print("="*70)
print("COMPREHENSIVE SQL QUERIES TEST")
print("="*70)

db_path = "../database/techstore_dw.db"
try:
    conn = sqlite3.connect(db_path)
    print(f"Connected to: {db_path}\n")
except Exception as e:
    print(f"Connection failed: {e}")
    exit(1)

queries = [
    ("Total Revenue", QUERY_TOTAL_REVENUE),
    ("Net Profit", QUERY_NET_PROFIT),
    ("Total Transactions", QUERY_TOTAL_TRANSACTIONS),
    ("Avg Transaction Value", QUERY_AVG_TRANSACTION_VALUE),
    ("Monthly Trends", QUERY_MONTHLY_TRENDS),
    ("Top Selling Products", QUERY_TOP_SELLING_PRODUCTS),
    ("Category Performance", QUERY_CATEGORY_PERFORMANCE),
    ("Store Ranking", QUERY_STORE_RANKING),
    ("Regional Performance", QUERY_REGIONAL_PERFORMANCE),
    ("Top Customers", QUERY_TOP_CUSTOMERS),
    ("Profit Margin by Category", QUERY_PROFIT_MARGIN_BY_CATEGORY),
    ("Marketing ROI", QUERY_MARKETING_ROI),
    ("Dashboard Summary", QUERY_DASHBOARD_SUMMARY),
]

results = []
for i, (name, query) in enumerate(queries, 1):
    print(f"[{i}/{len(queries)}] Testing: {name}")
    
    try:
        result = pd.read_sql(query, conn)
        row_count = len(result)
        col_count = len(result.columns)
        
        print(f"  Success: {row_count} rows, {col_count} columns")
        
        if row_count <= 3:
            print(f"  Result:")
            print(result.to_string(index=False))
        
        results.append({
            'Query': name,
            'Status': 'Pass',
            'Rows': row_count,
            'Columns': col_count
        })
        
    except Exception as e:
        error_msg = str(e)[:50]
        print(f"  Failed: {error_msg}")
        results.append({
            'Query': name,
            'Status': f'Fail: {error_msg}',
            'Rows': 0,
            'Columns': 0
        })
    
    print()

print("="*70)
print("TEST SUMMARY")
print("="*70)

summary_df = pd.DataFrame(results)
print(summary_df.to_string(index=False))

passed = sum(1 for r in results if 'Pass' in r['Status'])
failed = len(results) - passed

print(f"\nPassed: {passed}")
print(f"Failed: {failed}")

if failed == 0:
    print("\nAll queries are working perfectly!")
    print("Ready for dashboard integration!")
else:
    print(f"\n{failed} queries need fixing")

conn.close()