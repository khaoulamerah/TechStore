"""
SQL Query Testing Script
Tests all queries from sql_queries.py against the Data Warehouse
Author: Database Architect - TechStore BI Project
"""

import sqlite3
import pandas as pd
from sql_queries import *

print("="*70)
print("TESTING SQL QUERIES FOR TECHSTORE DASHBOARD")
print("="*70)

# Connect to database
db_path = '../Data/database/techstore_dw.db'
try:
    conn = sqlite3.connect(db_path)
    print(f"\n✓ Connected to: {db_path}\n")
except Exception as e:
    print(f"\n✗ Error connecting to database: {e}")
    exit(1)

# Define all queries to test
queries_to_test = [
    ("Total Revenue", QUERY_TOTAL_REVENUE),
    ("Net Profit", QUERY_NET_PROFIT),
    ("Target Achievement", QUERY_TARGET_ACHIEVEMENT),
    ("Average Sentiment", QUERY_AVG_SENTIMENT),
    ("YTD Growth", QUERY_YTD_GROWTH),
    ("Monthly Trends", QUERY_MONTHLY_TRENDS),
    ("Top Products by Category", QUERY_TOP_PRODUCTS_BY_CATEGORY),
    ("Category Performance", QUERY_CATEGORY_PERFORMANCE),
    ("Marketing ROI", QUERY_MARKETING_ROI),
    ("Price Competitiveness", QUERY_PRICE_COMPETITIVENESS),
    ("Store Performance", QUERY_STORE_PERFORMANCE),
    ("Regional Analysis", QUERY_REGIONAL_ANALYSIS),
    ("Profit Margin by Category", QUERY_PROFIT_MARGIN_BY_CATEGORY),
    ("Sentiment vs Sales", QUERY_SENTIMENT_VS_SALES),
    ("Customer Segmentation", QUERY_CUSTOMER_SEGMENTATION),
]

# Test each query
results = []
for i, (query_name, query_sql) in enumerate(queries_to_test, 1):
    print(f"[{i}/{len(queries_to_test)}] Testing: {query_name}...")
    
    try:
        result = pd.read_sql(query_sql, conn)
        row_count = len(result)
        col_count = len(result.columns)
        
        print(f"  ✓ Success! Returned {row_count} rows, {col_count} columns")
        
        # Show preview for small results
        if row_count <= 5:
            print(f"\n  Preview:")
            print("  " + result.to_string(index=False).replace('\n', '\n  '))
            print()
        
        results.append({
            'Query': query_name,
            'Status': '✓ Pass',
            'Rows': row_count,
            'Columns': col_count
        })
        
    except Exception as e:
        print(f"  ✗ Failed: {e}\n")
        results.append({
            'Query': query_name,
            'Status': f'✗ Fail: {str(e)[:50]}',
            'Rows': 0,
            'Columns': 0
        })

# Summary
print("\n" + "="*70)
print("QUERY TEST SUMMARY")
print("="*70)

summary_df = pd.DataFrame(results)
print(summary_df.to_string(index=False))

passed = sum(1 for r in results if r['Status'] == '✓ Pass')
total = len(results)

print("\n" + "="*70)
print(f"RESULT: {passed}/{total} queries passed")
print("="*70)

if passed == total:
    print("✓ All queries are working correctly!")
    print("✓ Your sql_queries.py is ready for dashboard integration!")
else:
    print(f"⚠ {total - passed} queries failed - please review the errors above")

conn.close()