"""
Inspection Script for Transformed Star Schema Files
Checks the quality and structure of dimension and fact tables
"""

import pandas as pd

print("="*70)
print("STAR SCHEMA FILES INSPECTION")
print("="*70)

# Define the new transformed files
star_schema_files = {
    'Dim_Customer': '../Data/transformed/Dim_Customer.csv',
    'Dim_Date': '../Data/transformed/Dim_Date.csv',
    'Dim_Product': '../Data/transformed/Dim_Product.csv',
    'Dim_Store': '../Data/transformed/Dim_Store.csv',
    'Fact_Sales': '../Data/transformed/Fact_Sales.csv',
}

all_dataframes = {}

for name, path in star_schema_files.items():
    print(f"\n{'='*70}")
    print(f"TABLE: {name}")
    print(f"Path: {path}")
    print('='*70)
    
    try:
        df = pd.read_csv(path)
        all_dataframes[name] = df
        
        print(f"✓ File loaded successfully!")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        
        # Show column names and data types
        print(f"\n  Column Names and Data Types:")
        for col in df.columns:
            non_null = df[col].notna().sum()
            null_count = df[col].isna().sum()
            print(f"    - {col:30} ({str(df[col].dtype):10}) | Non-null: {non_null:>6,} | Nulls: {null_count:>6,}")
        
        # Show first 3 rows
        print(f"\n  First 3 Rows:")
        print(df.head(3).to_string(index=False))
        
        # Show data quality issues
        issues = []
        
        # Check for duplicates in potential key columns
        if 'Customer_ID' in df.columns:
            dups = df['Customer_ID'].duplicated().sum()
            if dups > 0:
                issues.append(f"⚠ {dups} duplicate Customer_IDs found")
        
        if 'Product_ID' in df.columns:
            dups = df['Product_ID'].duplicated().sum()
            if dups > 0:
                issues.append(f"⚠ {dups} duplicate Product_IDs found")
        
        if 'Store_ID' in df.columns:
            dups = df['Store_ID'].duplicated().sum()
            if dups > 0:
                issues.append(f"⚠ {dups} duplicate Store_IDs found")
        
        if 'Date_ID' in df.columns:
            dups = df['Date_ID'].duplicated().sum()
            if dups > 0:
                issues.append(f"⚠ {dups} duplicate Date_IDs found")
        
        if 'Sale_ID' in df.columns or 'trans_id' in df.columns:
            id_col = 'Sale_ID' if 'Sale_ID' in df.columns else 'trans_id'
            dups = df[id_col].duplicated().sum()
            if dups > 0:
                issues.append(f"⚠ {dups} duplicate {id_col}s found")
        
        # Check for missing values in important columns
        missing = df.isnull().sum()
        if missing.sum() > 0:
            print(f"\n  Missing Values:")
            for col in missing[missing > 0].index:
                pct = (missing[col] / len(df)) * 100
                print(f"    - {col}: {missing[col]:,} ({pct:.1f}%)")
        else:
            print(f"\n  ✓ No missing values")
        
        # Show any issues
        if issues:
            print(f"\n  Data Quality Issues:")
            for issue in issues:
                print(f"    {issue}")
        else:
            print(f"\n  ✓ No data quality issues detected")
            
    except FileNotFoundError:
        print(f"  ✗ FILE NOT FOUND!")
    except Exception as e:
        print(f"  ✗ ERROR: {e}")

# ============================================
# REFERENTIAL INTEGRITY CHECKS
# ============================================
print("\n" + "="*70)
print("REFERENTIAL INTEGRITY CHECKS")
print("="*70)

if len(all_dataframes) == 5:
    fact = all_dataframes['Fact_Sales']
    
    checks = []
    
    # Check Product_ID references
    if 'Product_ID' in fact.columns and 'Product_ID' in all_dataframes['Dim_Product'].columns:
        fact_products = set(fact['Product_ID'].unique())
        dim_products = set(all_dataframes['Dim_Product']['Product_ID'].unique())
        orphans = fact_products - dim_products
        if orphans:
            checks.append(f"⚠ {len(orphans)} Product_IDs in Fact_Sales not in Dim_Product")
        else:
            checks.append(f"✓ All Product_IDs have matching dimension records")
    
    # Check Store_ID references
    if 'Store_ID' in fact.columns and 'Store_ID' in all_dataframes['Dim_Store'].columns:
        fact_stores = set(fact['Store_ID'].unique())
        dim_stores = set(all_dataframes['Dim_Store']['Store_ID'].unique())
        orphans = fact_stores - dim_stores
        if orphans:
            checks.append(f"⚠ {len(orphans)} Store_IDs in Fact_Sales not in Dim_Store")
        else:
            checks.append(f"✓ All Store_IDs have matching dimension records")
    
    # Check Customer_ID references
    if 'Customer_ID' in fact.columns and 'Customer_ID' in all_dataframes['Dim_Customer'].columns:
        fact_customers = set(fact['Customer_ID'].unique())
        dim_customers = set(all_dataframes['Dim_Customer']['Customer_ID'].unique())
        orphans = fact_customers - dim_customers
        if orphans:
            checks.append(f"⚠ {len(orphans)} Customer_IDs in Fact_Sales not in Dim_Customer")
        else:
            checks.append(f"✓ All Customer_IDs have matching dimension records")
    
    # Check Date_ID references
    if 'Date_ID' in fact.columns and 'Date_ID' in all_dataframes['Dim_Date'].columns:
        fact_dates = set(fact['Date_ID'].unique())
        dim_dates = set(all_dataframes['Dim_Date']['Date_ID'].unique())
        orphans = fact_dates - dim_dates
        if orphans:
            checks.append(f"⚠ {len(orphans)} Date_IDs in Fact_Sales not in Dim_Date")
        else:
            checks.append(f"✓ All Date_IDs have matching dimension records")
    
    for check in checks:
        print(f"  {check}")
else:
    print("  ⚠ Not all files loaded - skipping integrity checks")

print("\n" + "="*70)
print("INSPECTION COMPLETE!")
print("="*70)