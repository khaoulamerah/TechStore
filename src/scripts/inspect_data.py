import pandas as pd
import os

print("="*60)
print("DATA INSPECTION REPORT")
print("="*60)

# Define file paths
files_to_check = {
    'Dim Product (Transformed)': '../Data/transformed/dim_product.csv',
    'Fact Sales Final (Transformed)': '../Data/transformed/fact_sales_final.csv',
    'Sales Enriched': '../Data/transformed/sales_enriched.csv',
    'Stores (Extracted)': '../Data/extracted/stores.csv',
    'Customers (Extracted)': '../Data/extracted/customers.csv',
    'Cities (Extracted)': '../Data/extracted/cities.csv',
    'Categories': '../Data/extracted/categories.csv',
    'Subcategories': '../Data/extracted/subcategories.csv',
    'Monthly Targets (Excel)': '../Data/flat_files/monthly_targets.xlsx',
    'Marketing Expenses (Excel)': '../Data/flat_files/marketing_expenses.xlsx',
    'Shipping Rates (Excel)': '../Data/flat_files/shipping_rates.xlsx',
}

for name, path in files_to_check.items():
    print(f"\n{'='*60}")
    print(f"FILE: {name}")
    print(f"Path: {path}")
    print('='*60)
    
    try:
        # Read file based on extension
        if path.endswith('.xlsx'):
            df = pd.read_excel(path)
        else:
            df = pd.read_csv(path)
        
        # Show basic info
        print(f"✓ File loaded successfully!")
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {len(df.columns)}")
        
        # Show column names and data types
        print(f"\n  Column Names and Types:")
        for col in df.columns:
            print(f"    - {col} ({df[col].dtype})")
        
        # Show first 3 rows
        print(f"\n  First 3 Rows Preview:")
        print(df.head(3).to_string())
        
        # Check for missing values
        missing = df.isnull().sum()
        if missing.sum() > 0:
            print(f"\n  ⚠ Missing Values Found:")
            for col in missing[missing > 0].index:
                print(f"    - {col}: {missing[col]} missing")
        else:
            print(f"\n  ✓ No missing values")
            
    except FileNotFoundError:
        print(f"  ✗ FILE NOT FOUND!")
    except Exception as e:
        print(f"  ✗ ERROR: {e}")

print("\n" + "="*60)
print("INSPECTION COMPLETE!")
print("="*60)