import pandas as pd
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pytesseract
from PIL import Image
import re
from fuzzywuzzy import process

# ===============================================================
# GLOBAL CONFIGURATION
# ===============================================================
EXCHANGE_RATE_USD_DZD = 135.0

# Chemin vers Tesseract (à adapter si nécessaire)
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'


def standardize_columns(df):
    """Standardise les noms de colonnes : minuscules et avec underscores"""
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df


def clean_id_column(series, column_name='id'):
    """
    Nettoie une colonne d'ID en supprimant les préfixes non-numériques.
    Exemple: 'P125' -> 125, 'Store_5' -> 5, 'C001' -> 1
    """
    try:
        cleaned = pd.to_numeric(
            series.astype(str).str.replace(r'[^\d]', '', regex=True), 
            errors='coerce'
        ).fillna(0).astype(int)
        return cleaned
    except Exception as e:
        print(f"⚠ Warning cleaning {column_name}: {e}")
        return series


# ===============================================================
# 1. LOAD FLAT FILES
# ===============================================================
def load_flat_files():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    flat_files_dir = os.path.join(base_dir, '../data/flat_files')

    try:
        marketing_df = pd.read_excel(os.path.join(flat_files_dir, 'marketing_expenses.xlsx')).copy()
        targets_df = pd.read_excel(os.path.join(flat_files_dir, 'monthly_targets.xlsx')).copy()
        shipping_df = pd.read_excel(os.path.join(flat_files_dir, 'shipping_rates.xlsx')).copy()

        marketing_df = standardize_columns(marketing_df)
        targets_df = standardize_columns(targets_df)
        shipping_df = standardize_columns(shipping_df)

        print("✓ Flat files loaded successfully")
        print(f"   Marketing: {marketing_df.shape}")
        print(f"   Targets:   {targets_df.shape}")
        print(f"   Shipping:  {shipping_df.shape}")

        return marketing_df, targets_df, shipping_df
    except Exception as e:
        print(f"✗ Error loading flat files: {e}")
        return None, None, None


# ===============================================================
# 2. CLEAN DATAFRAMES
# ===============================================================
def clean_dataframes(marketing_df, targets_df, shipping_df):
    marketing_df = marketing_df.drop_duplicates().copy()
    marketing_df['category'] = marketing_df['category'].astype(str).str.lower().str.strip()
    marketing_df['date'] = pd.to_datetime(marketing_df['date'], errors='coerce')
    marketing_df['marketing_cost_usd'] = pd.to_numeric(marketing_df['marketing_cost_usd'], errors='coerce').fillna(0).clip(lower=0)

    targets_df = targets_df.drop_duplicates().copy()
    targets_df['month'] = pd.to_datetime(targets_df['month'], errors='coerce')
    targets_df['store_id'] = targets_df['store_id'].astype(str).str.replace(r'^[Ss]tore_?', '', regex=True).str.strip()
    targets_df['store_id'] = pd.to_numeric(targets_df['store_id'], errors='coerce').astype('Int64')
    targets_df['target_revenue'] = pd.to_numeric(
        targets_df['target_revenue'].astype(str).str.replace(',', ''), 
        errors='coerce'
    ).fillna(0).clip(lower=0)

    shipping_df = shipping_df.drop_duplicates().copy()
    shipping_df['region_name'] = shipping_df['region_name'].astype(str).str.lower().str.strip()
    shipping_df['shipping_cost'] = pd.to_numeric(shipping_df['shipping_cost'], errors='coerce').fillna(0).clip(lower=0)

    print("✓ Cleaning completed")
    return marketing_df, targets_df, shipping_df


# ===============================================================
# 3. CURRENCY HARMONIZATION
# ===============================================================
def harmonize_currency(marketing_df, targets_df, shipping_df):
    marketing_df['marketing_cost_dzd'] = (marketing_df['marketing_cost_usd'] * EXCHANGE_RATE_USD_DZD).round(2)
    print("✓ USD → DZD conversion done")
    return marketing_df, targets_df, shipping_df


# ===============================================================
# 4. SENTIMENT ANALYSIS
# ===============================================================
def analyze_sentiment():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, '../data/extracted/reviews.csv')
    if not os.path.exists(path):
        print("✗ reviews.csv not found → sentiment skipped")
        return pd.DataFrame(columns=['product_id', 'avg_sentiment', 'avg_rating', 'review_count'])

    try:
        df = standardize_columns(pd.read_csv(path))
        if not {'review_text', 'product_id', 'rating'}.issubset(df.columns):
            print("✗ Missing columns in reviews → sentiment skipped")
            return pd.DataFrame(columns=['product_id', 'avg_sentiment', 'avg_rating', 'review_count'])

        df['review_text'] = df['review_text'].fillna('').astype(str).str.lower().str.strip()
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        
        analyzer = SentimentIntensityAnalyzer()
        df['sentiment_score'] = df['review_text'].apply(
            lambda x: round(analyzer.polarity_scores(x)['compound'], 2) if x else 0.0
        )

        result = df.groupby('product_id').agg({
            'sentiment_score': 'mean',
            'rating': 'mean',
            'review_text': 'count'
        }).reset_index()
        result.columns = ['product_id', 'avg_sentiment', 'avg_rating', 'review_count']
        result['avg_sentiment'] = result['avg_sentiment'].round(2)
        result['avg_rating'] = result['avg_rating'].round(2)

        print(f"✓ Sentiment analysis done ({len(result)} products)")
        return result
    except Exception as e:
        print(f"✗ Error in sentiment analysis: {e} → skipped")
        return pd.DataFrame(columns=['product_id', 'avg_sentiment', 'avg_rating', 'review_count'])


# ===============================================================
# 5. COMPETITOR PRICE INTEGRATION
# ===============================================================
def integrate_competitor_prices(products_df):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, '../data/extracted/competitor_prices.csv')

    if not os.path.exists(path):
        print("✗ competitor_prices.csv not found → skipped")
        products_df['competitor_price'] = None
        products_df['price_difference'] = None
        products_df['price_difference_pct'] = None
        return products_df

    if os.path.getsize(path) == 0:
        print("⚠ competitor_prices.csv is empty → skipped")
        products_df['competitor_price'] = None
        products_df['price_difference'] = None
        products_df['price_difference_pct'] = None
        return products_df

    try:
        comp_df = standardize_columns(pd.read_csv(path))

        if comp_df.empty or not {'competitor_product_name', 'competitor_price'}.issubset(comp_df.columns):
            print("⚠ competitor_prices.csv has no valid data → skipped")
            products_df['competitor_price'] = None
            products_df['price_difference'] = None
            products_df['price_difference_pct'] = None
            return products_df

        comp_names = comp_df['competitor_product_name'].str.lower().tolist()

        def match(name):
            if pd.isna(name):
                return None, None
            best, score = process.extractOne(name.lower(), comp_names)
            if score > 80:
                price = comp_df.loc[comp_df['competitor_product_name'].str.lower() == best, 'competitor_price'].iloc[0]
                return best, price
            return None, None

        matches = products_df['product_name'].apply(match)
        products_df['competitor_price'] = matches.apply(lambda x: x[1])
        products_df['price_difference'] = products_df['unit_price'] - products_df['competitor_price']
        
        # FIX: price_difference_pct should always be positive (absolute value)
        # It represents how much MORE expensive (positive) or CHEAPER (negative) we are
        products_df['price_difference_pct'] = (
            (products_df['price_difference'] / products_df['competitor_price']) * 100
        ).round(2)
        
        # If you want ABSOLUTE percentage difference (always positive):
        # products_df['price_difference_pct'] = (
        #     (abs(products_df['price_difference']) / products_df['competitor_price']) * 100
        # ).round(2)

        matched_count = products_df['competitor_price'].notna().sum()
        print(f"✓ {matched_count} competitor prices matched")
        return products_df

    except pd.errors.EmptyDataError:
        print("⚠ competitor_prices.csv is empty (EmptyDataError) → skipped")
        products_df['competitor_price'] = None
        products_df['price_difference'] = None
        products_df['price_difference_pct'] = None
        return products_df
    except Exception as e:
        print(f"✗ Error processing competitor prices: {e} → skipped")
        products_df['competitor_price'] = None
        products_df['price_difference'] = None
        products_df['price_difference_pct'] = None
        return products_df


# ===============================================================
# 6. CREATE DIM_PRODUCT
# ===============================================================
def create_dim_product(sentiment_df):
    """
    COLONNES FINALES ATTENDUES:
    - product_id (INT)
    - product_name (VARCHAR)
    - subcategory_id (INT)
    - subcategory_name (VARCHAR)
    - category_id (INT)
    - category_name (VARCHAR)
    - unit_price (DECIMAL)
    - unit_cost (DECIMAL)
    - competitor_price (DECIMAL)
    - price_difference (DECIMAL)
    - price_difference_pct (DECIMAL)
    - avg_sentiment (DECIMAL)
    - avg_rating (DECIMAL)
    - review_count (INT)
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')
    
    products_path = os.path.join(extracted_dir, 'products.csv')
    if not os.path.exists(products_path):
        print("✗ products.csv not found → dim_product skipped")
        return None
    
    products_df = standardize_columns(pd.read_csv(products_path))
    
    # Load subcategories and categories
    subcategories_df = None
    subcat_path = os.path.join(extracted_dir, 'subcategories.csv')
    if os.path.exists(subcat_path):
        subcategories_df = standardize_columns(pd.read_csv(subcat_path))
    
    categories_df = None
    categories_path = os.path.join(extracted_dir, 'categories.csv')
    if os.path.exists(categories_path):
        categories_df = standardize_columns(pd.read_csv(categories_path))
    
    # Normalize subcategory column names
    if subcategories_df is not None:
        subcat_id_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                              if col in subcategories_df.columns), None)
        if subcat_id_col and subcat_id_col != 'subcategory_id':
            subcategories_df = subcategories_df.rename(columns={subcat_id_col: 'subcategory_id'})
        
        if 'subcat_name' in subcategories_df.columns:
            subcategories_df = subcategories_df.rename(columns={'subcat_name': 'subcategory_name'})
    
    # Normalize product subcategory column
    prod_subcat_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                            if col in products_df.columns), None)
    if prod_subcat_col and prod_subcat_col != 'subcategory_id':
        products_df = products_df.rename(columns={prod_subcat_col: 'subcategory_id'})
    
    # Merge with subcategories
    if subcategories_df is not None and 'subcategory_id' in products_df.columns:
        products_df = products_df.merge(
            subcategories_df[['subcategory_id', 'subcategory_name', 'category_id']], 
            on='subcategory_id', 
            how='left'
        )
    
    # Merge with categories
    if categories_df is not None and 'category_id' in products_df.columns:
        products_df = products_df.merge(
            categories_df[['category_id', 'category_name']], 
            on='category_id', 
            how='left'
        )
    
    # Add sentiment
    products_df = products_df.merge(sentiment_df, on='product_id', how='left')
    products_df['avg_sentiment'] = products_df['avg_sentiment'].fillna(0).round(2)
    products_df['avg_rating'] = products_df['avg_rating'].fillna(0).round(2)
    products_df['review_count'] = products_df['review_count'].fillna(0).astype(int)
    
    # Add competitor prices
    products_df = integrate_competitor_prices(products_df)
    
    # Ensure numeric columns are properly typed
    products_df['unit_price'] = pd.to_numeric(products_df['unit_price'], errors='coerce').round(2)
    products_df['unit_cost'] = pd.to_numeric(products_df['unit_cost'], errors='coerce').round(2)
    
    # Fill NaN values for optional columns
    products_df['subcategory_id'] = clean_id_column(products_df['subcategory_id'], 'subcategory_id')
    products_df['subcategory_name'] = products_df['subcategory_name'].fillna('Unknown')
    products_df['category_id'] = clean_id_column(products_df['category_id'], 'category_id')
    products_df['category_name'] = products_df['category_name'].fillna('Unknown')
    products_df['product_id'] = clean_id_column(products_df['product_id'], 'product_id')
    
    # Select final columns in correct order
    dim_product = products_df[[
        'product_id', 
        'product_name', 
        'subcategory_id', 
        'subcategory_name',
        'category_id', 
        'category_name',
        'unit_price', 
        'unit_cost',
        'competitor_price', 
        'price_difference', 'price_difference_pct',
        'avg_sentiment', 
        'avg_rating', 
        'review_count'
    ]].copy()
    
    # FIX: Remove duplicates based on product_id
    dim_product = dim_product.drop_duplicates(subset=['product_id'], keep='first')
    
    # Validate data
    print(f"✓ dim_product created ({len(dim_product)} products)")
    print(f"   Price range: {dim_product['unit_price'].min():.2f} - {dim_product['unit_price'].max():.2f} DZD")
    print(f"   Categories: {dim_product['category_name'].nunique()} unique")
    
    return dim_product

# ===============================================================
# 7. CREATE DIM_STORE - AVEC ANNUAL_TARGET
# ===============================================================
def create_dim_store(targets_df):
    """
    COLONNES FINALES ATTENDUES:
    - store_id (INT)
    - store_name (VARCHAR)
    - city_id (INT)
    - city_name (VARCHAR)
    - region (VARCHAR)
    - monthly_target (DECIMAL) - Average monthly target
    - annual_target (DECIMAL) - monthly_target × 12
    - manager_name (VARCHAR)
    
    FIX: Added annual_target calculation
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')
    
    stores_path = os.path.join(extracted_dir, 'stores.csv')
    cities_path = os.path.join(extracted_dir, 'cities.csv')
    
    if not os.path.exists(stores_path) or not os.path.exists(cities_path):
        print("✗ stores.csv or cities.csv not found → dim_store skipped")
        return None
    
    stores_df = standardize_columns(pd.read_csv(stores_path))
    cities_df = standardize_columns(pd.read_csv(cities_path))
    
    # Normalize region column name
    if 'region_name' in cities_df.columns:
        cities_df = cities_df.rename(columns={'region_name': 'region'})
    
    # Merge with cities
    dim_store = stores_df.merge(
        cities_df[['city_id', 'city_name', 'region']], 
        on='city_id', 
        how='left'
    )
    
    # Calculate AVERAGE monthly target per store
    targets_df['month'] = pd.to_datetime(targets_df['month'], errors='coerce')
    store_targets = targets_df.groupby('store_id').agg({
        'target_revenue': 'mean',  # Average monthly target
        'manager_name': 'first'
    }).reset_index()
    
    # Rename to monthly_target for clarity
    store_targets = store_targets.rename(columns={'target_revenue': 'monthly_target'})
    
    dim_store = dim_store.merge(
        store_targets, 
        on='store_id', 
        how='left'
    )
    
    # Fill missing values
    dim_store['monthly_target'] = dim_store['monthly_target'].fillna(0).round(2)
    dim_store['manager_name'] = dim_store['manager_name'].fillna('Unknown')
    dim_store['city_name'] = dim_store['city_name'].fillna('Unknown')
    dim_store['region'] = dim_store['region'].fillna('Unknown')
    
    # FIX: Calculate annual_target = monthly_target × 12
    dim_store['annual_target'] = (dim_store['monthly_target'] * 12).round(2)
    
    # Clean IDs
    dim_store['store_id'] = clean_id_column(dim_store['store_id'], 'store_id')
    dim_store['city_id'] = clean_id_column(dim_store['city_id'], 'city_id')
    
    # Select final columns
    dim_store = dim_store[[
        'store_id', 
        'store_name',
        'city_id', 
        'city_name', 
        'region',
        'monthly_target',
        'annual_target',
        'manager_name'
    ]].copy()
    
    # Remove duplicates based on store_id
    dim_store = dim_store.drop_duplicates(subset=['store_id'], keep='first')
    
    # Validate data
    print(f"✓ dim_store created ({len(dim_store)} stores)")
    print(f"   Regions: {dim_store['region'].nunique()} unique")
    print(f"   Average monthly target: {dim_store['monthly_target'].mean():,.2f} DZD")
    print(f"   Average annual target: {dim_store['annual_target'].mean():,.2f} DZD")
    
    return dim_store
# ===============================================================
# 8. CREATE DIM_CUSTOMER - SUPPRESSION DES HOMONYMES
# ===============================================================
def create_dim_customer():
    """
    COLONNES FINALES ATTENDUES:
    - customer_id (INT)
    - full_name (VARCHAR)
    - city_id (INT)
    - city_name (VARCHAR)
    - region (VARCHAR)
    
    FIX: Supprime les homonymes (même nom + même ville)
    Garde seulement le premier customer_id trouvé
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')
    
    customers_path = os.path.join(extracted_dir, 'customers.csv')
    cities_path = os.path.join(extracted_dir, 'cities.csv')
    
    if not os.path.exists(customers_path) or not os.path.exists(cities_path):
        print("✗ customers.csv or cities.csv not found → dim_customer skipped")
        return None
    
    customers_df = standardize_columns(pd.read_csv(customers_path))
    cities_df = standardize_columns(pd.read_csv(cities_path))
    
    # Normalize region column name
    if 'region_name' in cities_df.columns:
        cities_df = cities_df.rename(columns={'region_name': 'region'})
    
    print(f"   Initial rows: {len(customers_df)}")
    
    # Clean IDs
    customers_df['customer_id'] = clean_id_column(customers_df['customer_id'], 'customer_id')
    customers_df['city_id'] = clean_id_column(customers_df['city_id'], 'city_id')
    cities_df['city_id'] = clean_id_column(cities_df['city_id'], 'city_id')
    
    # Strip whitespace and normalize names
    customers_df['full_name'] = customers_df['full_name'].astype(str).str.strip()
    
    # STEP 1: Remove duplicates based on customer_id (true duplicates)
    dup_by_id = customers_df.duplicated(subset=['customer_id']).sum()
    if dup_by_id > 0:
        print(f"   Found {dup_by_id} duplicate customer_ids → removing")
        customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')
    
    # STEP 2: Remove homonymes (same name + same city = likely duplicate person)
    dup_by_name_city = customers_df.duplicated(subset=['full_name', 'city_id']).sum()
    if dup_by_name_city > 0:
        print(f"   Found {dup_by_name_city} homonymes (same name + city) → removing")
        
        # Show examples
        homonymes = customers_df[customers_df.duplicated(subset=['full_name', 'city_id'], keep=False)].sort_values(['full_name', 'city_id'])
        unique_homonymes = homonymes.groupby(['full_name', 'city_id']).size().reset_index(name='count')
        
        print(f"   Examples of homonymes found:")
        for _, row in unique_homonymes.head(5).iterrows():
            entries = customers_df[(customers_df['full_name'] == row['full_name']) & 
                                   (customers_df['city_id'] == row['city_id'])]
            customer_ids = entries['customer_id'].tolist()
            print(f"      '{row['full_name']}' in city {row['city_id']}: customer_ids {customer_ids}")
        
        # Keep only first occurrence of each name+city combination
        customers_df = customers_df.drop_duplicates(subset=['full_name', 'city_id'], keep='first')
    
    print(f"   After deduplication: {len(customers_df)} rows")
    
    # Merge with cities
    dim_customer = customers_df.merge(
        cities_df[['city_id', 'city_name', 'region']], 
        on='city_id', 
        how='left'
    )
    
    # Fill missing values
    dim_customer['city_name'] = dim_customer['city_name'].fillna('Unknown')
    dim_customer['region'] = dim_customer['region'].fillna('Unknown')
    
    # Select final columns
    dim_customer = dim_customer[[
        'customer_id', 
        'full_name',
        'city_id', 
        'city_name', 
        'region'
    ]].copy()
    
    # Reset index
    dim_customer = dim_customer.reset_index(drop=True)
    
    # Final verification
    final_dup_id = dim_customer.duplicated(subset=['customer_id']).sum()
    final_dup_name_city = dim_customer.duplicated(subset=['full_name', 'city_id']).sum()
    
    if final_dup_id > 0 or final_dup_name_city > 0:
        print(f"   ⚠ WARNING: Still found duplicates!")
        print(f"      By customer_id: {final_dup_id}")
        print(f"      By name+city: {final_dup_name_city}")
    else:
        print(f"   ✓ NO duplicates (by customer_id or name+city)")
    
    print(f"✓ dim_customer: {len(dim_customer)} unique customers")
    print(f"   Regions: {dim_customer['region'].nunique()}")
    print(f"   Cities: {dim_customer['city_name'].nunique()}")
    
    return dim_customer

def create_dim_date():
    """
    COLONNES FINALES:
    - date_id (INT) - YYYYMMDD, PRIMARY KEY
    - date (VARCHAR) - YYYY-MM-DD
    - year (INT)
    - quarter (INT) - 1-4
    - month (INT) - 1-12
    - month_name (VARCHAR)
    - day (INT) - 1-31
    - day_of_week (INT) - 1=Monday, 7=Sunday
    - day_name (VARCHAR)
    - week_of_year (INT) - 1-53
    """
    import pandas as pd
    
    START_DATE = '2023-01-01'
    END_DATE = '2024-12-31'
    
    date_range = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
    
    dim_date = pd.DataFrame({'date': date_range})
    
    dim_date['date_id'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['date'].dt.year.astype(int)
    dim_date['quarter'] = dim_date['date'].dt.quarter.astype(int)
    dim_date['month'] = dim_date['date'].dt.month.astype(int)
    dim_date['month_name'] = dim_date['date'].dt.strftime('%B')
    dim_date['day'] = dim_date['date'].dt.day.astype(int)
    dim_date['day_of_week'] = (dim_date['date'].dt.dayofweek + 1).astype(int)
    dim_date['day_name'] = dim_date['date'].dt.strftime('%A')
    dim_date['week_of_year'] = dim_date['date'].dt.isocalendar().week.clip(1, 53).astype(int)
    
    dim_date['date'] = dim_date['date'].dt.strftime('%Y-%m-%d')
    
    dim_date = dim_date[[
        'date_id', 'date', 'year', 'quarter', 'month', 'month_name',
        'day', 'day_of_week', 'day_name', 'week_of_year'
    ]]
    
    assert dim_date['date_id'].nunique() == len(dim_date), "date_id not unique!"
    assert not dim_date.duplicated(subset=['date_id']).any(), "Duplicate date_ids found!"
    
    return dim_date


def add_date_id_to_fact_sales(fact_sales_df, dim_date_df):
    """
    Ajoute date_id à fact_sales et convertit date en format ISO.
    """
    import pandas as pd
    
    fact_sales_df['date'] = pd.to_datetime(fact_sales_df['date'])
    fact_sales_df['date_id'] = fact_sales_df['date'].dt.strftime('%Y%m%d').astype(int)
    
    missing = fact_sales_df[~fact_sales_df['date_id'].isin(dim_date_df['date_id'])]
    if not missing.empty:
        print(f"⚠ {len(missing)} transactions with dates NOT in dim_date")
    
    fact_sales_df['date'] = fact_sales_df['date'].dt.strftime('%Y-%m-%d')
    
    return fact_sales_df
# ===============================================================
# 10. NET PROFIT CALCULATION & FACT_SALES
# ===============================================================
def calculate_net_profit(marketing_df, shipping_df):
    """
    COLONNES FINALES ATTENDUES:
    - trans_id (INT)
    - date (DATE format YYYY-MM-DD)
    - store_id (INT)
    - product_id (INT)
    - customer_id (INT)
    - quantity (INT)
    - total_revenue (DECIMAL)
    - cost (DECIMAL)
    - gross_profit (DECIMAL)
    - shipping_cost (DECIMAL)
    - marketing_cost (DECIMAL)
    - net_profit (DECIMAL)
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')

    required_files = ['sales.csv', 'products.csv', 'customers.csv', 'cities.csv', 'categories.csv']
    for f in required_files:
        if not os.path.exists(os.path.join(extracted_dir, f)):
            return None

    sales_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'sales.csv')))
    products_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'products.csv')))
    customers_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'customers.csv')))
    cities_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'cities.csv')))
    categories_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'categories.csv')))

    sales_df = sales_df.drop_duplicates(subset=['trans_id'], keep='first')

    subcategories_df = None
    subcat_path = os.path.join(extracted_dir, 'subcategories.csv')
    if os.path.exists(subcat_path):
        subcategories_df = standardize_columns(pd.read_csv(subcat_path))
        subcat_id_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                              if col in subcategories_df.columns), None)
        if subcat_id_col and subcat_id_col != 'subcategory_id':
            subcategories_df = subcategories_df.rename(columns={subcat_id_col: 'subcategory_id'})

    if 'region' in cities_df.columns and 'region_name' not in cities_df.columns:
        cities_df = cities_df.rename(columns={'region': 'region_name'})

    sales_df['date'] = pd.to_datetime(sales_df['date'], errors='coerce')
    sales_df['month'] = sales_df['date'].dt.to_period('M')

    enriched_df = sales_df.copy()

    enriched_df = enriched_df.merge(
        products_df[['product_id', 'unit_cost']], 
        on='product_id', 
        how='left'
    )

    enriched_df = enriched_df.merge(
        customers_df[['customer_id', 'city_id']], 
        on='customer_id', 
        how='left'
    )
    enriched_df = enriched_df.merge(
        cities_df[['city_id', 'region_name']], 
        on='city_id', 
        how='left'
    )
    
    enriched_df['region_name'] = enriched_df['region_name'].astype(str).str.lower().str.strip()
    shipping_df['region_name'] = shipping_df['region_name'].astype(str).str.lower().str.strip()
    
    enriched_df = enriched_df.merge(
        shipping_df[['region_name', 'shipping_cost']], 
        on='region_name', 
        how='left'
    )

    category_added = False
    if subcategories_df is not None:
        prod_subcat_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                                if col in products_df.columns), None)
        
        if prod_subcat_col:
            if prod_subcat_col != 'subcategory_id':
                products_df = products_df.rename(columns={prod_subcat_col: 'subcategory_id'})
            
            enriched_df = enriched_df.merge(
                products_df[['product_id', 'subcategory_id']], 
                on='product_id', 
                how='left'
            )
            enriched_df = enriched_df.merge(
                subcategories_df[['subcategory_id', 'category_id']], 
                on='subcategory_id', 
                how='left'
            )
            enriched_df = enriched_df.merge(
                categories_df[['category_id', 'category_name']], 
                on='category_id', 
                how='left'
            )
            enriched_df = enriched_df.rename(columns={'category_name': 'category'})
            category_added = True

    if not category_added:
        cat_col_in_products = next((col for col in ['category', 'category_name', 'category_id'] 
                                    if col in products_df.columns), None)
        
        if cat_col_in_products == 'category_id' and 'category_name' in categories_df.columns:
            temp = products_df[['product_id', 'category_id']].merge(
                categories_df[['category_id', 'category_name']], 
                on='category_id', 
                how='left'
            )
            temp = temp.rename(columns={'category_name': 'category'})
            enriched_df = enriched_df.merge(
                temp[['product_id', 'category']], 
                on='product_id', 
                how='left'
            )
            category_added = True
        elif cat_col_in_products in ['category', 'category_name']:
            enriched_df = enriched_df.merge(
                products_df[['product_id', cat_col_in_products]].rename(columns={cat_col_in_products: 'category'}),
                on='product_id', 
                how='left'
            )
            category_added = True

    if not category_added:
        enriched_df['category'] = 'unknown'

    enriched_df['category'] = enriched_df['category'].astype(str).str.lower().str.strip()

    marketing_df['month'] = marketing_df['date'].dt.to_period('M')
    marketing_df['category'] = marketing_df['category'].astype(str).str.lower().str.strip()

    cat_month_rev = enriched_df.groupby(['category', 'month'])['total_revenue'].sum().reset_index(name='cat_month_total')
    
    enriched_df = enriched_df.merge(
        cat_month_rev, 
        on=['category', 'month'], 
        how='left'
    )
    
    enriched_df = enriched_df.merge(
        marketing_df[['category', 'month', 'marketing_cost_dzd']], 
        on=['category', 'month'], 
        how='left'
    )

    enriched_df['allocated_marketing_dzd'] = (
        enriched_df['total_revenue'] / enriched_df['cat_month_total']
    ) * enriched_df['marketing_cost_dzd']

    enriched_df['unit_cost'] = pd.to_numeric(enriched_df['unit_cost'], errors='coerce').fillna(0)
    enriched_df['cost'] = (enriched_df['unit_cost'] * enriched_df['quantity']).round(2)
    
    enriched_df['shipping_cost'] = pd.to_numeric(enriched_df['shipping_cost'], errors='coerce')
    
    if enriched_df['shipping_cost'].isna().all():
        enriched_df['shipping_cost'] = 50.0
    else:
        enriched_df['shipping_cost'] = enriched_df['shipping_cost'].fillna(50.0)
    
    enriched_df['shipping_cost_total'] = (enriched_df['shipping_cost'] * enriched_df['quantity']).round(2)
    
    enriched_df['allocated_marketing_dzd'] = enriched_df['allocated_marketing_dzd'].fillna(0).round(2)

    enriched_df['gross_profit'] = (enriched_df['total_revenue'] - enriched_df['cost']).round(2)

    enriched_df['net_profit'] = (
        enriched_df['gross_profit'] -
        enriched_df['shipping_cost_total'] -
        enriched_df['allocated_marketing_dzd']
    ).round(2)

    fact_sales = enriched_df[[
        'trans_id', 
        'date', 
        'store_id', 
        'product_id', 
        'customer_id',
        'quantity', 
        'total_revenue', 
        'cost', 
        'gross_profit', 
        'shipping_cost_total', 
        'allocated_marketing_dzd', 
        'net_profit'
    ]].copy()

    fact_sales = fact_sales.rename(columns={
        'shipping_cost_total': 'shipping_cost',
        'allocated_marketing_dzd': 'marketing_cost'
    })

    fact_sales['trans_id'] = clean_id_column(fact_sales['trans_id'], 'trans_id')
    fact_sales['store_id'] = clean_id_column(fact_sales['store_id'], 'store_id')
    fact_sales['product_id'] = clean_id_column(fact_sales['product_id'], 'product_id')
    fact_sales['customer_id'] = clean_id_column(fact_sales['customer_id'], 'customer_id')
    fact_sales['quantity'] = pd.to_numeric(fact_sales['quantity'], errors='coerce').fillna(0).astype(int)
    
    decimal_cols = ['total_revenue', 'cost', 'gross_profit', 'shipping_cost', 'marketing_cost', 'net_profit']
    for col in decimal_cols:
        fact_sales[col] = fact_sales[col].round(2)
    
    fact_sales = fact_sales.drop_duplicates(subset=['trans_id'], keep='first')

    return fact_sales


# ===============================================================
# 11. CALCULATE MARKETING ROI
# ===============================================================
def calculate_marketing_roi(fact_sales_df, marketing_df):
    """
    COLONNES FINALES ATTENDUES:
    - category (VARCHAR)
    - month (VARCHAR format YYYY-MM)
    - total_revenue (DECIMAL)
    - marketing_cost (DECIMAL)
    - roi_percent (DECIMAL)
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')
    
    try:
        # Simplify: Use the aggregated data already in fact_sales
        # fact_sales already has product_id and marketing_cost allocated per transaction
        
        products_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'products.csv')))
        categories_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'categories.csv')))
        
        # FIX: Clean product_id to ensure same type (int)
        products_df['product_id'] = clean_id_column(products_df['product_id'], 'product_id')
        categories_df['category_id'] = clean_id_column(categories_df['category_id'], 'category_id')
        
        # Create a copy and ensure date is datetime
        fact_with_cat = fact_sales_df.copy()
        fact_with_cat['date'] = pd.to_datetime(fact_with_cat['date'])
        fact_with_cat['month'] = fact_with_cat['date'].dt.to_period('M')
        
        # Try to get category via subcategories first
        subcategories_df = None
        subcat_path = os.path.join(extracted_dir, 'subcategories.csv')
        if os.path.exists(subcat_path):
            subcategories_df = standardize_columns(pd.read_csv(subcat_path))
            subcat_id_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                                  if col in subcategories_df.columns), None)
            if subcat_id_col and subcat_id_col != 'subcategory_id':
                subcategories_df = subcategories_df.rename(columns={subcat_id_col: 'subcategory_id'})
            
            # FIX: Clean subcategory IDs
            subcategories_df['subcategory_id'] = clean_id_column(subcategories_df['subcategory_id'], 'subcategory_id')
            subcategories_df['category_id'] = clean_id_column(subcategories_df['category_id'], 'category_id')
        
        # Normalize product subcategory column
        prod_subcat_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                                if col in products_df.columns), None)
        if prod_subcat_col and prod_subcat_col != 'subcategory_id':
            products_df = products_df.rename(columns={prod_subcat_col: 'subcategory_id'})
        
        # FIX: Clean subcategory_id in products
        if 'subcategory_id' in products_df.columns:
            products_df['subcategory_id'] = clean_id_column(products_df['subcategory_id'], 'subcategory_id')
        
        # Merge to get category
        if subcategories_df is not None and 'subcategory_id' in products_df.columns:
            fact_with_cat = fact_with_cat.merge(
                products_df[['product_id', 'subcategory_id']], 
                on='product_id', 
                how='left'
            )
            fact_with_cat = fact_with_cat.merge(
                subcategories_df[['subcategory_id', 'category_id']], 
                on='subcategory_id', 
                how='left'
            )
        else:
            # Fallback: merge directly with products if it has category_id
            if 'category_id' in products_df.columns:
                fact_with_cat = fact_with_cat.merge(
                    products_df[['product_id', 'category_id']], 
                    on='product_id', 
                    how='left'
                )
        
        # Merge with categories to get category name
        fact_with_cat = fact_with_cat.merge(
            categories_df[['category_id', 'category_name']], 
            on='category_id', 
            how='left'
        )
        
        # Normalize category
        fact_with_cat['category'] = fact_with_cat['category_name'].fillna('unknown').astype(str).str.lower().str.strip()
        
        # Group by category and month
        roi_df = fact_with_cat.groupby(['category', 'month']).agg({
            'total_revenue': 'sum',
            'marketing_cost': 'sum'
        }).reset_index()
        
        # FIX: Filter out rows where marketing_cost is 0 or very small
        roi_df = roi_df[roi_df['marketing_cost'] > 1.0].copy()
        
        if roi_df.empty:
            print("⚠ No valid marketing costs found for ROI calculation")
            return pd.DataFrame(columns=['category', 'month', 'total_revenue', 'marketing_cost', 'roi_percent'])
        
        # Calculate ROI: ((Revenue - Marketing Cost) / Marketing Cost) * 100
        roi_df['roi_percent'] = (
            (roi_df['total_revenue'] - roi_df['marketing_cost']) /
            roi_df['marketing_cost'] * 100
        ).round(2)
        
        # Handle division by zero and infinity (shouldn't happen now but keep as safety)
        roi_df['roi_percent'] = roi_df['roi_percent'].replace([float('inf'), -float('inf')], 0).fillna(0)
        
        # Convert month to string format YYYY-MM
        roi_df['month'] = roi_df['month'].astype(str)
        
        # Round numeric columns
        roi_df['total_revenue'] = roi_df['total_revenue'].round(2)
        roi_df['marketing_cost'] = roi_df['marketing_cost'].round(2)
        
        print(f"✓ Marketing ROI calculated ({len(roi_df)} category-months)")
        return roi_df
        
    except Exception as e:
        print(f"✗ Error calculating marketing ROI: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=['category', 'month', 'total_revenue', 'marketing_cost', 'roi_percent'])


# ===============================================================
# 12. SAVE ALL TABLES
# ===============================================================
def save_all_tables(dim_product, dim_store, dim_customer, dim_date, fact_sales, marketing_roi):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, '../data/transformed')
    os.makedirs(output_dir, exist_ok=True)
    
    tables = {
        'dim_product.csv': dim_product,
        'dim_store.csv': dim_store,
        'dim_customer.csv': dim_customer,
        'dim_date.csv': dim_date,
        'fact_sales.csv': fact_sales,
        'marketing_roi.csv': marketing_roi
    }
    
    saved_count = 0
    
    for filename, df in tables.items():
        if df is not None and not df.empty:
            path = os.path.join(output_dir, filename)
            
            try:
                # Special handling for fact_sales - convert date to string
                if filename == 'fact_sales.csv' and 'date' in df.columns:
                    df_copy = df.copy()
                    df_copy['date'] = pd.to_datetime(df_copy['date']).dt.strftime('%Y-%m-%d')
                    df_copy.to_csv(path, index=False)
                else:
                    df.to_csv(path, index=False)
                
                print(f"✓ Saved: {filename} ({len(df)} rows)")
                saved_count += 1
            
            except PermissionError:
                print(f"✗ ERROR: {filename} is open in another program (Excel?)")
                print(f"   Please close the file and run the script again.")
            except Exception as e:
                print(f"✗ Error saving {filename}: {e}")
        else:
            print(f"⚠ Skipped: {filename} (empty or None)")
    
    print(f"\n✓ {saved_count}/6 tables saved to: {output_dir}")


# ===============================================================
# 13. MAIN ORCHESTRATION
# ===============================================================
def main():
    print("=" * 70)
    print("ETL PIPELINE - TRANSFORMATION PHASE")
    print("=" * 70)
    
    # Step 1: Load flat files
    print("\n[1/8] Loading flat files...")
    marketing_df, targets_df, shipping_df = load_flat_files()
    if marketing_df is None:
        print("✗ Cannot proceed without flat files")
        return
    
    # Step 2: Clean dataframes
    print("\n[2/8] Cleaning dataframes...")
    marketing_df, targets_df, shipping_df = clean_dataframes(marketing_df, targets_df, shipping_df)
    
    # Step 3: Currency harmonization
    print("\n[3/8] Harmonizing currency...")
    marketing_df, targets_df, shipping_df = harmonize_currency(marketing_df, targets_df, shipping_df)
    
    # Step 4: Sentiment analysis
    print("\n[4/8] Analyzing sentiment...")
    sentiment_df = analyze_sentiment()
    
    # Step 5: Create dimension tables
    print("\n[5/8] Creating dimension tables...")
    dim_product = create_dim_product(sentiment_df)
    dim_store = create_dim_store(targets_df)
    dim_customer = create_dim_customer()
    
    # Step 6: Calculate net profit & create fact_sales
    print("\n[6/8] Calculating net profit & creating fact_sales...")
    fact_sales = calculate_net_profit(marketing_df, shipping_df)
    if fact_sales is None:
        print("✗ Cannot proceed without fact_sales")
        return
    
    # Step 7: Create dim_date
    print("\n[7/8] Creating dim_date...")
    dim_date = create_dim_date()
    
    # Step 8: Calculate marketing ROI
    print("\n[8/8] Calculating marketing ROI...")
    marketing_roi = calculate_marketing_roi(fact_sales, marketing_df)
    
    if marketing_roi is None or marketing_roi.empty:
        print("⚠ WARNING: marketing_roi is empty! Debugging info:")
        print(f"   - fact_sales shape: {fact_sales.shape}")
        print(f"   - fact_sales has marketing_cost column: {'marketing_cost' in fact_sales.columns}")
        if 'marketing_cost' in fact_sales.columns:
            print(f"   - Sum of marketing_cost: {fact_sales['marketing_cost'].sum()}")
    
    # Save all tables
    print("\n[FINAL] Saving all tables...")
    save_all_tables(dim_product, dim_store, dim_customer, dim_date, fact_sales, marketing_roi)
    
    # Summary
    print("\n" + "=" * 70)
    print("TRANSFORMATION COMPLETE - SUMMARY")
    print("=" * 70)
    print(f"dim_product:    {len(dim_product) if dim_product is not None else 0:>6} rows")
    print(f"dim_store:      {len(dim_store) if dim_store is not None else 0:>6} rows")
    print(f"dim_customer:   {len(dim_customer) if dim_customer is not None else 0:>6} rows")
    print(f"dim_date:       {len(dim_date) if dim_date is not None else 0:>6} rows")
    print(f"fact_sales:     {len(fact_sales) if fact_sales is not None else 0:>6} rows")
    print(f"marketing_roi:  {len(marketing_roi) if marketing_roi is not None else 0:>6} rows")
    print("=" * 70)
    print("\n✅ Pipeline completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()