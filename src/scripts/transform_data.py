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
        products_df['price_difference_pct'] = (
            (products_df['price_difference'] / products_df['competitor_price']) * 100
        ).round(2)

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
    
    # Merge with subcategories and categories
    if subcategories_df is not None:
        # Detect column names
        subcat_id_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                              if col in subcategories_df.columns), None)
        if subcat_id_col:
            subcategories_df = subcategories_df.rename(columns={subcat_id_col: 'subcat_id'})
        
        # Detect product subcategory column
        prod_subcat_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                                if col in products_df.columns), None)
        if prod_subcat_col:
            products_df = products_df.rename(columns={prod_subcat_col: 'subcat_id'})
        
        products_df = products_df.merge(
            subcategories_df[['subcat_id', 'subcat_name', 'category_id']], 
            on='subcat_id', 
            how='left'
        )
    
    if categories_df is not None:
        products_df = products_df.merge(
            categories_df[['category_id', 'category_name']], 
            on='category_id', 
            how='left'
        )
    
    # Add sentiment
    products_df = products_df.merge(sentiment_df, on='product_id', how='left')
    products_df['avg_sentiment'] = products_df['avg_sentiment'].fillna(0)
    products_df['avg_rating'] = products_df['avg_rating'].fillna(0)
    products_df['review_count'] = products_df['review_count'].fillna(0).astype(int)
    
    # Add competitor prices
    products_df = integrate_competitor_prices(products_df)
    
    # Select final columns
    dim_product = products_df[[
        'product_id', 'product_name', 
        'subcat_id', 'subcat_name',
        'category_id', 'category_name',
        'unit_price', 'unit_cost',
        'competitor_price', 'price_difference', 'price_difference_pct',
        'avg_sentiment', 'avg_rating', 'review_count'
    ]].copy()
    
    print(f"✓ dim_product created ({len(dim_product)} products)")
    return dim_product


# ===============================================================
# 7. CREATE DIM_STORE
# ===============================================================
def create_dim_store(targets_df):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')
    
    stores_path = os.path.join(extracted_dir, 'stores.csv')
    cities_path = os.path.join(extracted_dir, 'cities.csv')
    
    if not os.path.exists(stores_path) or not os.path.exists(cities_path):
        print("✗ stores.csv or cities.csv not found → dim_store skipped")
        return None
    
    stores_df = standardize_columns(pd.read_csv(stores_path))
    cities_df = standardize_columns(pd.read_csv(cities_path))
    
   
    dim_store = stores_df.merge(
        cities_df[['city_id', 'city_name', 'region']], 
        on='city_id', 
        how='left'
    )
    
    targets_df['month'] = pd.to_datetime(targets_df['month'], errors='coerce')
    store_targets = targets_df.groupby('store_id').agg({
        'target_revenue': 'sum',
        'manager_name': 'first'
    }).reset_index()
    
    dim_store = dim_store.merge(
        store_targets, 
        on='store_id', 
        how='left'
    )
    
    
    dim_store = dim_store[[
        'store_id', 'store_name',
        'city_id', 'city_name', 'region',
        'target_revenue', 'manager_name'
    ]].copy()
    
    print(f"✓ dim_store created ({len(dim_store)} stores)")
    return dim_store


# ===============================================================
# 8. CREATE DIM_CUSTOMER
# ===============================================================
def create_dim_customer():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')
    
    customers_path = os.path.join(extracted_dir, 'customers.csv')
    cities_path = os.path.join(extracted_dir, 'cities.csv')
    
    if not os.path.exists(customers_path) or not os.path.exists(cities_path):
        print("✗ customers.csv or cities.csv not found → dim_customer skipped")
        return None
    
    customers_df = standardize_columns(pd.read_csv(customers_path))
    cities_df = standardize_columns(pd.read_csv(cities_path))
    
    
    dim_customer = customers_df.merge(
        cities_df[['city_id', 'city_name', 'region']], 
        on='city_id', 
        how='left'
    )
    
    
    dim_customer = dim_customer[[
        'customer_id', 'full_name',
        'city_id', 'city_name', 'region'
    ]].copy()
    
    print(f"✓ dim_customer created ({len(dim_customer)} customers)")
    return dim_customer


# ===============================================================
# 9. CREATE DIM_DATE
# ===============================================================
def create_dim_date(sales_df):
    
    all_dates = pd.to_datetime(sales_df['date'].dropna().unique())
    
    dim_date = pd.DataFrame({
        'date': all_dates
    })
    
   
    dim_date = dim_date.sort_values('date').reset_index(drop=True)
    
  
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['quarter'] = dim_date['date'].dt.quarter
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['month_name'] = dim_date['date'].dt.strftime('%B')
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
    dim_date['day_name'] = dim_date['date'].dt.strftime('%A')
    dim_date['week_of_year'] = dim_date['date'].dt.isocalendar().week.astype(int)
    
    
    dim_date['date'] = dim_date['date'].dt.strftime('%Y-%m-%d')
    
    print(f"✓ dim_date created ({len(dim_date)} dates)")
    return dim_date

# ===============================================================
# 10. NET PROFIT CALCULATION & FACT_SALES
# ===============================================================
def calculate_net_profit(marketing_df, shipping_df):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')

    required_files = ['sales.csv', 'products.csv', 'customers.csv', 'cities.csv', 'categories.csv']
    for f in required_files:
        if not os.path.exists(os.path.join(extracted_dir, f)):
            print(f"✗ Missing required extracted file: {f}")
            return None

    
    sales_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'sales.csv')))
    products_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'products.csv')))
    customers_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'customers.csv')))
    cities_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'cities.csv')))
    categories_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'categories.csv')))

    
    subcategories_df = None
    subcat_path = os.path.join(extracted_dir, 'subcategories.csv')
    if os.path.exists(subcat_path):
        subcategories_df = standardize_columns(pd.read_csv(subcat_path))
        
        subcat_id_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                              if col in subcategories_df.columns), None)
        if subcat_id_col and subcat_id_col != 'subcat_id':
            subcategories_df = subcategories_df.rename(columns={subcat_id_col: 'subcat_id'})
        print("✓ subcategories.csv loaded")

    if 'region' in cities_df.columns and 'region_name' not in cities_df.columns:
        cities_df = cities_df.rename(columns={'region': 'region_name'})


    sales_df['date'] = pd.to_datetime(sales_df['date'], errors='coerce')
    sales_df['month'] = sales_df['date'].dt.to_period('M')

    enriched_df = sales_df.copy()

    
    enriched_df = enriched_df.merge(products_df[['product_id', 'unit_cost']], on='product_id', how='left')


    enriched_df = enriched_df.merge(customers_df[['customer_id', 'city_id']], on='customer_id', how='left')
    enriched_df = enriched_df.merge(cities_df[['city_id', 'region_name']], on='city_id', how='left')
    enriched_df = enriched_df.merge(shipping_df[['region_name', 'shipping_cost']], on='region_name', how='left')

   
    category_added = False
    if subcategories_df is not None:
       
        prod_subcat_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                                if col in products_df.columns), None)
        
        if prod_subcat_col:
            if prod_subcat_col != 'subcat_id':
                products_df = products_df.rename(columns={prod_subcat_col: 'subcat_id'})
            
            enriched_df = enriched_df.merge(
                products_df[['product_id', 'subcat_id']], 
                on='product_id', 
                how='left'
            )
            enriched_df = enriched_df.merge(
                subcategories_df[['subcat_id', 'category_id']], 
                on='subcat_id', 
                how='left'
            )
            enriched_df = enriched_df.merge(
                categories_df[['category_id', 'category_name']], 
                on='category_id', 
                how='left'
            )
            enriched_df = enriched_df.rename(columns={'category_name': 'category'})
            category_added = True
            print("✓ Category added via subcategories")

    
    if not category_added:
        cat_col_in_products = next((col for col in ['category', 'category_name', 'category_id'] 
                                    if col in products_df.columns), None)
        
        if cat_col_in_products == 'category_id' and 'category_name' in categories_df.columns:
            temp = products_df[['product_id', 'category_id']].merge(
                categories_df[['category_id', 'category_name']], on='category_id', how='left'
            )
            temp = temp.rename(columns={'category_name': 'category'})
            enriched_df = enriched_df.merge(temp[['product_id', 'category']], on='product_id', how='left')
            category_added = True
            print("✓ Category added directly from products + categories")
        elif cat_col_in_products in ['category', 'category_name']:
            enriched_df = enriched_df.merge(
                products_df[['product_id', cat_col_in_products]].rename(columns={cat_col_in_products: 'category'}),
                on='product_id', how='left'
            )
            category_added = True
            print("✓ Category added directly from products")

    
    if not category_added:
        enriched_df['category'] = 'unknown'
        print("⚠ No category link found → using 'unknown'")

   
    enriched_df['category'] = enriched_df['category'].astype(str).str.lower().str.strip()

    marketing_df['month'] = marketing_df['date'].dt.to_period('M')
    marketing_df['category'] = marketing_df['category'].astype(str).str.lower().str.strip()

    cat_month_rev = enriched_df.groupby(['category', 'month'])['total_revenue'].sum().reset_index(name='cat_month_total')
    enriched_df = enriched_df.merge(cat_month_rev, on=['category', 'month'], how='left')
    enriched_df = enriched_df.merge(
        marketing_df[['category', 'month', 'marketing_cost_dzd']], 
        on=['category', 'month'], 
        how='left'
    )

    enriched_df['allocated_marketing_dzd'] = (
        enriched_df['total_revenue'] / enriched_df['cat_month_total']
    ) * enriched_df['marketing_cost_dzd']


    enriched_df['cost'] = enriched_df['unit_cost'] * enriched_df['quantity']
    enriched_df['shipping_cost_total'] = enriched_df['shipping_cost'] * enriched_df['quantity']
    enriched_df.fillna({
        'shipping_cost': 0, 
        'shipping_cost_total': 0,
        'allocated_marketing_dzd': 0, 
        'unit_cost': 0,
        'cost': 0
    }, inplace=True)

    enriched_df['gross_profit'] = enriched_df['total_revenue'] - enriched_df['cost']

    
    enriched_df['net_profit'] = (
        enriched_df['gross_profit'] -
        enriched_df['shipping_cost_total'] -
        enriched_df['allocated_marketing_dzd']
    ).round(2)

    enriched_df.drop(columns=['cat_month_total', 'marketing_cost_dzd', 'shipping_cost', 
                              'category', 'region_name', 'city_id', 'subcat_id', 'category_id'], 
                     errors='ignore', inplace=True)

   
    fact_sales = enriched_df[[
        'trans_id', 'date', 'store_id', 'product_id', 'customer_id',
        'quantity', 'total_revenue', 'cost', 
        'gross_profit', 'shipping_cost_total', 
        'allocated_marketing_dzd', 'net_profit'
    ]].copy()

    print(f"✓ fact_sales created ({len(fact_sales)} transactions)")
    return fact_sales

# ===============================================================
# 11. CALCULATE MARKETING ROI
# ===============================================================
def calculate_marketing_roi(fact_sales_df, marketing_df):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')
    
    try:
        products_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'products.csv')))
        categories_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'categories.csv')))
        
       
        fact_with_cat = fact_sales_df.copy()
        
        
        prod_subcat_col = next((col for col in ['subcategory_id', 'subcat_id', 'sub_category_id'] 
                                if col in products_df.columns), None)
        
        if prod_subcat_col:
            if prod_subcat_col != 'subcat_id':
                products_df = products_df.rename(columns={prod_subcat_col: 'subcat_id'})
            
            fact_with_cat = fact_with_cat.merge(
                products_df[['product_id', 'subcat_id']], 
                on='product_id', 
                how='left'
            )
            
            subcat_path = os.path.join(extracted_dir, 'subcategories.csv')
            if os.path.exists(subcat_path):
                subcategories_df = standardize_columns(pd.read_csv(subcat_path))
                subcat_id_col = next((col for col in ['subcategory_id', 'subcat_id'] 
                                      if col in subcategories_df.columns), 'subcat_id')
                if subcat_id_col != 'subcat_id':
                    subcategories_df = subcategories_df.rename(columns={subcat_id_col: 'subcat_id'})
                
                fact_with_cat = fact_with_cat.merge(
                    subcategories_df[['subcat_id', 'category_id']], 
                    on='subcat_id', 
                    how='left'
                )
        
        fact_with_cat = fact_with_cat.merge(
            categories_df[['category_id', 'category_name']], 
            on='category_id', 
            how='left'
        )
        fact_with_cat['category'] = fact_with_cat['category_name'].str.lower().str.strip()
        fact_with_cat['month'] = fact_with_cat['date'].dt.to_period('M')
        
        roi_df = fact_with_cat.groupby(['category', 'month']).agg({
            'total_revenue': 'sum',
            'allocated_marketing_dzd': 'sum'
        }).reset_index()
        roi_df['roi_percent'] = (
            (roi_df['total_revenue'] - roi_df['allocated_marketing_dzd']) /
            roi_df['allocated_marketing_dzd'] * 100
        ).round(2).replace([float('inf'), -float('inf')], 0)
        
        print(f"✓ Marketing ROI calculated ({len(roi_df)} category-months)")
        return roi_df
        
    except Exception as e:
        print(f"✗ Error calculating marketing ROI: {e}")
        return pd.DataFrame(columns=['category', 'month', 'total_revenue', 'allocated_marketing_dzd', 'roi_percent'])


# ===============================================================
# 13. SAVE ALL TABLES
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
    
    for filename, df in tables.items():
        if df is not None and not df.empty:
            path = os.path.join(output_dir, filename)
            
            try:
             
                if filename == 'dim_date.csv' and 'date' in df.columns:
                    df_copy = df.copy()
                    df_copy['date'] = df_copy['date'].astype(str)
                    df_copy.to_csv(path, index=False)
               
                elif filename == 'fact_sales.csv' and 'date' in df.columns:
                    df_copy = df.copy()
                    df_copy['date'] = pd.to_datetime(df_copy['date']).dt.strftime('%Y-%m-%d')
                    df_copy.to_csv(path, index=False)
                else:
                    df.to_csv(path, index=False)
                
                print(f"✓ Saved: {filename} ({len(df)} rows)")
            
            except PermissionError:
                print(f"✗ ERROR: {filename} is open in another program (Excel?)")
                print(f"   Please close the file and run the script again.")
            except Exception as e:
                print(f"✗ Error saving {filename}: {e}")
        else:
            print(f"⚠ Skipped: {filename} (empty or None)")
    
    print(f"\n✓ All tables saved to: {output_dir}")


# ===============================================================
# 14. MAIN ORCHESTRATION
# ===============================================================
def main():
    print("=" * 70)
    print("ETL PIPELINE - TRANSFORMATION PHASE")
    print("=" * 70)
    
    # Step 1: Load flat files
    print("\n[1/9] Loading flat files...")
    marketing_df, targets_df, shipping_df = load_flat_files()
    if marketing_df is None:
        print("✗ Cannot proceed without flat files")
        return
    
    # Step 2: Clean dataframes
    print("\n[2/9] Cleaning dataframes...")
    marketing_df, targets_df, shipping_df = clean_dataframes(marketing_df, targets_df, shipping_df)
    
    # Step 3: Currency harmonization
    print("\n[3/9] Harmonizing currency...")
    marketing_df, targets_df, shipping_df = harmonize_currency(marketing_df, targets_df, shipping_df)
    
    # Step 4: Sentiment analysis
    print("\n[4/9] Analyzing sentiment...")
    sentiment_df = analyze_sentiment()
    
    # Step 5: Create dimension tables
    print("\n[5/9] Creating dimension tables...")
    dim_product = create_dim_product(sentiment_df)
    dim_store = create_dim_store(targets_df)
    dim_customer = create_dim_customer()
    
    # Step 6: Calculate net profit & create fact_sales
    print("\n[6/9] Calculating net profit & creating fact_sales...")
    fact_sales = calculate_net_profit(marketing_df, shipping_df)
    if fact_sales is None:
        print("✗ Cannot proceed without fact_sales")
        return
    
    # Step 7: Create dim_date
    print("\n[7/9] Creating dim_date...")
    dim_date = create_dim_date(fact_sales)
    
    # Step 8: Calculate marketing ROI
    print("\n[8/9] Calculating marketing ROI...")
    marketing_roi = calculate_marketing_roi(fact_sales, marketing_df)
    
    # Step 9: Save all tables
    print("\n[9/9] Saving all tables...")
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
    print("✓ Pipeline completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
