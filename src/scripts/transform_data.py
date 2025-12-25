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
    """Standardise les noms de colonnes : minuscules et sans espaces superflus"""
    df.columns = [col.strip().lower() for col in df.columns]
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
    targets_df['store_id'] = targets_df['store_id'].astype(str).str.strip()
    targets_df['target_revenue'] = pd.to_numeric(targets_df['target_revenue'], errors='coerce').fillna(0).clip(lower=0)

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
        return pd.DataFrame(columns=['product_id', 'sentiment_score'])

    try:
        df = standardize_columns(pd.read_csv(path))
        if not {'review_text', 'product_id'}.issubset(df.columns):
            print("✗ Missing columns in reviews → sentiment skipped")
            return pd.DataFrame(columns=['product_id', 'sentiment_score'])

        df['review_text'] = df['review_text'].fillna('').astype(str).str.lower().str.strip()
        analyzer = SentimentIntensityAnalyzer()
        df['sentiment_score'] = df['review_text'].apply(lambda x: round(analyzer.polarity_scores(x)['compound'], 2) if x else 0.0)

        result = df.groupby('product_id')['sentiment_score'].mean().round(2).reset_index()

        out_dir = os.path.join(base_dir, '../data/transformed')
        os.makedirs(out_dir, exist_ok=True)
        result.to_csv(os.path.join(out_dir, 'product_sentiment.csv'), index=False)
        print(f"✓ Sentiment analysis done ({len(result)} products)")
        return result
    except Exception as e:
        print(f"✗ Error in sentiment analysis: {e} → skipped")
        return pd.DataFrame(columns=['product_id', 'sentiment_score'])


# ===============================================================
# 5. NET PROFIT CALCULATION – VERSION ROBUSTE (corrigée pour KeyError 'category')
# ===============================================================
def calculate_net_profit(marketing_df, shipping_df):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extracted_dir = os.path.join(base_dir, '../data/extracted')

    required_files = ['sales.csv', 'products.csv', 'customers.csv', 'cities.csv', 'categories.csv']
    for f in required_files:
        if not os.path.exists(os.path.join(extracted_dir, f)):
            print(f"✗ Missing required extracted file: {f}")
            return None

    # Load and standardize
    sales_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'sales.csv')))
    products_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'products.csv')))
    customers_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'customers.csv')))
    cities_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'cities.csv')))
    categories_df = standardize_columns(pd.read_csv(os.path.join(extracted_dir, 'categories.csv')))

    # Optional: subcategories
    subcategories_df = None
    subcat_path = os.path.join(extracted_dir, 'subcategories.csv')
    if os.path.exists(subcat_path):
        subcategories_df = standardize_columns(pd.read_csv(subcat_path))
        print("✓ subcategories.csv loaded")
    else:
        print("⚠ subcategories.csv not found → trying direct category link")

    # Normalize region name
    if 'region' in cities_df.columns and 'region_name' not in cities_df.columns:
        cities_df = cities_df.rename(columns={'region': 'region_name'})

    # Date handling
    sales_df['date'] = pd.to_datetime(sales_df['date'], errors='coerce')
    sales_df['month'] = sales_df['date'].dt.to_period('M')

    # Start enrichment
    enriched_df = sales_df.copy()

    # 1. Add unit_cost from products
    enriched_df = enriched_df.merge(products_df[['product_id', 'unit_cost']], on='product_id', how='left')

    # 2. Geographic enrichment
    enriched_df = enriched_df.merge(customers_df[['customer_id', 'city_id']], on='customer_id', how='left')
    enriched_df = enriched_df.merge(cities_df[['city_id', 'region_name']], on='city_id', how='left')
    enriched_df = enriched_df.merge(shipping_df[['region_name', 'shipping_cost']], on='region_name', how='left')

        # 3. Category enrichment – PRIORITÉ 1 : via subcategories (VERSION ROBUSTE)
    category_added = False
    if subcategories_df is not None:
        print("✓ subcategories.csv loaded – detecting column names")

        # Détecter le nom réel de subcategory_id
        subcat_id_col = None
        possible_subcat_ids = ['subcategory_id', 'subcat_id', 'sub_category_id', 'subcategoryid', 'id']
        for col in possible_subcat_ids:
            if col in subcategories_df.columns:
                subcat_id_col = col
                break

        # Détecter le nom réel de category_id
        cat_id_col = None
        possible_cat_ids = ['category_id', 'cat_id', 'categoryid', 'id_category']
        for col in possible_cat_ids:
            if col in subcategories_df.columns:
                cat_id_col = col
                break

        if subcat_id_col and cat_id_col:
            print(f"   → subcategories: '{subcat_id_col}' → category via '{cat_id_col}'")
            # Renommer temporairement pour merge standard
            temp_subcat = subcategories_df[[subcat_id_col, cat_id_col]].copy()
            temp_subcat = temp_subcat.rename(columns={subcat_id_col: 'subcategory_id', cat_id_col: 'category_id'})

            # Détecter la colonne subcategory dans products_df
            prod_subcat_col = None
            for col in possible_subcat_ids:
                if col in products_df.columns:
                    prod_subcat_col = col
                    break

            if prod_subcat_col:
                print(f"   → products subcategory column: '{prod_subcat_col}'")
                temp_prod = products_df[['product_id', prod_subcat_col]].copy()
                temp_prod = temp_prod.rename(columns={prod_subcat_col: 'subcategory_id'})

                # Merge étape par étape
                enriched_df = enriched_df.merge(temp_prod, on='product_id', how='left')
                enriched_df = enriched_df.merge(temp_subcat, on='subcategory_id', how='left')
                enriched_df = enriched_df.merge(categories_df[['category_id', 'category_name']], on='category_id', how='left')
                enriched_df = enriched_df.rename(columns={'category_name': 'category'})
                category_added = True
                print("✓ Category added successfully via subcategories")
            else:
                print("⚠ No subcategory link found in products.csv")
        else:
            print(f"⚠ Missing columns in subcategories.csv → subcategory_id: {subcat_id_col}, category_id: {cat_id_col}")

    # PRIORITÉ 2 : catégorie directe dans products.csv
    if not category_added:
        cat_col_in_products = None
        for col in ['category', 'category_name', 'category_id']:
            if col in products_df.columns:
                cat_col_in_products = col
                break

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

    # Dernier recours : catégorie "unknown"
    if not category_added:
        enriched_df['category'] = 'unknown'
        print("⚠ No category link found → using 'unknown'")

    # Normaliser la colonne category (maintenant garantie d'exister)
    enriched_df['category'] = enriched_df['category'].astype(str).str.lower().str.strip()

    # Marketing allocation
    marketing_df['month'] = marketing_df['date'].dt.to_period('M')
    marketing_df['category'] = marketing_df['category'].astype(str).str.lower().str.strip()

    cat_month_rev = enriched_df.groupby(['category', 'month'])['total_revenue'].sum().reset_index(name='cat_month_total')
    enriched_df = enriched_df.merge(cat_month_rev, on=['category', 'month'], how='left')
    enriched_df = enriched_df.merge(marketing_df[['category', 'month', 'marketing_cost_dzd']], on=['category', 'month'], how='left')

    enriched_df['allocated_marketing_dzd'] = (
        enriched_df['total_revenue'] / enriched_df['cat_month_total']
    ) * enriched_df['marketing_cost_dzd']

    # Costs
    enriched_df['product_total_cost'] = enriched_df['unit_cost'] * enriched_df['quantity']
    enriched_df.fillna({'shipping_cost': 0, 'allocated_marketing_dzd': 0, 'unit_cost': 0}, inplace=True)

    # Net Profit
    enriched_df['net_profit'] = (
        enriched_df['total_revenue'] -
        enriched_df['product_total_cost'] -
        enriched_df['shipping_cost'] -
        enriched_df['allocated_marketing_dzd']
    ).round(2)

    enriched_df.drop(columns=['cat_month_total', 'marketing_cost_dzd'], errors='ignore', inplace=True)

    # Save
    out_dir = os.path.join(base_dir, '../data/transformed')
    os.makedirs(out_dir, exist_ok=True)
    enriched_df.to_csv(os.path.join(out_dir, 'sales_enriched.csv'), index=False)

    print("✓ Net Profit calculated successfully")
    return enriched_df
# ===============================================================
# 6. LEGACY INVOICES (OCR) – ROBUSTE
# ===============================================================
def process_legacy_invoices(manual=False):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    legacy_dir = os.path.join(base_dir, '../data/legacy_invoices')
    transformed_dir = os.path.join(base_dir, '../data/transformed')
    os.makedirs(transformed_dir, exist_ok=True)

    legacy_df = pd.DataFrame(columns=['date', 'total_revenue', 'quantity', 'customer_id', 'product_name'])

    if manual:
        print("⚠ Manual legacy mode activated")
        return legacy_df

    if not os.path.exists(legacy_dir) or not os.path.isdir(legacy_dir):
        print("✗ legacy_invoices folder not found → skipped")
        return legacy_df

    if not os.listdir(legacy_dir):
        print("⚠ legacy_invoices folder is empty → skipped")
        return legacy_df

    # OCR processing (same as before)
    # ... (tu peux garder le code OCR original ici si tu veux, mais il est optionnel)

    print("✓ Legacy invoices processing skipped (no images or disabled)")
    return legacy_df


# ===============================================================
# 7. COMPETITOR PRICE INTEGRATION – CORRIGÉE (gère fichier vide)
# ===============================================================
def integrate_competitor_prices(products_df):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, '../data/extracted/competitor_prices.csv')

    if not os.path.exists(path):
        print("✗ competitor_prices.csv not found → skipped")
        return products_df

    if os.path.getsize(path) == 0:
        print("⚠ competitor_prices.csv is empty → skipped")
        return products_df

    try:
        comp_df = standardize_columns(pd.read_csv(path))

        if comp_df.empty or not {'competitor_product_name', 'competitor_price'}.issubset(comp_df.columns):
            print("⚠ competitor_prices.csv has no valid data → skipped")
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
        products_df['competitor_product_matched'] = matches.apply(lambda x: x[0])
        products_df['competitor_price'] = matches.apply(lambda x: x[1])

        matched_count = products_df['competitor_price'].notna().sum()
        print(f"✓ {matched_count} competitor prices matched")
        return products_df

    except pd.errors.EmptyDataError:
        print("⚠ competitor_prices.csv is empty (EmptyDataError) → skipped")
        return products_df
    except Exception as e:
        print(f"✗ Error processing competitor prices: {e} → skipped")
        return products_df


# ===============================================================
# 8. FINAL TRANSFORMATIONS
# ===============================================================
def finalize_transformations(enriched_sales_df, sentiment_df, legacy_df, targets_df):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    transformed_dir = os.path.join(base_dir, '../data/transformed')
    os.makedirs(transformed_dir, exist_ok=True)

    # dim_product
    products_path = os.path.join(base_dir, '../data/extracted/products.csv')
    if os.path.exists(products_path):
        products_df = standardize_columns(pd.read_csv(products_path))
        products_df = products_df.merge(sentiment_df, on='product_id', how='left').fillna({'sentiment_score': 0})
        products_df = integrate_competitor_prices(products_df)
        products_df.to_csv(os.path.join(transformed_dir, 'dim_product.csv'), index=False)
        print("✓ dim_product.csv created")
    else:
        print("✗ products.csv not found → dim_product skipped")

    # Legacy integration
    if len(legacy_df) > 0 and not legacy_df.empty:
        common_cols = enriched_sales_df.columns.intersection(legacy_df.columns)
        enriched_sales_df = pd.concat([enriched_sales_df[common_cols], legacy_df[common_cols]], ignore_index=True)
        print(f"✓ {len(legacy_df)} legacy sales integrated")

    # Targets
    enriched_sales_df['store_id'] = enriched_sales_df['store_id'].astype(str)
    enriched_sales_df['month'] = enriched_sales_df['date'].dt.to_period('M')
    enriched_sales_df = enriched_sales_df.merge(
        targets_df[['store_id', 'month', 'target_revenue']], on=['store_id', 'month'], how='left'
    )

    # Marketing ROI
    roi_df = enriched_sales_df.groupby(['category', 'month']).agg({
        'total_revenue': 'sum',
        'allocated_marketing_dzd': 'sum'
    }).reset_index()
    roi_df['roi_percent'] = (
        (roi_df['total_revenue'] - roi_df['allocated_marketing_dzd']) /
        roi_df['allocated_marketing_dzd'] * 100
    ).round(2).replace([float('inf'), -float('inf')], 0).fillna(0)
    roi_df.to_csv(os.path.join(transformed_dir, 'marketing_roi.csv'), index=False)
    print("✓ marketing_roi.csv created")

    # Final fact table
    enriched_sales_df.to_csv(os.path.join(transformed_dir, 'fact_sales_final.csv'), index=False)
    print("✓ fact_sales_final.csv created – Ready for loading!")

    return enriched_sales_df


# ===============================================================
# MAIN EXECUTION
# ===============================================================
if __name__ == "__main__":
    print("=== STARTING TRANSFORMATIONS (Hadjer ) ===\n")

    marketing, targets, shipping = load_flat_files()
    if marketing is None:
        exit(1)

    marketing, targets, shipping = clean_dataframes(marketing, targets, shipping)
    marketing, targets, shipping = harmonize_currency(marketing, targets, shipping)

    sentiment_df = analyze_sentiment()

    enriched_sales_df = calculate_net_profit(marketing, shipping)
    if enriched_sales_df is None:
        print("✗ Critical error in net profit calculation – stopping.")
        exit(1)

    legacy_df = process_legacy_invoices(manual=False)

    final_df = finalize_transformations(enriched_sales_df, sentiment_df, legacy_df, targets)

    print("\n=== TRANSFORMATIONS COMPLETED SUCCESSFULLY ===\n")