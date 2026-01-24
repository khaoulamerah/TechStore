import pandas as pd
import os
import subprocess
import sys
from pathlib import Path
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ========================================================
# CONFIGURATION DES CHEMINS
# ========================================================
try:
    script_dir = Path(__file__).parent
except NameError:
    cwd = Path.cwd()
    if (cwd / 'src').exists():
        script_dir = cwd / 'src'
    else:
        script_dir = cwd

project_root = script_dir.parent
extracted_dir = project_root / 'data' / 'extracted'
flat_files_dir = project_root / 'data' / 'flat_files'
transformed_dir = project_root / 'data' / 'transformed'

if not extracted_dir.exists():
    print(f"⚠️ Le dossier {extracted_dir} n'existe pas. Création du dossier vide.")
    extracted_dir.mkdir(parents=True, exist_ok=True)

transformed_dir.mkdir(parents=True, exist_ok=True)

def load_csv_safe(path, cols=None):
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception as e:
            print(f"⚠️ Erreur lecture {path}: {e}")
            return pd.DataFrame(columns=list(cols) if cols else [])
    else:
        print(f"⚠️ Fichier manquant: {path}. Création d'un DataFrame vide.")
        return pd.DataFrame(columns=list(cols) if cols else [])

print("=" * 70)
print("PARTIE 2 – TRANSFORMATION (MEMBRE 2) + CRÉATION DIM TABLES")
print("=" * 70)
print(f"📂 Projet racine   : {project_root}")
print(f"📥 Lecture depuis  : {extracted_dir}")
print(f"📤 Écriture dans   : {transformed_dir}")
print("-" * 70)

# ========================================================
# BONUS : EXÉCUTION DU SCRIPT OCR
# ========================================================
print("\n🎯 BONUS - EXTRACTION OCR DES FACTURES 2022...")

ocr_script = script_dir / 'ocr_invoices.py'
legacy_file = extracted_dir / 'legacy_sales_2022.csv'

if ocr_script.exists() and not legacy_file.exists():
    print("   🔍 Lancement du script OCR...")
    try:
        result = subprocess.run(
            [sys.executable, str(ocr_script)],
            capture_output=True,
            text=True,
            timeout=300
        )
        print("   ✔️ OCR script terminé")
        if result.stdout:
            print("   STDOUT:", result.stdout.strip())
        if result.stderr:
            print("   STDERR:", result.stderr.strip())
    except subprocess.TimeoutExpired:
        print("   ⌛ Le script OCR a expiré (timeout)")
    except Exception as e:
        print(f"   ⌛ Erreur lors de l'exécution du script OCR: {e}")

# ========================================================
# 1. CHARGEMENT DES DONNÉES
# ========================================================
print("\n📄 CHARGEMENT DES DONNÉES...")

sales_df = load_csv_safe(extracted_dir / 'sales.csv',
                         ['trans_id','date','store_id','product_id','customer_id','quantity','total_revenue'])
products_df = load_csv_safe(extracted_dir / 'products.csv',
                            ['product_id','product_name','subcat_id','unit_price','unit_cost'])
reviews_df = load_csv_safe(extracted_dir / 'reviews.csv',
                           ['review_id','product_id','customer_id','rating','review_text'])
customers_df = load_csv_safe(extracted_dir / 'customers.csv',
                             ['customer_id','full_name','city_id'])
stores_df = load_csv_safe(extracted_dir / 'stores.csv',
                          ['store_id','store_name','city_id'])
cities_df = load_csv_safe(extracted_dir / 'cities.csv',
                          ['city_id','city_name','region'])
categories_df = load_csv_safe(extracted_dir / 'categories.csv',
                              ['category_id','category_name'])
subcategories_df = load_csv_safe(extracted_dir / 'subcategories.csv',
                                 ['subcat_id','subcat_name','category_id'])

try:
    marketing_df = pd.read_excel(flat_files_dir / 'marketing_expenses.xlsx')
except Exception as e:
    print(f"⚠️ Impossible de charger marketing_expenses.xlsx: {e}")
    marketing_df = pd.DataFrame(columns=['Date','Category','Campaign_Type','Marketing_Cost_USD'])

try:
    targets_df = pd.read_excel(flat_files_dir / 'monthly_targets.xlsx')
except Exception as e:
    print(f"⚠️ Impossible de charger monthly_targets.xlsx: {e}")
    targets_df = pd.DataFrame(columns=['Store_ID','Month','Target_Revenue','Manager_Name'])

try:
    shipping_df = pd.read_excel(flat_files_dir / 'shipping_rates.xlsx')
except Exception as e:
    print(f"⚠️ Impossible de charger shipping_rates.xlsx: {e}")
    shipping_df = pd.DataFrame(columns=['region_name','provider','shipping_cost','average_delivery_days'])

competitor_file = extracted_dir / 'competitor_prices.csv'
if competitor_file.exists() and os.path.getsize(competitor_file) > 100:
    competitor_df = pd.read_csv(competitor_file)
    has_competitor = len(competitor_df) > 0
    print(f"✅ Prix concurrents : {len(competitor_df)} produits")
else:
    competitor_df = pd.DataFrame(columns=['Competitor_Product_Name', 'Competitor_Price'])
    has_competitor = False
    print("⚠️  Pas de données concurrentes")

if legacy_file.exists():
    try:
        legacy_df = pd.read_csv(legacy_file)
        has_legacy = len(legacy_df) > 0
        print(f"🎯 BONUS - Factures 2022 : {len(legacy_df)} lignes")
    except Exception as e:
        print(f"⚠️ Erreur lecture legacy file: {e}")
        legacy_df = pd.DataFrame()
        has_legacy = False
else:
    legacy_df = pd.DataFrame()
    has_legacy = False
    print("⚠️  Pas de données legacy 2022")

print(f"✅ {len(sales_df):,} lignes de ventes chargées")

# ========================================================
# 2. NETTOYAGE
# ========================================================
print("\n🧹 NETTOYAGE DES DONNÉES...")

def clean_column_names(df):
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    return df

sales = sales_df.copy()
products = products_df.copy()
reviews = reviews_df.copy()
customers = customers_df.copy()
stores = stores_df.copy()
cities = cities_df.copy()
categories = categories_df.copy()
subcategories = subcategories_df.copy()
marketing = marketing_df.copy()
targets = targets_df.copy()
shipping = shipping_df.copy()
competitor = competitor_df.copy()

for df in [sales, products, reviews, customers, stores, cities, 
           categories, subcategories, marketing, targets, shipping, competitor]:
    clean_column_names(df)
    df.drop_duplicates(inplace=True)

sales['date'] = pd.to_datetime(sales['date'], errors='coerce')
sales['quantity'] = pd.to_numeric(sales['quantity'], errors='coerce').fillna(1).astype(int)
sales['total_revenue'] = pd.to_numeric(sales['total_revenue'], errors='coerce').fillna(0)

products['unit_price'] = pd.to_numeric(products['unit_price'], errors='coerce')
products['unit_cost'] = pd.to_numeric(products['unit_cost'], errors='coerce')

marketing['date'] = pd.to_datetime(marketing['date'], errors='coerce', dayfirst=True)
marketing['marketing_cost_usd'] = pd.to_numeric(marketing['marketing_cost_usd'], errors='coerce').fillna(0)

targets['store_id'] = targets['store_id'].astype(str).str.replace(r'^[Ss]tore_?', '', regex=True).str.strip()
targets['store_id'] = pd.to_numeric(targets['store_id'], errors='coerce').astype('Int64')
targets['month'] = pd.to_datetime(targets['month'], errors='coerce', dayfirst=True).dt.to_period('M')
targets['target_revenue'] = pd.to_numeric(
    targets['target_revenue'].astype(str).str.replace(',', ''), 
    errors='coerce'
).fillna(0)

if has_legacy:
    legacy = legacy_df.copy()
    clean_column_names(legacy)
    legacy['date'] = pd.to_datetime(legacy['date'], errors='coerce')
    legacy['quantity'] = pd.to_numeric(legacy['quantity'], errors='coerce').fillna(1).astype(int)
    legacy['total_revenue'] = pd.to_numeric(legacy['total_revenue'], errors='coerce').fillna(0)
    legacy['trans_id'] = range(100001, 100001 + len(legacy))
    legacy['store_id'] = 1
    legacy['product_id'] = 'P100'
    
    legacy_subset = legacy[['trans_id', 'date', 'store_id', 'product_id', 'customer_id', 'quantity', 'total_revenue']]
    sales = pd.concat([sales, legacy_subset], ignore_index=True)
    print(f"   🎯 {len(legacy)} lignes legacy 2022 fusionnées")

print("✅ Nettoyage terminé")

# ========================================================
# 3. ANALYSE DE SENTIMENT (VADER)
# ========================================================
print("\n💬 ANALYSE DE SENTIMENT (VADER)...")

analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    if pd.isna(text) or str(text).strip() == '':
        return 0.0
    scores = analyzer.polarity_scores(str(text))
    return scores['compound']

reviews['sentiment_score'] = reviews['review_text'].apply(analyze_sentiment)

product_sentiment = reviews.groupby('product_id').agg({
    'sentiment_score': 'mean',
    'rating': 'mean',
    'review_id': 'count'
}).reset_index()
product_sentiment.columns = ['product_id', 'avg_sentiment', 'avg_rating', 'review_count']

print(f"✅ {len(product_sentiment)} produits analysés")
print(f"   📊 Score sentiment moyen : {product_sentiment['avg_sentiment'].mean():.3f}")

# ========================================================
# 4. INTÉGRATION PRIX CONCURRENTS (FIXED)
# ========================================================
print("\n🏪 INTÉGRATION PRIX CONCURRENTS...")

if has_competitor:
    # Normalize both product names for better matching
    competitor['match_name'] = competitor['competitor_product_name'].str.lower().str.strip()
    competitor['match_name'] = competitor['match_name'].str.replace(r'[^\w\s]', '', regex=True)
    
    products['match_name'] = products['product_name'].str.lower().str.strip()
    products['match_name'] = products['match_name'].str.replace(r'[^\w\s]', '', regex=True)
    
    # Direct merge first
    products = products.merge(
        competitor[['match_name', 'competitor_price']], 
        on='match_name', 
        how='left'
    )
    
    # For unmatched products, try partial matching
    unmatched_mask = products['competitor_price'].isna()
    
    if unmatched_mask.sum() > 0:
        print(f"   🔍 Trying fuzzy matching for {unmatched_mask.sum()} unmatched products...")
        
        # Create a mapping dictionary from competitor data
        comp_dict = competitor.set_index('match_name')['competitor_price'].to_dict()
        
        # Try to find partial matches
        for idx in products[unmatched_mask].index:
            product_name = products.loc[idx, 'match_name']
            
            # Look for substring matches
            for comp_name, comp_price in comp_dict.items():
                # Check if product name contains competitor name or vice versa
                if len(product_name) >= 3 and len(comp_name) >= 3:
                    if product_name in comp_name or comp_name in product_name:
                        products.loc[idx, 'competitor_price'] = comp_price
                        break
    
    products.drop('match_name', axis=1, inplace=True)
    
    products['price_difference'] = products['unit_price'] - products['competitor_price']
    products['price_difference_pct'] = (
        (products['price_difference'] / products['competitor_price']) * 100
    ).round(2)
    
    matched = products['competitor_price'].notna().sum()
    print(f"✅ {matched} produits matchés avec concurrents ({matched/len(products)*100:.1f}%)")
else:
    products['competitor_price'] = None
    products['price_difference'] = None
    products['price_difference_pct'] = None
    print("⚠️  Pas de matching effectué")

products = products.merge(
    product_sentiment[['product_id', 'avg_sentiment', 'avg_rating', 'review_count']], 
    on='product_id', 
    how='left'
)

# ========================================================
# 5. CRÉATION DES TABLES DE DIMENSION
# ========================================================
print("\n🗃️ CRÉATION DES TABLES DE DIMENSION...")

# --- DIM_PRODUCT ---
dim_product = products.merge(
    subcategories[['subcat_id', 'subcat_name', 'category_id']], 
    on='subcat_id', 
    how='left'
).merge(
    categories[['category_id', 'category_name']], 
    on='category_id', 
    how='left'
)

dim_product = dim_product[[
    'product_id', 'product_name', 
    'subcat_id', 'subcat_name',
    'category_id', 'category_name',
    'unit_price', 'unit_cost',
    'competitor_price', 'price_difference', 'price_difference_pct',
    'avg_sentiment', 'avg_rating', 'review_count'
]]

# --- DIM_STORE ---
dim_store = stores.merge(
    cities[['city_id', 'city_name', 'region']], 
    on='city_id', 
    how='left'
)

# Ajouter les targets mensuels
store_targets = targets.groupby('store_id').agg({
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
]]

# --- DIM_CUSTOMER ---
dim_customer = customers.merge(
    cities[['city_id', 'city_name', 'region']], 
    on='city_id', 
    how='left',
    suffixes=('', '_customer')
)

dim_customer = dim_customer[[
    'customer_id', 'full_name',
    'city_id', 'city_name', 'region'
]]

# --- DIM_DATE ---
all_dates = pd.to_datetime(sales['date'].dropna().unique())
dim_date = pd.DataFrame({
    'date': all_dates
})

dim_date['year'] = dim_date['date'].dt.year
dim_date['quarter'] = dim_date['date'].dt.quarter
dim_date['month'] = dim_date['date'].dt.month
dim_date['month_name'] = dim_date['date'].dt.strftime('%B')
dim_date['day'] = dim_date['date'].dt.day
dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
dim_date['day_name'] = dim_date['date'].dt.strftime('%A')
dim_date['week_of_year'] = dim_date['date'].dt.isocalendar().week

dim_date = dim_date.sort_values('date').reset_index(drop=True)

print(f"✅ Dim_Product : {len(dim_product)} lignes")
print(f"✅ Dim_Store : {len(dim_store)} lignes")
print(f"✅ Dim_Customer : {len(dim_customer)} lignes")
print(f"✅ Dim_Date : {len(dim_date)} lignes")

# ========================================================
# 6. CRÉATION DE LA TABLE DE FAITS (FIXED)
# ========================================================
print("\n💰 CRÉATION DE LA TABLE DE FAITS...")

fact_sales = sales.copy()

# Calculs financiers
fact_sales = fact_sales.merge(
    products[['product_id', 'unit_cost', 'subcat_id']], 
    on='product_id', 
    how='left'
)

# FIX: Ensure unit_cost is not null
fact_sales['unit_cost'] = fact_sales['unit_cost'].fillna(0)

fact_sales['cost'] = fact_sales['quantity'] * fact_sales['unit_cost']
fact_sales['gross_profit'] = fact_sales['total_revenue'] - fact_sales['cost']

# Coût livraison
fact_sales = fact_sales.merge(
    stores[['store_id', 'city_id']], 
    on='store_id', 
    how='left'
).merge(
    cities[['city_id', 'region']], 
    on='city_id', 
    how='left'
)

shipping_avg = shipping.groupby('region_name')['shipping_cost'].mean().reset_index()
shipping_avg.rename(columns={'region_name': 'region'}, inplace=True)
fact_sales = fact_sales.merge(shipping_avg, on='region', how='left')

# FIX: Use reasonable shipping cost per unit (not total quantity)
fact_sales['shipping_cost_total'] = fact_sales['shipping_cost'].fillna(0)

# ========================================================
# MARKETING ALLOCATION (FIXED - MAJOR CORRECTION)
# ========================================================
print("   📊 Allocation marketing...")

# Currency harmonization
EXCHANGE_RATE = 135.0
marketing['marketing_cost_dzd'] = marketing['marketing_cost_usd'] * EXCHANGE_RATE
marketing['month'] = marketing['date'].dt.to_period('M')
fact_sales['month'] = fact_sales['date'].dt.to_period('M')

# Récupérer la catégorie
fact_sales = fact_sales.merge(
    subcategories[['subcat_id', 'category_id']], 
    on='subcat_id', 
    how='left'
).merge(
    categories[['category_id', 'category_name']], 
    on='category_id', 
    how='left'
)

# FIX: Calculate TOTAL category/month revenue for proper allocation
marketing_monthly = marketing.groupby(['category', 'month'])['marketing_cost_dzd'].sum().reset_index()

# Calculate total revenue per category/month
category_month_revenue = fact_sales.groupby(['category_name', 'month'])['total_revenue'].sum().reset_index()
category_month_revenue.rename(columns={'category_name': 'category'}, inplace=True)

# Merge to get total category revenue
fact_sales = fact_sales.merge(
    category_month_revenue,
    left_on=['category_name', 'month'],
    right_on=['category', 'month'],
    how='left',
    suffixes=('', '_cat_total')
)

# Merge marketing costs
fact_sales = fact_sales.merge(
    marketing_monthly[['category', 'month', 'marketing_cost_dzd']], 
    left_on=['category_name', 'month'],
    right_on=['category', 'month'],
    how='left'
)

# FIX: Allocate marketing proportionally BUT cap per transaction
# Marketing cost per transaction = (transaction_revenue / total_category_revenue) * total_marketing_cost
fact_sales['allocated_marketing_dzd'] = (
    (fact_sales['total_revenue'] / fact_sales['total_revenue_cat_total']) * 
    fact_sales['marketing_cost_dzd']
).fillna(0)

# SAFETY: Cap marketing allocation to not exceed 30% of transaction revenue
max_marketing_pct = 0.30
fact_sales['allocated_marketing_dzd'] = fact_sales.apply(
    lambda row: min(row['allocated_marketing_dzd'], row['total_revenue'] * max_marketing_pct),
    axis=1
)

# Drop temporary columns
fact_sales.drop(['category', 'total_revenue_cat_total', 'marketing_cost_dzd'], 
                axis=1, inplace=True, errors='ignore')

# ========================================================
# NET PROFIT CALCULATION (FIXED)
# ========================================================
print("   💵 Calcul du profit net...")

# Profit net = Revenue - Product Cost - Shipping - Marketing
fact_sales['net_profit'] = (
    fact_sales['gross_profit'] - 
    fact_sales['shipping_cost_total'] - 
    fact_sales['allocated_marketing_dzd']
).round(2)

# VALIDATION: Check for unrealistic values
negative_profit_count = (fact_sales['net_profit'] < 0).sum()
negative_profit_pct = (negative_profit_count / len(fact_sales)) * 100

print(f"   ℹ️  Transactions with negative profit: {negative_profit_count} ({negative_profit_pct:.1f}%)")

if negative_profit_pct > 50:
    print(f"   ⚠️  WARNING: {negative_profit_pct:.1f}% of transactions have negative profit!")
    print("   This may indicate pricing issues or high costs.")
else:
    print(f"   ✅ Profit distribution looks reasonable")

# Sélectionner uniquement les colonnes nécessaires
fact_sales = fact_sales[[
    'trans_id', 'date', 'store_id', 'product_id', 'customer_id',
    'quantity', 'total_revenue', 'cost', 
    'gross_profit', 'shipping_cost_total', 
    'allocated_marketing_dzd', 'net_profit'
]]

print(f"✅ Fact_Sales : {len(fact_sales)} lignes")

# ========================================================
# 7. SAUVEGARDE DES FICHIERS
# ========================================================
print("\n💾 SAUVEGARDE DES FICHIERS...")

dim_product.to_csv(transformed_dir / 'Dim_Product.csv', index=False)
dim_store.to_csv(transformed_dir / 'Dim_Store.csv', index=False)
dim_customer.to_csv(transformed_dir / 'Dim_Customer.csv', index=False)
dim_date.to_csv(transformed_dir / 'Dim_Date.csv', index=False)
fact_sales.to_csv(transformed_dir / 'Fact_Sales.csv', index=False)

print("✅ Tous les fichiers sauvegardés")

# ========================================================
# 8. RÉSUMÉ FINAL
# ========================================================
print("\n" + "=" * 70)
print("📋 FICHIERS CRÉÉS DANS data/transformed/")
print("=" * 70)

for f in sorted(os.listdir(transformed_dir)):
    if f.endswith('.csv'):
        size = os.path.getsize(transformed_dir / f) / 1024
        rows = len(pd.read_csv(transformed_dir / f))
        print(f"  ✓ {f:30} {size:8.1f} KB | {rows:,} lignes")

# Final business metrics
total_revenue = fact_sales['total_revenue'].sum()
total_profit = fact_sales['net_profit'].sum()
profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

print("\n" + "=" * 70)
print("📊 MÉTRIQUES BUSINESS FINALES")
print("=" * 70)
print(f"  💰 Revenu Total:      {total_revenue:>15,.0f} DZD")
print(f"  💵 Profit Net Total:  {total_profit:>15,.0f} DZD")
print(f"  📈 Marge Profit:      {profit_margin:>15.2f} %")
print(f"  🏪 Magasins:          {fact_sales['store_id'].nunique():>15,}")
print(f"  📦 Produits:          {fact_sales['product_id'].nunique():>15,}")
print(f"  👥 Clients:           {fact_sales['customer_id'].nunique():>15,}")
print(f"  🧾 Transactions:      {len(fact_sales):>15,}")

print("\n" + "=" * 70)
print("✅ TRANSFORMATION TERMINÉE")
print("=" * 70)
print("\n📌 TÂCHES EFFECTUÉES :")
print("   1. ✅ Nettoyage données (duplicates, types, formats)")
print("   2. ✅ Analyse sentiment VADER sur reviews")
print("   3. ✅ Intégration prix concurrents (IMPROVED matching)")
print("   4. ✅ Calcul Net Profit = Revenue - Cost - Shipping - Marketing (FIXED)")
print("   5. ✅ Harmonisation devise USD→DZD (taux: 135)")
print("   6. ✅ Création des 4 tables de dimension + 1 table de faits")
print("   7. ✅ Allocation marketing par catégorie/mois (FIXED with safety cap)")
print("   8. ✅ Validation des métriques business")

if has_legacy:
    print("   9. 🎯 BONUS: OCR - Factures 2022 intégrées")
print("=" * 70)