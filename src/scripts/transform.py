
import pandas as pd
import os
import subprocess
import sys
from pathlib import Path
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ========================================================
# CONFIGURATION DES CHEMINS
# ========================================================
script_dir = Path(__file__).parent
project_root = script_dir.parent

extracted_dir = project_root / 'data' / 'extracted'
flat_files_dir = project_root / 'data' / 'flat_files'
transformed_dir = project_root / 'data' / 'transformed'

# Vérifications
if not extracted_dir.exists():
    raise FileNotFoundError(f"❌ Le dossier {extracted_dir} n'existe pas. Exécutez d'abord extract_mysql.py (Membre 1)")

transformed_dir.mkdir(parents=True, exist_ok=True)

print("=" * 70)
print("PARTIE 2 – TRANSFORMATION (MEMBRE 2) + BONUS OCR")
print("=" * 70)
print(f"📁 Projet racine   : {project_root}")
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
            timeout=300  # 5 minutes max
        )
        print(result.stdout)
        if result.returncode != 0:
            print(f"   ⚠️  Erreur OCR : {result.stderr}")
    except Exception as e:
        print(f"   ⚠️  Impossible d'exécuter OCR : {e}")
elif legacy_file.exists():
    print("   ✅ Fichier legacy_sales_2022.csv déjà existant")
else:
    print("   ⚠️  Script ocr_invoices.py introuvable - BONUS ignoré")

# ========================================================
# 1. CHARGEMENT DES DONNÉES
# ========================================================
print("\n📄 CHARGEMENT DES DONNÉES...")

# Données ERP (Membre 1)
sales_df = pd.read_csv(extracted_dir / 'sales.csv')
products_df = pd.read_csv(extracted_dir / 'products.csv')
reviews_df = pd.read_csv(extracted_dir / 'reviews.csv')
customers_df = pd.read_csv(extracted_dir / 'customers.csv')
stores_df = pd.read_csv(extracted_dir / 'stores.csv')
cities_df = pd.read_csv(extracted_dir / 'cities.csv')
categories_df = pd.read_csv(extracted_dir / 'categories.csv')
subcategories_df = pd.read_csv(extracted_dir / 'subcategories.csv')

# Fichiers Excel
marketing_df = pd.read_excel(flat_files_dir / 'marketing_expenses.xlsx')
targets_df = pd.read_excel(flat_files_dir / 'monthly_targets.xlsx')
shipping_df = pd.read_excel(flat_files_dir / 'shipping_rates.xlsx')

# Prix concurrents
competitor_file = extracted_dir / 'competitor_prices.csv'
if competitor_file.exists() and os.path.getsize(competitor_file) > 100:
    competitor_df = pd.read_csv(competitor_file)
    has_competitor = len(competitor_df) > 0
    print(f"✅ Prix concurrents : {len(competitor_df)} produits")
else:
    competitor_df = pd.DataFrame(columns=['Competitor_Product_Name', 'Competitor_Price'])
    has_competitor = False
    print("⚠️  Pas de données concurrentes")

# BONUS : Données legacy OCR
if legacy_file.exists():
    legacy_df = pd.read_csv(legacy_file)
    has_legacy = len(legacy_df) > 0
    print(f"🎯 BONUS - Factures 2022 : {len(legacy_df)} lignes")
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

# Copie
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

# Nettoyage
for df in [sales, products, reviews, customers, stores, cities, 
           categories, subcategories, marketing, targets, shipping, competitor]:
    clean_column_names(df)
    df.drop_duplicates(inplace=True)

# Conversions
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

# BONUS : Nettoyage données legacy
if has_legacy:
    legacy = legacy_df.copy()
    clean_column_names(legacy)
    legacy['date'] = pd.to_datetime(legacy['date'], errors='coerce')
    legacy['quantity'] = pd.to_numeric(legacy['quantity'], errors='coerce').fillna(1).astype(int)
    legacy['total_revenue'] = pd.to_numeric(legacy['total_revenue'], errors='coerce').fillna(0)
    
    # Attribuer des IDs fictifs pour les données legacy
    legacy['trans_id'] = range(100001, 100001 + len(legacy))
    legacy['store_id'] = 1  # Magasin principal par défaut
    legacy['product_id'] = 'P100'  # Produit par défaut
    
    # Fusionner avec sales
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

product_sentiment.to_csv(transformed_dir / 'product_sentiment.csv', index=False)

# ========================================================
# 4. INTÉGRATION PRIX CONCURRENTS
# ========================================================
print("\n🏪 INTÉGRATION PRIX CONCURRENTS...")

if has_competitor:
    competitor['match_name'] = competitor['competitor_product_name'].str.lower().str.strip()
    products['match_name'] = products['product_name'].str.lower().str.strip()
    
    products = products.merge(
        competitor[['match_name', 'competitor_price']], 
        on='match_name', 
        how='left'
    )
    products.drop('match_name', axis=1, inplace=True)
    
    products['price_difference'] = products['unit_price'] - products['competitor_price']
    products['price_difference_pct'] = (
        (products['price_difference'] / products['competitor_price']) * 100
    ).round(2)
    
    matched = products['competitor_price'].notna().sum()
    print(f"✅ {matched} produits matchés avec concurrents")
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
# 5. ENRICHISSEMENT → sales_enriched
# ========================================================
print("\n💰 ENRICHISSEMENT DES VENTES...")

sales_enriched = sales.merge(
    products[[
        'product_id', 'product_name', 'unit_price', 'unit_cost', 'subcat_id',
        'competitor_price', 'price_difference_pct', 
        'avg_sentiment', 'avg_rating'
    ]], 
    on='product_id', 
    how='left'
)

# Calculs financiers
sales_enriched['cost'] = sales_enriched['quantity'] * sales_enriched['unit_cost'].fillna(0)
sales_enriched['gross_profit'] = sales_enriched['total_revenue'] - sales_enriched['cost']

# Hiérarchie produit
sales_enriched = sales_enriched.merge(
    subcategories[['subcat_id', 'subcat_name', 'category_id']], 
    on='subcat_id', 
    how='left'
)
sales_enriched = sales_enriched.merge(
    categories[['category_id', 'category_name']], 
    on='category_id', 
    how='left'
)

# Localisation
sales_enriched = sales_enriched.merge(
    stores[['store_id', 'store_name', 'city_id']], 
    on='store_id', 
    how='left'
)
sales_enriched = sales_enriched.merge(
    cities[['city_id', 'city_name', 'region']], 
    on='city_id', 
    how='left'
)

# Client
sales_enriched = sales_enriched.merge(
    customers[['customer_id', 'full_name']], 
    on='customer_id', 
    how='left'
)

# Coût livraison
shipping_avg = shipping.groupby('region_name')['shipping_cost'].mean().reset_index()
shipping_avg.rename(columns={'region_name': 'region'}, inplace=True)
sales_enriched = sales_enriched.merge(shipping_avg, on='region', how='left')
sales_enriched['shipping_cost_total'] = sales_enriched['shipping_cost'].fillna(0) * sales_enriched['quantity']

# Allocation marketing
EXCHANGE_RATE = 135.0
marketing['marketing_cost_dzd'] = marketing['marketing_cost_usd'] * EXCHANGE_RATE
marketing['month'] = marketing['date'].dt.to_period('M')
sales_enriched['month'] = sales_enriched['date'].dt.to_period('M')

marketing_monthly = marketing.groupby(['category', 'month'])['marketing_cost_dzd'].sum().reset_index()
revenue_monthly = sales_enriched.groupby(['category_name', 'month'])['total_revenue'].sum().reset_index()
revenue_monthly.rename(columns={'category_name': 'category'}, inplace=True)

alloc = revenue_monthly.merge(marketing_monthly, on=['category', 'month'], how='left').fillna(0)
alloc['ratio'] = alloc['total_revenue'] / alloc.groupby('month')['total_revenue'].transform('sum')
alloc['allocated_marketing_dzd'] = alloc['ratio'] * alloc['marketing_cost_dzd']

sales_enriched = sales_enriched.merge(
    alloc[['category', 'month', 'allocated_marketing_dzd']], 
    left_on=['category_name', 'month'],
    right_on=['category', 'month'],
    how='left'
)
sales_enriched.drop('category', axis=1, inplace=True, errors='ignore')
sales_enriched['allocated_marketing_dzd'] = sales_enriched['allocated_marketing_dzd'].fillna(0)

# Profit net (FORMULE DU PDF)
sales_enriched['net_profit'] = (
    sales_enriched['gross_profit'] - 
    sales_enriched['shipping_cost_total'] - 
    sales_enriched['allocated_marketing_dzd']
)

# Cibles mensuelles
sales_enriched = sales_enriched.merge(
    targets[['store_id', 'month', 'target_revenue']], 
    on=['store_id', 'month'], 
    how='left'
)
sales_enriched['target_revenue'] = sales_enriched['target_revenue'].fillna(0)

# Colonnes temporelles
sales_enriched['year'] = sales_enriched['date'].dt.year
sales_enriched['quarter'] = sales_enriched['date'].dt.quarter
sales_enriched['month_num'] = sales_enriched['date'].dt.month

# ========================================================
# 6. SAUVEGARDE
# ========================================================
sales_enriched.to_csv(transformed_dir / 'sales_enriched.csv', index=False)

print(f"\n✅ FICHIER PRINCIPAL : sales_enriched.csv")
print(f"   📊 {len(sales_enriched):,} lignes × {len(sales_enriched.columns)} colonnes")

# ========================================================
# 7. RÉSUMÉ FINAL
# ========================================================
print("\n" + "=" * 70)
print("📋 FICHIERS CRÉÉS")
print("=" * 70)

for f in sorted(os.listdir(transformed_dir)):
    if f.endswith('.csv'):
        size = os.path.getsize(transformed_dir / f) / 1024
        rows = len(pd.read_csv(transformed_dir / f))
        print(f"  ✓ {f:30} {size:8.1f} KB | {rows:,} lignes")

print("\n" + "=" * 70)
print("✅ TRANSFORMATION TERMINÉE")
print("=" * 70)
print("\n📌 TÂCHES EFFECTUÉES :")
print("   1. ✅ Nettoyage données (duplicates, types, formats)")
print("   2. ✅ Analyse sentiment VADER sur 3000 reviews")
print("   3. ✅ Intégration prix concurrents (price_difference_pct)")
print("   4. ✅ Calcul Net Profit = Revenue - Cost - Shipping - Marketing")
print("   5. ✅ Harmonisation devise USD→DZD (taux: 135)")
print("   6. ✅ Enrichissement : catégories, régions, clients, sentiment")
print("   7. ✅ Allocation marketing par catégorie/mois")
print("   8. ✅ Intégration cibles mensuelles")

if has_legacy:
    print("   9.OCR")

print("=" * 70)