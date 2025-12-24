"""
Dashboard Business Intelligence - TechStore
Membre 4 : Frontend Developer
Projet : TechStore Business Intelligence

Application Streamlit pour visualiser les KPIs et analyses
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

# ====================================================================
# CONFIGURATION DE LA PAGE
# ====================================================================

st.set_page_config(
    page_title="TechStore BI Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour améliorer l'apparence
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 {
        color: #1f77b4;
        padding-bottom: 10px;
        border-bottom: 3px solid #1f77b4;
    }
    h2 {
        color: #2c3e50;
        margin-top: 20px;
    }
    .highlight {
        background-color: #ffffcc;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# ====================================================================
# CONNEXION À LA BASE DE DONNÉES
# ====================================================================

@st.cache_resource
def get_database_connection():
    """Établir une connexion réutilisable à la base de données"""
    try:
        conn = sqlite3.connect('database/techstore_dw.db', check_same_thread=False)
        return conn
    except Exception as e:
        st.error(f"❌ Erreur de connexion à la base de données: {e}")
        return None

# Connexion globale
conn = get_database_connection()

# ====================================================================
# FONCTIONS DE RÉCUPÉRATION DES DONNÉES
# ====================================================================

@st.cache_data(ttl=300)
def get_date_range():
    """Obtenir la plage de dates disponible"""
    query = """
    SELECT 
        MIN(Date) as min_date,
        MAX(Date) as max_date
    FROM Fact_Sales
    """
    df = pd.read_sql(query, conn)
    return pd.to_datetime(df['min_date'][0]), pd.to_datetime(df['max_date'][0])

@st.cache_data(ttl=300)
def get_stores():
    """Obtenir la liste des magasins"""
    query = "SELECT DISTINCT Store_ID, Store_Name FROM Dim_Store ORDER BY Store_Name"
    return pd.read_sql(query, conn)

@st.cache_data(ttl=300)
def get_categories():
    """Obtenir la liste des catégories"""
    query = "SELECT DISTINCT Category FROM Dim_Product ORDER BY Category"
    return pd.read_sql(query, conn)

def build_filter_query(base_query, date_range, stores, categories):
    """Construire une requête avec les filtres appliqués"""
    conditions = []
    params = []
    
    if date_range:
        conditions.append("f.Date BETWEEN ? AND ?")
        params.extend([date_range[0].strftime('%Y-%m-%d'), date_range[1].strftime('%Y-%m-%d')])
    
    if stores:
        placeholders = ','.join(['?'] * len(stores))
        conditions.append(f"s.Store_ID IN ({placeholders})")
        params.extend(stores)
    
    if categories:
        placeholders = ','.join(['?'] * len(categories))
        conditions.append(f"p.Category IN ({placeholders})")
        params.extend(categories)
    
    if conditions:
        where_clause = " AND " + " AND ".join(conditions)
        query = base_query.replace("WHERE 1=1", f"WHERE 1=1 {where_clause}")
    else:
        query = base_query
    
    return query, params

# ====================================================================
# EN-TÊTE DE L'APPLICATION
# ====================================================================

st.title("📊 TechStore - Business Intelligence Dashboard")
st.markdown("**Plateforme d'Analyse et de Visualisation des Données**")
st.markdown("---")

# ====================================================================
# SIDEBAR - FILTRES INTERACTIFS
# ====================================================================

st.sidebar.title("🔍 Filtres d'Analyse")
st.sidebar.markdown("Personnalisez votre analyse en sélectionnant les critères ci-dessous.")

# Filtre de date
st.sidebar.subheader("📅 Période")
min_date, max_date = get_date_range()

# Période prédéfinie ou personnalisée
period_option = st.sidebar.radio(
    "Sélection rapide:",
    ["Tout", "Dernier mois", "Dernier trimestre", "Personnalisé"]
)

if period_option == "Dernier mois":
    date_start = max_date - timedelta(days=30)
    date_end = max_date
elif period_option == "Dernier trimestre":
    date_start = max_date - timedelta(days=90)
    date_end = max_date
elif period_option == "Personnalisé":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        date_start = st.date_input("Du", min_date, min_value=min_date, max_value=max_date)
    with col2:
        date_end = st.date_input("Au", max_date, min_value=min_date, max_value=max_date)
else:
    date_start = min_date
    date_end = max_date

date_range = (pd.to_datetime(date_start), pd.to_datetime(date_end))

# Filtre de magasin
st.sidebar.subheader("🏪 Magasins")
df_stores = get_stores()
store_option = st.sidebar.radio("Sélection:", ["Tous les magasins", "Sélection personnalisée"])

if store_option == "Sélection personnalisée":
    selected_stores = st.sidebar.multiselect(
        "Choisir les magasins:",
        options=df_stores['Store_ID'].tolist(),
        format_func=lambda x: df_stores[df_stores['Store_ID'] == x]['Store_Name'].values[0],
        default=df_stores['Store_ID'].tolist()
    )
else:
    selected_stores = df_stores['Store_ID'].tolist()

# Filtre de catégorie
st.sidebar.subheader("📦 Catégories de Produits")
df_categories = get_categories()
category_option = st.sidebar.radio("Sélection:", ["Toutes les catégories", "Sélection personnalisée"], key="cat_radio")

if category_option == "Sélection personnalisée":
    selected_categories = st.sidebar.multiselect(
        "Choisir les catégories:",
        options=df_categories['Category'].tolist(),
        default=df_categories['Category'].tolist()
    )
else:
    selected_categories = df_categories['Category'].tolist()

st.sidebar.markdown("---")
st.sidebar.info(f"📊 Période analysée: {(date_end - date_start).days} jours")

# ====================================================================
# SECTION 1 : KPIs GLOBAUX
# ====================================================================

st.header("📈 Indicateurs Clés de Performance (KPIs)")

# Requête pour les KPIs principaux
kpi_query = """
SELECT 
    SUM(f.Total_Revenue) as Total_Revenue,
    SUM(f.Net_Profit) as Net_Profit,
    AVG(f.Net_Profit / NULLIF(f.Total_Revenue, 0)) * 100 as Profit_Margin,
    COUNT(DISTINCT f.Sale_ID) as Total_Orders,
    SUM(f.Total_Revenue) / NULLIF(COUNT(DISTINCT f.Sale_ID), 0) as Avg_Order_Value
FROM Fact_Sales f
JOIN Dim_Store s ON f.Store_ID = s.Store_ID
JOIN Dim_Product p ON f.Product_ID = p.Product_ID
WHERE 1=1
"""

kpi_query_filtered, params = build_filter_query(kpi_query, date_range, selected_stores, selected_categories)
df_kpis = pd.read_sql(kpi_query_filtered, conn, params=params)

# Requête pour les objectifs
target_query = """
SELECT 
    SUM(f.Total_Revenue) as Actual_Sales,
    SUM(s.Monthly_Target) as Total_Target
FROM Fact_Sales f
JOIN Dim_Store s ON f.Store_ID = s.Store_ID
JOIN Dim_Product p ON f.Product_ID = p.Product_ID
WHERE 1=1
"""

target_query_filtered, params_target = build_filter_query(target_query, date_range, selected_stores, selected_categories)
df_target = pd.read_sql(target_query_filtered, conn, params=params_target)

# Requête pour le sentiment
sentiment_query = """
SELECT AVG(p.Sentiment_Score) as Avg_Sentiment
FROM Fact_Sales f
JOIN Dim_Product p ON f.Product_ID = p.Product_ID
JOIN Dim_Store s ON f.Store_ID = s.Store_ID
WHERE 1=1 AND p.Sentiment_Score IS NOT NULL
"""

sentiment_query_filtered, params_sent = build_filter_query(sentiment_query, date_range, selected_stores, selected_categories)
df_sentiment = pd.read_sql(sentiment_query_filtered, conn, params=params_sent)

# Affichage des KPIs
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    revenue = df_kpis['Total_Revenue'].values[0]
    st.metric(
        "💰 Revenu Total",
        f"{revenue:,.0f} DZD" if not pd.isna(revenue) else "N/A"
    )

with col2:
    profit = df_kpis['Net_Profit'].values[0]
    st.metric(
        "💵 Profit Net",
        f"{profit:,.0f} DZD" if not pd.isna(profit) else "N/A"
    )

with col3:
    actual = df_target['Actual_Sales'].values[0]
    target = df_target['Total_Target'].values[0]
    achievement = (actual / target * 100) if target and target > 0 else 0
    st.metric(
        "🎯 Objectif Atteint",
        f"{achievement:.1f}%",
        delta=f"{actual - target:,.0f} DZD" if not pd.isna(target) else None
    )

with col4:
    sentiment = df_sentiment['Avg_Sentiment'].values[0]
    sentiment_emoji = "😊" if sentiment > 0.5 else "😐" if sentiment > 0 else "😞"
    st.metric(
        f"{sentiment_emoji} Sentiment",
        f"{sentiment:.2f}" if not pd.isna(sentiment) else "N/A"
    )

with col5:
    avg_order = df_kpis['Avg_Order_Value'].values[0]
    st.metric(
        "🛒 Panier Moyen",
        f"{avg_order:,.0f} DZD" if not pd.isna(avg_order) else "N/A"
    )

st.markdown("---")

# ====================================================================
# SECTION 2 : ANALYSES VISUELLES AVANCÉES
# ====================================================================

st.header("📊 Analyses Détaillées")

# ==================== Graphique 1 & 2 ====================
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📈 Évolution du Revenu (YTD)")
    
    ytd_query = """
    SELECT 
        d.Year,
        d.Month,
        d.Month_Name,
        SUM(f.Total_Revenue) as Monthly_Revenue
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date = d.Date
    JOIN Dim_Store s ON f.Store_ID = s.Store_ID
    JOIN Dim_Product p ON f.Product_ID = p.Product_ID
    WHERE 1=1
    GROUP BY d.Year, d.Month, d.Month_Name
    ORDER BY d.Year, d.Month
    """
    
    ytd_query_filtered, params_ytd = build_filter_query(ytd_query, date_range, selected_stores, selected_categories)
    df_ytd = pd.read_sql(ytd_query_filtered, conn, params=params_ytd)
    
    if not df_ytd.empty:
        df_ytd['YTD_Revenue'] = df_ytd['Monthly_Revenue'].cumsum()
        
        fig_ytd = go.Figure()
        
        fig_ytd.add_trace(go.Scatter(
            x=df_ytd['Month_Name'],
            y=df_ytd['Monthly_Revenue'],
            name='Revenu Mensuel',
            mode='lines+markers',
            line=dict(color='#3498db', width=3)
        ))
        
        fig_ytd.add_trace(go.Scatter(
            x=df_ytd['Month_Name'],
            y=df_ytd['YTD_Revenue'],
            name='Revenu Cumulé (YTD)',
            mode='lines+markers',
            line=dict(color='#e74c3c', width=3, dash='dash')
        ))
        
        fig_ytd.update_layout(
            xaxis_title="Mois",
            yaxis_title="Revenu (DZD)",
            hovermode='x unified',
            showlegend=True
        )
        
        st.plotly_chart(fig_ytd, use_container_width=True)
    else:
        st.info("Aucune donnée disponible pour cette période")

with col_right:
    st.subheader("💸 ROI Marketing par Catégorie")
    
    roi_query = """
    SELECT 
        p.Category,
        SUM(f.Total_Revenue) as Revenue,
        SUM(f.Marketing_Cost_DZD) as Marketing_Cost,
        ((SUM(f.Total_Revenue) - SUM(f.Marketing_Cost_DZD)) / NULLIF(SUM(f.Marketing_Cost_DZD), 0)) * 100 as ROI
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_ID = p.Product_ID
    JOIN Dim_Store s ON f.Store_ID = s.Store_ID
    WHERE 1=1
    GROUP BY p.Category
    HAVING SUM(f.Marketing_Cost_DZD) > 0
    ORDER BY ROI DESC
    """
    
    roi_query_filtered, params_roi = build_filter_query(roi_query, date_range, selected_stores, selected_categories)
    df_roi = pd.read_sql(roi_query_filtered, conn, params=params_roi)
    
    if not df_roi.empty:
        fig_roi = px.bar(
            df_roi,
            x='Category',
            y='ROI',
            color='ROI',
            color_continuous_scale=['red', 'yellow', 'green'],
            title="Retour sur Investissement (%)",
            labels={'ROI': 'ROI (%)', 'Category': 'Catégorie'}
        )
        
        fig_roi.add_hline(y=0, line_dash="dash", line_color="black", annotation_text="Seuil de rentabilité")
        
        st.plotly_chart(fig_roi, use_container_width=True)
    else:
        st.info("Aucune donnée de marketing disponible")

# ==================== Graphique 3 ====================
st.subheader("🏆 Top 3 Produits par Catégorie")

top_products_query = """
WITH RankedProducts AS (
    SELECT 
        p.Category,
        p.Product_Name,
        SUM(f.Quantity) as Total_Sold,
        SUM(f.Total_Revenue) as Revenue,
        ROW_NUMBER() OVER (PARTITION BY p.Category ORDER BY SUM(f.Quantity) DESC) as rank
    FROM Fact_Sales f
    JOIN Dim_Product p ON f.Product_ID = p.Product_ID
    JOIN Dim_Store s ON f.Store_ID = s.Store_ID
    WHERE 1=1
    GROUP BY p.Category, p.Product_Name
)
SELECT Category, Product_Name, Total_Sold, Revenue
FROM RankedProducts
WHERE rank <= 3
ORDER BY Category, rank
"""

top_query_filtered, params_top = build_filter_query(top_products_query, date_range, selected_stores, selected_categories)
df_top = pd.read_sql(top_query_filtered, conn, params=params_top)

if not df_top.empty:
    fig_top = px.bar(
        df_top,
        x='Product_Name',
        y='Total_Sold',
        color='Category',
        title="Meilleurs produits vendus",
        labels={'Total_Sold': 'Quantité Vendue', 'Product_Name': 'Produit'},
        text='Total_Sold'
    )
    
    fig_top.update_traces(texttemplate='%{text:.0f}', textposition='outside')
    fig_top.update_layout(showlegend=True, xaxis_tickangle=-45)
    
    st.plotly_chart(fig_top, use_container_width=True)
else:
    st.info("Aucun produit trouvé pour les filtres sélectionnés")

# ==================== Graphique 4 ====================
st.subheader("💲 Comparaison des Prix Concurrents")

price_query = """
SELECT 
    p.Product_Name,
    p.Unit_Cost as Our_Price,
    p.Competitor_Price,
    (p.Unit_Cost - p.Competitor_Price) as Price_Difference,
    CASE 
        WHEN p.Unit_Cost > p.Competitor_Price THEN 'Surpayé'
        WHEN p.Unit_Cost < p.Competitor_Price THEN 'Compétitif'
        ELSE 'Égal'
    END as Status
FROM Dim_Product p
WHERE p.Competitor_Price IS NOT NULL
ORDER BY ABS(p.Unit_Cost - p.Competitor_Price) DESC
LIMIT 15
"""

df_price = pd.read_sql(price_query, conn)

if not df_price.empty:
    fig_price = go.Figure()
    
    fig_price.add_trace(go.Bar(
        name='Notre Prix',
        x=df_price['Product_Name'],
        y=df_price['Our_Price'],
        marker_color='#3498db'
    ))
    
    fig_price.add_trace(go.Bar(
        name='Prix Concurrent',
        x=df_price['Product_Name'],
        y=df_price['Competitor_Price'],
        marker_color='#e74c3c'
    ))
    
    fig_price.update_layout(
        barmode='group',
        title="Comparaison des Prix (Top 15 différences)",
        xaxis_title="Produit",
        yaxis_title="Prix (DZD)",
        xaxis_tickangle=-45,
        showlegend=True
    )
    
    st.plotly_chart(fig_price, use_container_width=True)
    
    # Tableau des différences
    with st.expander("📋 Voir le détail des différences de prix"):
        df_price_display = df_price[['Product_Name', 'Our_Price', 'Competitor_Price', 'Price_Difference', 'Status']]
        df_price_display.columns = ['Produit', 'Notre Prix', 'Prix Concurrent', 'Différence', 'Statut']
        st.dataframe(df_price_display, use_container_width=True)
else:
    st.info("Aucune donnée de prix concurrent disponible")

st.markdown("---")

# ====================================================================
# SECTION 3 : KPIs PERSONNALISÉS
# ====================================================================

st.header("🎯 KPIs Personnalisés")

col_a, col_b, col_c = st.columns(3)

# KPI Custom 1: Marge Bénéficiaire Globale
with col_a:
    st.subheader("📊 Marge Bénéficiaire")
    profit_margin = df_kpis['Profit_Margin'].values[0]
    
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=profit_margin if not pd.isna(profit_margin) else 0,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Marge (%)"},
        delta={'reference': 20},
        gauge={
            'axis': {'range': [None, 50]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 10], 'color': "lightcoral"},
                {'range': [10, 20], 'color': "lightyellow"},
                {'range': [20, 50], 'color': "lightgreen"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 20
            }
        }
    ))
    
    fig_gauge.update_layout(height=250)
    st.plotly_chart(fig_gauge, use_container_width=True)

# KPI Custom 2: Meilleure Région
with col_b:
    st.subheader("🗺️ Performance Régionale")
    
    region_query = """
    SELECT 
        c.Region,
        SUM(f.Total_Revenue) as Revenue,
        COUNT(DISTINCT f.Sale_ID) as Orders
    FROM Fact_Sales f
    JOIN Dim_Customer c ON f.Customer_ID = c.Customer_ID
    JOIN Dim_Store s ON f.Store_ID = s.Store_ID
    JOIN Dim_Product p ON f.Product_ID = p.Product_ID
    WHERE 1=1
    GROUP BY c.Region
    ORDER BY Revenue DESC
    """
    
    region_query_filtered, params_reg = build_filter_query(region_query, date_range, selected_stores, selected_categories)
    df_region = pd.read_sql(region_query_filtered, conn, params=params_reg)
    
    if not df_region.empty:
        fig_region = px.pie(
            df_region,
            values='Revenue',
            names='Region',
            title='Répartition du revenu par région'
        )
        
        st.plotly_chart(fig_region, use_container_width=True)
    else:
        st.info("Aucune donnée régionale disponible")

# KPI Custom 3: Tendance des Ventes
with col_c:
    st.subheader("📉 Tendance Hebdomadaire")
    
    trend_query = """
    SELECT 
        d.Week,
        SUM(f.Total_Revenue) as Weekly_Revenue
    FROM Fact_Sales f
    JOIN Dim_Date d ON f.Date = d.Date
    JOIN Dim_Store s ON f.Store_ID = s.Store_ID
    JOIN Dim_Product p ON f.Product_ID = p.Product_ID
    WHERE 1=1
    GROUP BY d.Week
    ORDER BY d.Week
    LIMIT 12
    """
    
    trend_query_filtered, params_trend = build_filter_query(trend_query, date_range, selected_stores, selected_categories)
    df_trend = pd.read_sql(trend_query_filtered, conn, params=params_trend)
    
    if not df_trend.empty and len(df_trend) > 1:
        fig_trend = px.line(
            df_trend,
            x='Week',
            y='Weekly_Revenue',
            title='Évolution sur 12 semaines',
            markers=True
        )
        
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("Données insuffisantes pour afficher la tendance")

st.markdown("---")

# ====================================================================
# SECTION 4 : INSIGHTS & RECOMMANDATIONS
# ====================================================================

st.header("💡 Insights et Recommandations")

# Identifier les produits problématiques (ROI négatif)
if not df_roi.empty:
    negative_roi = df_roi[df_roi['ROI'] < 0]
    
    if not negative_roi.empty:
        st.warning(f"⚠️ **Alerte :** {len(negative_roi)} catégorie(s) avec un ROI marketing négatif")
        
        for _, row in negative_roi.iterrows():
            st.markdown(f"""
            <div class="highlight">
            <strong>{row['Category']}</strong> : ROI de {row['ROI']:.1f}%<br>
            → Coût marketing : {row['Marketing_Cost']:,.0f} DZD<br>
            → Revenu généré : {row['Revenue']:,.0f} DZD<br>
            <strong>Recommandation :</strong> Réduire ou suspendre les investissements publicitaires pour cette catégorie.
            </div>
            """, unsafe_allow_html=True)

# Identifier les produits surpayés
if not df_price.empty:
    overpriced = df_price[df_price['Status'] == 'Surpayé']
    
    if not overpriced.empty and len(overpriced) > 3:
        st.warning(f"⚠️ **Alerte Prix :** {len(overpriced)} produits sont plus chers que la concurrence")
        
        top_overpriced = overpriced.nlargest(3, 'Price_Difference')
        
        for _, row in top_overpriced.iterrows():
            st.markdown(f"""
            <div class="highlight">
            <strong>{row['Product_Name']}</strong><br>
            → Notre prix : {row['Our_Price']:,.0f} DZD<br>
            → Prix concurrent : {row['Competitor_Price']:,.0f} DZD<br>
            → Différence : +{row['Price_Difference']:,.0f} DZD<br>
            <strong>Recommandation :</strong> Aligner le prix avec la concurrence ou justifier la différence par la qualité.
            </div>
            """, unsafe_allow_html=True)

# Performance vs Objectifs
if not df_target.empty:
    if achievement < 90:
        st.error(f"❌ **Objectifs non atteints :** Seulement {achievement:.1f}% des objectifs réalisés")
        st.markdown("""
        **Actions recommandées :**
        - Analyser les catégories sous-performantes
        - Renforcer les efforts marketing sur les produits rentables
        - Revoir les objectifs de ventes avec les managers
        """)
    elif achievement >= 100:
        st.success(f"✅ **Excellente performance !** Objectifs dépassés à {achievement:.1f}%")
        st.balloons()

# ====================================================================
# FOOTER
# ====================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray; padding: 20px;'>
    <p>📊 <strong>TechStore Business Intelligence Dashboard</strong></p>
    <p>Projet BI - 4ème Année Ingénierie IA | 2025</p>
    <p>Dernière mise à jour : {}</p>
</div>
""".format(datetime.now().strftime('%d/%m/%Y %H:%M')), unsafe_allow_html=True)

# Fermer la connexion à la fin (optionnel car cache_resource la garde ouverte)
# conn.close()