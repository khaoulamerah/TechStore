"""
TechStore Business Intelligence Dashboard
Main Streamlit Application
UPDATED: Now uses queries from sql_queries.py module
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from dashboard.utils.database_connector import DatabaseConnector, get_db_connection
from dashboard.components.kpi_cards import display_kpi_row
from dashboard.components.filters import DashboardFilters
from dashboard.components import charts

# Import SQL queries from the scripts module
sys.path.append(str(Path(__file__).parent.parent / 'scripts'))
from sql_queries import (
    QUERY_TOTAL_REVENUE,
    QUERY_NET_PROFIT,
    QUERY_MONTHLY_TRENDS,
    QUERY_TOP_SELLING_PRODUCTS,
    QUERY_CATEGORY_PERFORMANCE,
    QUERY_STORE_RANKING,
    QUERY_REGIONAL_PERFORMANCE,
    QUERY_TOP_CUSTOMERS,
    QUERY_PROFIT_MARGIN_BY_CATEGORY,
    QUERY_MARKETING_ROI,
    QUERY_SENTIMENT_VS_SALES,
    QUERY_DASHBOARD_SUMMARY
)

# Page configuration
st.set_page_config(
    page_title="TechStore BI Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #3498db;
        padding-bottom: 0.5rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Initialize database connection (NO CACHING - thread-safe)
def init_database():
    """Initialize database connector (creates new instance each time)"""
    return DatabaseConnector()

# Get fresh database connection
db = init_database()

# Initialize filters with fresh DB connection
filters_manager = DashboardFilters(db)

# Main App
def main():
    """Main dashboard application"""
    
    # Header
    st.markdown('<h1 class="main-header">🏪 TechStore Business Intelligence Dashboard</h1>', 
                unsafe_allow_html=True)
    
    # Sidebar filters
    filters = filters_manager.render_sidebar_filters()
    
    # Display filter summary
    st.info(f"**Active Filters:** {filters_manager.get_filter_summary(filters)}")
    
    # Navigation tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Dashboard Overview",
        "📊 Advanced Analytics", 
        "🗂️ Raw Data Explorer",
        "ℹ️ About"
    ])
    
    with tab1:
        render_dashboard_overview(filters)
    
    with tab2:
        render_advanced_analytics(filters)
    
    with tab3:
        render_raw_data_explorer()
    
    with tab4:
        render_about_page()


def render_dashboard_overview(filters):
    """Render main dashboard with KPIs and key charts"""
    
    st.markdown('<h2 class="section-header">📊 Global KPIs</h2>', unsafe_allow_html=True)
    
    # Build filter conditions
    where_clause, params = filters_manager.build_filter_sql_conditions(filters)
    
    # Fetch and display KPIs with filters
    kpi_data = fetch_global_kpis_filtered(db, where_clause, params)
    display_kpi_row(kpi_data)
    
    st.markdown("---")
    
    # Monthly Trends with filters
    st.markdown('<h2 class="section-header">📈 Monthly Revenue & Profit Trends</h2>', 
                unsafe_allow_html=True)
    
    # Use base QUERY_MONTHLY_TRENDS with filter modifications
    query_monthly = f"""
    SELECT 
        dd.Year,
        dd.Month,
        dd.Month_Name,
        dd.Year || '-' || PRINTF('%02d', dd.Month) as Year_Month,
        COUNT(*) as Transaction_Count,
        ROUND(SUM(fs.Total_Revenue), 2) as Monthly_Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Monthly_Profit,
        ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    GROUP BY dd.Year, dd.Month, dd.Month_Name
    ORDER BY dd.Year, dd.Month
    """
    
    df_monthly = db.execute_query(query_monthly, tuple(params))
    
    if len(df_monthly) > 0:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_monthly['Year_Month'], 
            y=df_monthly['Monthly_Revenue'],
            name='Revenue',
            mode='lines+markers',
            line=dict(color='#3498db', width=3),
            marker=dict(size=8)
        ))
        fig.add_trace(go.Scatter(
            x=df_monthly['Year_Month'], 
            y=df_monthly['Monthly_Profit'],
            name='Profit',
            mode='lines+markers',
            line=dict(color='#2ecc71', width=3),
            marker=dict(size=8)
        ))
        fig.update_layout(
            title="Monthly Revenue vs Profit",
            xaxis_title="Month",
            yaxis_title="Amount (DZD)",
            height=400,
            hovermode='x unified',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for the selected filters")
    
    # Category Performance with filters
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<h3>🎯 Revenue by Category</h3>', unsafe_allow_html=True)
        # Based on QUERY_CATEGORY_PERFORMANCE with filters
        query_category = f"""
        SELECT 
            dp.Category_Name,
            COUNT(*) as Transactions,
            SUM(fs.Quantity) as Units_Sold,
            ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
            ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
            ROUND((SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)), 2) as Profit_Margin_Pct
        FROM Fact_Sales fs
        JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
        JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
        JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
        WHERE {where_clause}
        GROUP BY dp.Category_Name
        ORDER BY Total_Revenue DESC
        """
        df_category = db.execute_query(query_category, tuple(params))
        
        if len(df_category) > 0:
            fig_cat = px.pie(
                df_category, 
                values='Total_Revenue', 
                names='Category_Name',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_cat.update_traces(
                textposition='inside',
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>%{value:,.0f} DZD<br>%{percent}<extra></extra>'
            )
            fig_cat.update_layout(height=350, showlegend=True)
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.info("No data available")
    
    with col2:
        st.markdown('<h3>🏆 Top 10 Products</h3>', unsafe_allow_html=True)
        # Based on QUERY_TOP_SELLING_PRODUCTS with filters
        query_top_products = f"""
        SELECT 
            dp.Product_Name,
            dp.Category_Name,
            SUM(fs.Quantity) as Units_Sold,
            ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
            ROUND(SUM(fs.Net_Profit), 2) as Total_Profit,
            ROUND(AVG(dp.Sentiment_Score), 3) as Avg_Sentiment
        FROM Fact_Sales fs
        JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
        JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
        JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
        WHERE {where_clause}
        GROUP BY dp.Product_ID, dp.Product_Name, dp.Category_Name
        ORDER BY Total_Revenue DESC
        LIMIT 10
        """
        df_top_products = db.execute_query(query_top_products, tuple(params))
        
        if len(df_top_products) > 0:
            fig_products = px.bar(
                df_top_products,
                x='Total_Revenue',
                y='Product_Name',
                orientation='h',
                color='Total_Revenue',
                color_continuous_scale='Blues'
            )
            fig_products.update_layout(
                height=350, 
                showlegend=False,
                yaxis={'categoryorder': 'total ascending'},
                plot_bgcolor='rgba(0,0,0,0)'
            )
            fig_products.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig_products, use_container_width=True)
        else:
            st.info("No data available")


def fetch_global_kpis_filtered(db_connector, where_clause, params):
    """
    Fetch global KPIs with filters applied - uses queries from sql_queries.py
    
    Args:
        db_connector: DatabaseConnector instance
        where_clause: SQL WHERE conditions
        params: Query parameters
        
    Returns:
        Dictionary with KPI values
    """
    kpis = {}
    
    # Total Revenue - using QUERY_TOTAL_REVENUE with filters
    query_revenue = f"""
    SELECT ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue 
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """
    result = db_connector.execute_query(query_revenue, tuple(params))
    kpis['total_revenue'] = float(result['Total_Revenue'].iloc[0]) if result['Total_Revenue'].iloc[0] is not None else 0
    
    # Net Profit - using QUERY_NET_PROFIT with filters
    query_profit = f"""
    SELECT ROUND(SUM(fs.Net_Profit), 2) as Net_Profit 
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """
    result = db_connector.execute_query(query_profit, tuple(params))
    kpis['net_profit'] = float(result['Net_Profit'].iloc[0]) if result['Net_Profit'].iloc[0] is not None else 0
    
    # Target Achievement
    query_target = f"""
    SELECT 
        ROUND(SUM(fs.Total_Revenue), 2) as Actual_Sales,
        ROUND(SUM(COALESCE(ds.Annual_Target, ds.Monthly_Target * 12, 0)), 2) as Total_Target,
        ROUND(
            CASE 
                WHEN SUM(COALESCE(ds.Annual_Target, ds.Monthly_Target * 12, 0)) > 0 
                THEN (SUM(fs.Total_Revenue) * 100.0 / SUM(COALESCE(ds.Annual_Target, ds.Monthly_Target * 12, 0)))
                ELSE 0 
            END, 
            2
        ) as Achievement_Percentage
    FROM Fact_Sales fs
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """
    result = db_connector.execute_query(query_target, tuple(params))
    kpis['target_achievement'] = float(result['Achievement_Percentage'].iloc[0]) if result['Achievement_Percentage'].iloc[0] is not None else 0
    
    # Average Sentiment — compute only for products present in the filtered Fact_Sales
    query_sentiment = f"""
    SELECT ROUND(AVG(dp.Sentiment_Score), 3) as Avg_Sentiment
    FROM Dim_Product dp
    JOIN Fact_Sales fs ON dp.Product_ID = fs.Product_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    WHERE {where_clause}
    """
    result = db_connector.execute_query(query_sentiment, tuple(params))
    # If no matching rows (no sales under filters), fall back to global average from Dim_Product
    if result.empty or result['Avg_Sentiment'].iloc[0] is None:
        fallback = db_connector.execute_query("""
            SELECT ROUND(AVG(Sentiment_Score), 3) as Avg_Sentiment
            FROM Dim_Product
            WHERE Sentiment_Score IS NOT NULL
        """)
        kpis['avg_sentiment'] = float(fallback['Avg_Sentiment'].iloc[0]) if fallback['Avg_Sentiment'].iloc[0] is not None else 0
    else:
        kpis['avg_sentiment'] = float(result['Avg_Sentiment'].iloc[0])
    
    return kpis


def render_advanced_analytics(filters):
    """Render advanced analytics with complex SQL queries using sql_queries.py"""
    
    st.markdown('<h2 class="section-header">📊 Advanced Business Analytics</h2>', 
                unsafe_allow_html=True)
    
    # Build filter conditions
    where_clause, params = filters_manager.build_filter_sql_conditions(filters)
    
    # YTD Growth Analysis
    st.markdown("### 📈 Year-to-Date (YTD) Revenue Growth")
    
    query_ytd = f"""
    SELECT 
        dd.Year,
        dd.Month,
        ROUND(SUM(fs.Total_Revenue), 2) as Monthly_Revenue,
        ROUND(SUM(SUM(fs.Total_Revenue)) OVER (
            PARTITION BY dd.Year 
            ORDER BY dd.Month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2) as YTD_Revenue
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    GROUP BY dd.Year, dd.Month
    ORDER BY dd.Year, dd.Month
    """
    
    df_ytd = db.execute_query(query_ytd, tuple(params))
    
    if len(df_ytd) > 0:
        df_ytd['Period'] = df_ytd['Year'].astype(str) + '-' + df_ytd['Month'].astype(str).str.zfill(2)
        
        fig_ytd = px.line(
            df_ytd,
            x='Period',
            y='YTD_Revenue',
            color='Year',
            title='Cumulative YTD Revenue by Year',
            markers=True
        )
        fig_ytd.update_layout(
            height=400,
            plot_bgcolor='rgba(0,0,0,0)'
        )
        fig_ytd.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig_ytd.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        st.plotly_chart(fig_ytd, use_container_width=True)
    else:
        st.info("No data available for the selected filters")
    
    st.markdown("---")
    
    # Marketing ROI Analysis - using QUERY_MARKETING_ROI
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 💰 Marketing ROI by Category")
        # Using QUERY_MARKETING_ROI with filters
        query_roi = f"""
        SELECT 
            dp.Category_Name,
            ROUND(SUM(fs.Marketing_Cost), 2) as Marketing_Spend,
            ROUND(SUM(fs.Total_Revenue), 2) as Revenue_Generated,
            ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
            ROUND(
                ((SUM(fs.Total_Revenue) - SUM(fs.Marketing_Cost)) * 100.0 / 
                NULLIF(SUM(fs.Marketing_Cost), 0)), 
                2
            ) as ROI_Percentage
        FROM Fact_Sales fs
        JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
        JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
        JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
        WHERE fs.Marketing_Cost > 0 AND {where_clause}
        GROUP BY dp.Category_Name
        ORDER BY ROI_Percentage DESC
        """
        df_roi = db.execute_query(query_roi, tuple(params))
        
        if len(df_roi) > 0:
            fig_roi = px.bar(
                df_roi,
                x='Category_Name',
                y='ROI_Percentage',
                color='ROI_Percentage',
                color_continuous_scale='RdYlGn',
                title='Marketing ROI % by Category'
            )
            fig_roi.update_layout(
                height=350,
                plot_bgcolor='rgba(0,0,0,0)'
            )
            fig_roi.update_xaxes(showgrid=False)
            fig_roi.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig_roi, use_container_width=True)
        else:
            st.info("No marketing data available")
    
    with col2:
        st.markdown("### 💵 Price Competitiveness Analysis")
        query_price = f"""
        SELECT 
            dp.Product_Name,
            ROUND(AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)), 2) as Our_Avg_Price,
            dp.Competitor_Price,
            ROUND(((AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)) - dp.Competitor_Price) * 100.0 / 
                   NULLIF(dp.Competitor_Price, 0)), 2) as Price_Diff_Pct
        FROM Fact_Sales fs
        JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
        JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
        JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
        WHERE dp.Competitor_Price IS NOT NULL AND {where_clause}
        GROUP BY dp.Product_ID, dp.Product_Name, dp.Competitor_Price
        HAVING COUNT(*) >= 5
        ORDER BY Price_Diff_Pct DESC
        LIMIT 10
        """
        df_price = db.execute_query(query_price, tuple(params))
        
        if len(df_price) > 0:
            fig_price = px.bar(
                df_price,
                x='Price_Diff_Pct',
                y='Product_Name',
                orientation='h',
                color='Price_Diff_Pct',
                color_continuous_scale='RdYlGn_r',
                title='Price Difference vs Competitors (%)'
            )
            fig_price.update_layout(
                height=350,
                yaxis={'categoryorder': 'total ascending'},
                plot_bgcolor='rgba(0,0,0,0)'
            )
            fig_price.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig_price, use_container_width=True)
        else:
            st.info("No competitor data available")
    
    st.markdown("---")
    
    # Store Performance - using QUERY_STORE_RANKING
    st.markdown("### 🏪 Store Performance Analysis")
    
    # Based on QUERY_STORE_RANKING with filters
    query_store = f"""
    SELECT 
        ds.Store_Name,
        ds.City_Name,
        ds.Region,
        COUNT(*) as Transactions,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
        ROUND(ds.Monthly_Target * 12, 2) as Annual_Target,
        ROUND((SUM(fs.Total_Revenue) * 100.0 / NULLIF(ds.Monthly_Target * 12, 0)), 2) as Target_Achievement_Pct
    FROM Fact_Sales fs
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE ds.Monthly_Target IS NOT NULL AND {where_clause}
    GROUP BY ds.Store_ID, ds.Store_Name, ds.City_Name, ds.Region, ds.Monthly_Target
    ORDER BY Net_Profit DESC
    """
    df_store = db.execute_query(query_store, tuple(params))
    
    if len(df_store) > 0:
        # Display as enhanced dataframe
        st.dataframe(
            df_store.style.background_gradient(subset=['Net_Profit'], cmap='Greens'),
            use_container_width=True,
            height=400
        )
    else:
        st.info("No store data available for the selected filters")
    
    st.markdown("---")
    
    # Profit Margin Analysis - using QUERY_PROFIT_MARGIN_BY_CATEGORY
    st.markdown("### 📊 Profit Margin Analysis by Category")
    
    # Using QUERY_PROFIT_MARGIN_BY_CATEGORY with filters
    query_margin = f"""
    SELECT 
        dp.Category_Name,
        COUNT(*) as Transactions,
        SUM(fs.Quantity) as Units_Sold,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
        ROUND(SUM(fs.Product_Cost), 2) as Product_Cost,
        ROUND(SUM(fs.Shipping_Cost), 2) as Shipping_Cost,
        ROUND(SUM(fs.Marketing_Cost), 2) as Marketing_Cost,
        ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
        ROUND((SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)), 2) as Profit_Margin_Pct
    FROM Fact_Sales fs
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    WHERE {where_clause}
    GROUP BY dp.Category_Name
    ORDER BY Profit_Margin_Pct DESC
    """
    df_margin = db.execute_query(query_margin, tuple(params))
    
    if len(df_margin) > 0:
        fig_margin = px.bar(
            df_margin,
            x='Category_Name',
            y='Profit_Margin_Pct',
            color='Profit_Margin_Pct',
            color_continuous_scale='RdYlGn',
            title='Profit Margin % by Category'
        )
        fig_margin.update_layout(
            height=400,
            xaxis_title="Category",
            yaxis_title="Profit Margin (%)",
            plot_bgcolor='rgba(0,0,0,0)'
        )
        fig_margin.update_xaxes(showgrid=False)
        fig_margin.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        st.plotly_chart(fig_margin, use_container_width=True)
    else:
        st.info("No data available")
    
    st.markdown("---")
    
    # Sentiment vs Sales Analysis - using QUERY_SENTIMENT_VS_SALES
    st.markdown("### 😊 Customer Sentiment vs Sales Performance")
    
    # Using QUERY_SENTIMENT_VS_SALES with filters
    query_sentiment = f"""
    SELECT 
        dp.Product_Name,
        dp.Category_Name,
        ROUND(dp.Sentiment_Score, 3) as Sentiment_Score,
        SUM(fs.Quantity) as Units_Sold,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
        ROUND(AVG(fs.Total_Revenue / NULLIF(fs.Quantity, 0)), 2) as Avg_Price
    FROM Fact_Sales fs
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    WHERE dp.Sentiment_Score IS NOT NULL AND {where_clause}
    GROUP BY dp.Product_ID, dp.Product_Name, dp.Category_Name, dp.Sentiment_Score
    HAVING SUM(fs.Quantity) >= 10
    ORDER BY Units_Sold DESC
    LIMIT 15
    """
    df_sentiment = db.execute_query(query_sentiment, tuple(params))
    
    if len(df_sentiment) > 0:
        fig_sentiment = px.scatter(
            df_sentiment,
            x='Sentiment_Score',
            y='Units_Sold',
            size='Total_Revenue',
            color='Category_Name',
            hover_data=['Product_Name', 'Total_Revenue'],
            title='Sentiment Score vs Units Sold (bubble size = revenue)'
        )
        fig_sentiment.update_layout(
            height=500,
            xaxis_title="Sentiment Score",
            yaxis_title="Units Sold",
            plot_bgcolor='rgba(0,0,0,0)'
        )
        fig_sentiment.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig_sentiment.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        st.plotly_chart(fig_sentiment, use_container_width=True)
    else:
        st.info("No sentiment data available for the selected filters")
    
    st.markdown("---")
    
    # Regional Performance - using QUERY_REGIONAL_PERFORMANCE
    st.markdown("### 🌍 Regional Performance Comparison")
    
    # Using QUERY_REGIONAL_PERFORMANCE with filters
    query_regional = f"""
    SELECT 
        ds.Region,
        COUNT(DISTINCT ds.Store_ID) as Store_Count,
        COUNT(*) as Transactions,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
        ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value
    FROM Fact_Sales fs
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    GROUP BY ds.Region
    ORDER BY Total_Revenue DESC
    """
    df_regional = db.execute_query(query_regional, tuple(params))
    
    if len(df_regional) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_regional_revenue = px.bar(
                df_regional,
                x='Region',
                y='Total_Revenue',
                color='Total_Revenue',
                color_continuous_scale='Blues',
                title='Revenue by Region'
            )
            fig_regional_revenue.update_layout(
                height=350,
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)'
            )
            fig_regional_revenue.update_xaxes(showgrid=False)
            fig_regional_revenue.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig_regional_revenue, use_container_width=True)
        
        with col2:
            fig_regional_profit = px.bar(
                df_regional,
                x='Region',
                y='Net_Profit',
                color='Net_Profit',
                color_continuous_scale='Greens',
                title='Profit by Region'
            )
            fig_regional_profit.update_layout(
                height=350,
                showlegend=False,
                plot_bgcolor='rgba(0,0,0,0)'
            )
            fig_regional_profit.update_xaxes(showgrid=False)
            fig_regional_profit.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
            st.plotly_chart(fig_regional_profit, use_container_width=True)
        
        # Display regional table
        st.dataframe(df_regional, use_container_width=True)
    else:
        st.info("No regional data available")
    
    st.markdown("---")
    
    # Top Customers Analysis - using QUERY_TOP_CUSTOMERS
    st.markdown("### 👥 Top Customers")
    
    # Using QUERY_TOP_CUSTOMERS with filters
    query_customers = f"""
    SELECT 
        dc.Customer_Name,
        dc.City_Name,
        dc.Region,
        COUNT(fs.Sale_ID) as Purchase_Count,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Spent,
        ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value,
        MAX(dd.Full_Date) as Last_Purchase_Date
    FROM Fact_Sales fs
    JOIN Dim_Customer dc ON fs.Customer_ID = dc.Customer_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    GROUP BY dc.Customer_ID, dc.Customer_Name, dc.City_Name, dc.Region
    ORDER BY Total_Spent DESC
    LIMIT 20
    """
    df_customers = db.execute_query(query_customers, tuple(params))
    
    if len(df_customers) > 0:
        st.dataframe(
            df_customers.style.background_gradient(subset=['Total_Spent'], cmap='YlOrRd'),
            use_container_width=True,
            height=400
        )
    else:
        st.info("No customer data available")


def render_raw_data_explorer():
    """Render raw data table viewer"""
    
    st.markdown('<h2 class="section-header">🗂️ Raw Data Explorer</h2>', 
                unsafe_allow_html=True)
    
    st.info("View and export raw data from the Data Warehouse tables")
    
    # Get all tables
    tables = db.get_table_list()
    
    # Table selector
    selected_table = st.selectbox(
        "Select Table to View",
        options=tables,
        index=0
    )
    
    if selected_table:
        # Get table info
        row_count = db.get_row_count(selected_table)
        schema = db.get_table_schema(selected_table)
        
        # Display metadata
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total Rows", f"{row_count:,}")
        with col2:
            st.metric("📋 Columns", len(schema))
        with col3:
            st.metric("💾 Table", selected_table)
        
        st.markdown("---")
        
        # Schema display
        with st.expander("🔍 View Table Schema"):
            st.dataframe(schema, use_container_width=True)
        
        # Row limit selector
        limit = st.slider("Number of rows to display", 10, 1000, 100, 10)
        
        # Fetch and display data
        df = db.get_table_data(selected_table, limit=limit)
        
        st.markdown(f"### Preview: {selected_table} (showing {len(df)} of {row_count:,} rows)")
        st.dataframe(df, use_container_width=True, height=500)
        
        # Download button
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"{selected_table}.csv",
            mime="text/csv",
            use_container_width=True
        )


def render_about_page():
    """Render about/documentation page"""
    
    st.markdown('<h2 class="section-header">ℹ️ About This Dashboard</h2>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    ## TechStore Business Intelligence Platform
    
    This dashboard provides comprehensive analytics for TechStore's retail operations across Algeria.
    
    ### 📊 Data Sources
    - **ERP System**: MySQL database with sales transactions, products, customers, and stores
    - **Marketing Data**: Excel spreadsheets tracking advertising expenses
    - **HR Data**: Monthly sales targets and store manager information
    - **Logistics**: Shipping rates by region
    - **Competitor Intelligence**: Web-scraped pricing data
    - **Legacy Archives**: OCR-digitized paper invoices from 2022
    
    ### 🏗️ Architecture
    - **ETL Pipeline**: Python-based extraction, transformation, and loading
    - **Data Warehouse**: SQLite database with Star Schema design
    - **Visualization**: Streamlit + Plotly for interactive dashboards
    
    ### ⭐ Key Features
    - **Global KPIs**: Revenue, profit, target achievement, sentiment analysis
    - **Time Series Analysis**: YTD growth, monthly trends
    - **Marketing ROI**: Campaign effectiveness measurement
    - **Price Intelligence**: Competitive pricing analysis
    - **OLAP Capabilities**: Multi-dimensional filtering and drill-down
    
    ### 👥 Project Team
    - **Sarah Djerrab & Khaoula Merah**: Data Extraction & Frontend Development
    - **Hadjer Hanani**: ETL & Transformation Specialist
    - **Tasnim Bagha**: Database Architecture & SQL
    
    ### 🛠️ Technology Stack
    - Python 3.x, Pandas, NumPy
    - MySQL Connector, BeautifulSoup (Web Scraping)
    - Tesseract OCR, VADER Sentiment Analysis
    - SQLite3, Streamlit, Plotly
    
    ---
    
    **Course**: Business Intelligence (BI)  
    **Level**: 4th Year Artificial Intelligence Engineering  
    **Institution**: University of 8 Mai 1945 Guelma
    
    **GitHub**: [https://github.com/khaoulamerah/TechStore.git](https://github.com/khaoulamerah/TechStore.git)
    """)


if __name__ == "__main__":
    main()