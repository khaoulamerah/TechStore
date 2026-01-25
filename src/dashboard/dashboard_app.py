"""
TechStore Business Intelligence Dashboard
Main Streamlit Application
UPDATED: Compatible with new schema (Annual_Target, proper column names)
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
from dashboard.components.kpi_cards import display_kpi_row, fetch_global_kpis
from dashboard.components.filters import DashboardFilters
from dashboard.components import charts

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
    
    query_monthly = f"""
    SELECT 
        dd.Year || '-' || PRINTF('%02d', dd.Month) as Year_Month,
        ROUND(SUM(fs.Total_Revenue), 2) as Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Profit,
        COUNT(*) as Transaction_Count
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    GROUP BY dd.Year, dd.Month
    ORDER BY dd.Year, dd.Month
    """
    
    df_monthly = db.execute_query(query_monthly, tuple(params))
    
    if len(df_monthly) > 0:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_monthly['Year_Month'], 
            y=df_monthly['Revenue'],
            name='Revenue',
            mode='lines+markers',
            line=dict(color='#3498db', width=3),
            marker=dict(size=8)
        ))
        fig.add_trace(go.Scatter(
            x=df_monthly['Year_Month'], 
            y=df_monthly['Profit'],
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
        query_category = f"""
        SELECT 
            dp.Category_Name,
            ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue
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
        query_top_products = f"""
        SELECT 
            dp.Product_Name,
            ROUND(SUM(fs.Total_Revenue), 2) as Revenue
        FROM Fact_Sales fs
        JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
        JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
        JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
        WHERE {where_clause}
        GROUP BY dp.Product_Name
        ORDER BY Revenue DESC
        LIMIT 10
        """
        df_top_products = db.execute_query(query_top_products, tuple(params))
        
        if len(df_top_products) > 0:
            fig_products = px.bar(
                df_top_products,
                x='Revenue',
                y='Product_Name',
                orientation='h',
                color='Revenue',
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
    Fetch global KPIs with filters applied
    UPDATED: Uses Annual_Target column
    
    Args:
        db_connector: DatabaseConnector instance
        where_clause: SQL WHERE conditions
        params: Query parameters
        
    Returns:
        Dictionary with KPI values
    """
    kpis = {}
    
    # Total Revenue
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
    
    # Net Profit
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
    
    # Target Achievement - UPDATED to use Annual_Target OR Monthly_Target * 12
    query_target = f"""
    SELECT 
        ROUND(SUM(fs.Total_Revenue), 2) as Actual_Sales,
        ROUND(SUM(COALESCE(ds.Annual_Target, ds.Monthly_Target * 12)), 2) as Annual_Target,
        ROUND((SUM(fs.Total_Revenue) * 100.0 / NULLIF(SUM(COALESCE(ds.Annual_Target, ds.Monthly_Target * 12)), 0)), 2) as Achievement_Percentage
    FROM Fact_Sales fs
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE (ds.Annual_Target IS NOT NULL OR ds.Monthly_Target IS NOT NULL) AND {where_clause}
    """
    result = db_connector.execute_query(query_target, tuple(params))
    kpis['target_achievement'] = float(result['Achievement_Percentage'].iloc[0]) if result['Achievement_Percentage'].iloc[0] is not None else 0
    
    # Average Sentiment
    query_sentiment = """
    SELECT ROUND(AVG(Sentiment_Score), 3) as Avg_Sentiment
    FROM Dim_Product
    WHERE Sentiment_Score IS NOT NULL
    """
    result = db_connector.execute_query(query_sentiment)
    kpis['avg_sentiment'] = float(result['Avg_Sentiment'].iloc[0]) if result['Avg_Sentiment'].iloc[0] is not None else 0
    
    return kpis


def render_advanced_analytics(filters):
    """Render advanced analytics with complex SQL queries"""
    
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
    
    # Marketing ROI Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 💰 Marketing ROI by Category")
        query_roi = f"""
        SELECT 
            dp.Category_Name,
            ROUND(SUM(fs.Marketing_Cost), 2) as Marketing_Spend,
            ROUND(SUM(fs.Total_Revenue), 2) as Revenue_Generated,
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
    
    # Store Performance - UPDATED to use Annual_Target
    st.markdown("### 🏪 Store Performance Analysis")
    
    query_store = f"""
    SELECT 
        ds.Store_Name,
        ds.Region,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
        ROUND(COALESCE(ds.Annual_Target, ds.Monthly_Target * 12), 2) as Annual_Target,
        ROUND((SUM(fs.Total_Revenue) * 100.0 / NULLIF(COALESCE(ds.Annual_Target, ds.Monthly_Target * 12), 0)), 2) as Target_Achievement_Pct,
        COUNT(DISTINCT fs.Customer_ID) as Unique_Customers
    FROM Fact_Sales fs
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE (ds.Annual_Target IS NOT NULL OR ds.Monthly_Target IS NOT NULL) AND {where_clause}
    GROUP BY ds.Store_ID, ds.Store_Name, ds.Region, ds.Monthly_Target, ds.Annual_Target
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
    - **Member 1**: Data Extraction Engineer (MySQL + Web Scraping)
    - **Member 2**: ETL & Transformation Specialist (Pandas + OCR)
    - **Member 3**: Database Architect (Star Schema + SQL)
    - **Member 4**: Dashboard Developer (Streamlit + Visualization)
    
    ### 🛠️ Technology Stack
    - Python 3.x
    - Pandas, NumPy
    - MySQL Connector
    - BeautifulSoup (Web Scraping)
    - Tesseract OCR
    - VADER Sentiment Analysis
    - SQLite3
    - Streamlit
    - Plotly
    
    ---
    
    **Course**: Business Intelligence (BI)  
    **Level**: 4th Year Artificial Intelligence Engineering  
    **Institution**: University of 8 Mai 1945 Guelma
    """)


if __name__ == "__main__":
    main()