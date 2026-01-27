import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from dashboard.utils.database_connector import DatabaseConnector, get_db_connection
from dashboard.components.kpi_cards import display_kpi_row
from dashboard.components.filters import DashboardFilters
from dashboard.components import charts
sys.path.append(str(Path(__file__).parent.parent / 'scripts'))
from sql_queries import (
    # KPI queries
    get_total_revenue_query,
    get_net_profit_query,
    get_target_achievement_query,
    get_avg_sentiment_query,
    get_avg_sentiment_global_query,
    # Time series queries
    get_monthly_trends_query,
    get_ytd_revenue_query,
    # Product queries
    get_top_selling_products_query,
    get_category_performance_query,
    # Store queries
    get_store_ranking_query,
    get_regional_performance_query,
    # Customer queries
    get_top_customers_query,
    # Profitability queries
    get_profit_margin_by_category_query,
    get_marketing_roi_query,
    # Sentiment queries
    get_sentiment_vs_sales_query,
    # Price queries
    get_price_competitiveness_query,
    # Dashboard summary
    get_dashboard_summary_query
)

st.set_page_config(
    page_title="TechStore BI Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

def init_database():
    """Initialize database connector (creates new instance each time)"""
    return DatabaseConnector()

db = init_database()

filters_manager = DashboardFilters(db)

def main():
    """Main dashboard application"""
    
    st.markdown('<h1 class="main-header"> TechStore Business Intelligence Dashboard</h1>', 
                unsafe_allow_html=True)
    
    filters = filters_manager.render_sidebar_filters()
    
    st.info(f"**Active Filters:** {filters_manager.get_filter_summary(filters)}")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        " Dashboard Overview",
        " Advanced Analytics", 
        " Raw Data Explorer",
        " About"
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
    
    st.markdown('<h2 class="section-header"> Global KPIs</h2>', unsafe_allow_html=True)
    where_clause, params = filters_manager.build_filter_sql_conditions(filters)
    kpi_data = fetch_global_kpis_filtered(db, where_clause, params)
    display_kpi_row(kpi_data)
    st.markdown("---")
    st.markdown('<h2 class="section-header"> Monthly Revenue & Profit Trends</h2>', 
                unsafe_allow_html=True)
    
    df_monthly = db.execute_query(get_monthly_trends_query(where_clause), tuple(params))
    
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
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<h3> Revenue by Category</h3>', unsafe_allow_html=True)
        df_category = db.execute_query(get_category_performance_query(where_clause), tuple(params))
        
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
        st.markdown('<h3> Top 10 Products</h3>', unsafe_allow_html=True)
        df_top_products = db.execute_query(get_top_selling_products_query(where_clause, limit=10), tuple(params))
        
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
    Fetch global KPIs with filters applied - uses query functions from sql_queries.py
    
    Args:
        db_connector: DatabaseConnector instance
        where_clause: SQL WHERE conditions
        params: Query parameters
        
    Returns:
        Dictionary with KPI values
    """
    kpis = {}
    result = db_connector.execute_query(get_total_revenue_query(where_clause), tuple(params))
    kpis['total_revenue'] = float(result['Total_Revenue'].iloc[0]) if result['Total_Revenue'].iloc[0] is not None else 0
    result = db_connector.execute_query(get_net_profit_query(where_clause), tuple(params))
    kpis['net_profit'] = float(result['Net_Profit'].iloc[0]) if result['Net_Profit'].iloc[0] is not None else 0
    result = db_connector.execute_query(get_target_achievement_query(where_clause), tuple(params))
    kpis['target_achievement'] = float(result['Achievement_Percentage'].iloc[0]) if result['Achievement_Percentage'].iloc[0] is not None else 0
    result = db_connector.execute_query(get_avg_sentiment_query(where_clause), tuple(params))
    if result.empty or result['Avg_Sentiment'].iloc[0] is None:
        fallback = db_connector.execute_query(get_avg_sentiment_global_query())
        kpis['avg_sentiment'] = float(fallback['Avg_Sentiment'].iloc[0]) if fallback['Avg_Sentiment'].iloc[0] is not None else 0
    else:
        kpis['avg_sentiment'] = float(result['Avg_Sentiment'].iloc[0])
    
    return kpis


def render_advanced_analytics(filters):
    """Render advanced analytics using query functions from sql_queries.py"""
    
    st.markdown('<h2 class="section-header"> Advanced Business Analytics</h2>', 
                unsafe_allow_html=True)
    where_clause, params = filters_manager.build_filter_sql_conditions(filters)
    st.markdown("###  Year-to-Date (YTD) Revenue Growth")
    
    df_ytd = db.execute_query(get_ytd_revenue_query(where_clause), tuple(params))
    
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
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("###  Marketing ROI by Category")
        df_roi = db.execute_query(get_marketing_roi_query(where_clause), tuple(params))
        
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
        st.markdown("###  Price Competitiveness Analysis")
        df_price = db.execute_query(get_price_competitiveness_query(where_clause, limit=10), tuple(params))
        
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

    st.markdown("###  Store Performance Analysis")
    
    df_store = db.execute_query(get_store_ranking_query(where_clause), tuple(params))
    
    if len(df_store) > 0:
        st.dataframe(
            df_store.style.background_gradient(subset=['Net_Profit'], cmap='Greens'),
            use_container_width=True,
            height=400
        )
    else:
        st.info("No store data available for the selected filters")
    
    st.markdown("---")
    st.markdown("###  Profit Margin Analysis by Category")
    
    df_margin = db.execute_query(get_profit_margin_by_category_query(where_clause), tuple(params))
    
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
    st.markdown("###  Customer Sentiment vs Sales Performance")
    
    df_sentiment = db.execute_query(get_sentiment_vs_sales_query(where_clause, limit=15), tuple(params))
    
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
   
    st.markdown("###  Regional Performance Comparison")
    
    df_regional = db.execute_query(get_regional_performance_query(where_clause), tuple(params))
    
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
        st.dataframe(df_regional, use_container_width=True)
    else:
        st.info("No regional data available")
    
    st.markdown("---")

    st.markdown("###  Top Customers")
    
    df_customers = db.execute_query(get_top_customers_query(where_clause, limit=20), tuple(params))
    
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
    
    st.markdown('<h2 class="section-header"> Raw Data Explorer</h2>', 
                unsafe_allow_html=True)
    
    st.info("View and export raw data from the Data Warehouse tables")
    tables = db.get_table_list()
    selected_table = st.selectbox(
        "Select Table to View",
        options=tables,
        index=0
    )
    
    if selected_table:
        row_count = db.get_row_count(selected_table)
        schema = db.get_table_schema(selected_table)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(" Total Rows", f"{row_count:,}")
        with col2:
            st.metric(" Columns", len(schema))
        with col3:
            st.metric(" Table", selected_table)
        
        st.markdown("---")
        with st.expander(" View Table Schema"):
            st.dataframe(schema, use_container_width=True)
        limit = st.slider("Number of rows to display", 10, 3000, 100, 10)
        df = db.get_table_data(selected_table, limit=limit)
        
        st.markdown(f"### Preview: {selected_table} (showing {len(df)} of {row_count:,} rows)")
        st.dataframe(df, use_container_width=True, height=500)
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label=" Download as CSV",
            data=csv,
            file_name=f"{selected_table}.csv",
            mime="text/csv",
            use_container_width=True
        )


def render_about_page():
    """Render about/documentation page"""
    
    st.markdown('<h2 class="section-header"> About This Dashboard</h2>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    ## TechStore Business Intelligence Platform
    
    This dashboard provides comprehensive analytics for TechStore's retail operations across Algeria.
    
    ###  Data Sources
    - **ERP System**: MySQL database with sales transactions, products, customers, and stores
    - **Marketing Data**: Excel spreadsheets tracking advertising expenses
    - **HR Data**: Monthly sales targets and store manager information
    - **Logistics**: Shipping rates by region
    - **Competitor Intelligence**: Web-scraped pricing data
    - **Legacy Archives**: OCR-digitized paper invoices from 2022
    
    ###  Architecture
    - **ETL Pipeline**: Python-based extraction, transformation, and loading
    - **Data Warehouse**: SQLite database with Star Schema design
    - **Visualization**: Streamlit + Plotly for interactive dashboards
    
    ###  Key Features
    - **Global KPIs**: Revenue, profit, target achievement, sentiment analysis
    - **Time Series Analysis**: YTD growth, monthly trends
    - **Marketing ROI**: Campaign effectiveness measurement
    - **Price Intelligence**: Competitive pricing analysis
    - **OLAP Capabilities**: Multi-dimensional filtering and drill-down
    
    ###  Project Team
    - **Sarah Djerrab & Khaoula Merah**: Data Extraction & Frontend Development
    - **Hadjer Hanani**: ETL & Transformation Specialist
    - **Tasnim Bagha**: Database Architecture & SQL
    
    ###  Technology Stack
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