"""
KPI Cards Component for TechStore Dashboard
Displays key performance indicators
"""

import streamlit as st
from typing import Dict


def display_kpi_row(kpi_data: Dict[str, float]):
    """
    Display a row of 4 KPI cards
    
    Args:
        kpi_data: Dictionary containing:
            - total_revenue: Total revenue in DZD
            - net_profit: Net profit in DZD
            - target_achievement: Target achievement percentage
            - avg_sentiment: Average sentiment score
    """
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="💰 Total Revenue",
            value=f"{kpi_data.get('total_revenue', 0):,.2f} DZD",
            delta=None
        )
    
    with col2:
        profit = kpi_data.get('net_profit', 0)
        profit_color = "normal" if profit >= 0 else "inverse"
        st.metric(
            label="📈 Net Profit",
            value=f"{profit:,.2f} DZD",
            delta=None,
            delta_color=profit_color
        )
    
    with col3:
        achievement = kpi_data.get('target_achievement', 0)
        delta_text = f"{achievement:.1f}% of target"
        delta_color = "normal" if achievement >= 100 else "inverse"
        st.metric(
            label="🎯 Target Achievement",
            value=f"{achievement:.1f}%",
            delta=delta_text,
            delta_color=delta_color
        )
    
    with col4:
        sentiment = kpi_data.get('avg_sentiment', 0)
        sentiment_emoji = "😊" if sentiment > 0.3 else "😐" if sentiment > 0 else "😞"
        
        # Determine sentiment label
        if sentiment >= 0.5:
            sentiment_label = "Very Positive"
        elif sentiment >= 0.2:
            sentiment_label = "Positive"
        elif sentiment >= 0:
            sentiment_label = "Neutral"
        elif sentiment >= -0.2:
            sentiment_label = "Negative"
        else:
            sentiment_label = "Very Negative"
        
        st.metric(
            label=f"{sentiment_emoji} Avg Sentiment",
            value=f"{sentiment:.3f}",
            delta=sentiment_label,
            help="Average customer sentiment score from product reviews (-1.0 to +1.0)"
        )


def fetch_global_kpis(db_connector):
    """
    Fetch global KPIs from database (without filters)
    
    Args:
        db_connector: DatabaseConnector instance
        
    Returns:
        Dictionary with KPI values
    """
    kpis = {}
    
    # Total Revenue
    query_revenue = """
    SELECT ROUND(SUM(Total_Revenue), 2) as Total_Revenue 
    FROM Fact_Sales
    """
    result = db_connector.execute_query(query_revenue)
    kpis['total_revenue'] = float(result['Total_Revenue'].iloc[0]) if result['Total_Revenue'].iloc[0] is not None else 0
    
    # Net Profit
    query_profit = """
    SELECT ROUND(SUM(Net_Profit), 2) as Net_Profit 
    FROM Fact_Sales
    """
    result = db_connector.execute_query(query_profit)
    kpis['net_profit'] = float(result['Net_Profit'].iloc[0]) if result['Net_Profit'].iloc[0] is not None else 0
    
    # Target Achievement (using Annual_Target from Dim_Store)
    query_target = """
    SELECT 
        ROUND(SUM(fs.Total_Revenue), 2) as Actual_Sales,
        ROUND(SUM(ds.Annual_Target), 2) as Annual_Target,
        ROUND((SUM(fs.Total_Revenue) * 100.0 / NULLIF(SUM(ds.Annual_Target), 0)), 2) as Achievement_Percentage
    FROM Fact_Sales fs
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    WHERE ds.Annual_Target IS NOT NULL
    """
    result = db_connector.execute_query(query_target)
    kpis['target_achievement'] = float(result['Achievement_Percentage'].iloc[0]) if result['Achievement_Percentage'].iloc[0] is not None else 0
    
    # Average Sentiment Score
    query_sentiment = """
    SELECT ROUND(AVG(Sentiment_Score), 3) as Avg_Sentiment
    FROM Dim_Product
    WHERE Sentiment_Score IS NOT NULL
    """
    result = db_connector.execute_query(query_sentiment)
    kpis['avg_sentiment'] = float(result['Avg_Sentiment'].iloc[0]) if result['Avg_Sentiment'].iloc[0] is not None else 0
    
    return kpis