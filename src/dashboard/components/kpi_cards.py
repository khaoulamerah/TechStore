"""
KPI Card Components for Dashboard
Displays global business metrics
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any

def display_kpi_row(kpi_data: Dict[str, Any]):
    """
    Display a row of KPI cards
    
    Args:
        kpi_data: Dictionary containing KPI values
            {
                'total_revenue': float,
                'net_profit': float,
                'target_achievement': float,
                'avg_sentiment': float
            }
    """
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        display_revenue_kpi(kpi_data.get('total_revenue', 0))
    
    with col2:
        display_profit_kpi(kpi_data.get('net_profit', 0))
    
    with col3:
        display_target_kpi(kpi_data.get('target_achievement', 0))
    
    with col4:
        display_sentiment_kpi(kpi_data.get('avg_sentiment', 0))


def display_revenue_kpi(revenue: float):
    """Display Total Revenue KPI card"""
    st.metric(
        label="💰 Total Revenue",
        value=f"{revenue:,.0f} DZD",
        delta=None,
        help="Total revenue from all sales transactions"
    )


def display_profit_kpi(profit: float):
    """Display Net Profit KPI card"""
    st.metric(
        label="💵 Net Profit",
        value=f"{profit:,.0f} DZD",
        delta=None,
        help="Profit after deducting product costs, shipping, and marketing"
    )


def display_target_kpi(achievement_pct: float):
    """Display Target Achievement KPI card with color coding"""
    
    # Color coding based on achievement
    if achievement_pct >= 100:
        color = "green"
        emoji = "✅"
    elif achievement_pct >= 90:
        color = "orange"
        emoji = "⚠️"
    else:
        color = "red"
        emoji = "❌"
    
    st.metric(
        label=f"{emoji} Target Achievement",
        value=f"{achievement_pct:.1f}%",
        delta=f"{achievement_pct - 100:.1f}% vs target" if achievement_pct != 0 else None,
        help="Actual sales performance compared to annual targets"
    )


def display_sentiment_kpi(sentiment: float):
    """Display Average Sentiment Score KPI card"""
    
    # Determine sentiment emoji
    if sentiment >= 0.5:
        emoji = "😄"
        label_suffix = "Very Positive"
    elif sentiment >= 0.2:
        emoji = "🙂"
        label_suffix = "Positive"
    elif sentiment >= 0:
        emoji = "😐"
        label_suffix = "Neutral"
    elif sentiment >= -0.2:
        emoji = "😟"
        label_suffix = "Negative"
    else:
        emoji = "😞"
        label_suffix = "Very Negative"
    
    st.metric(
        label=f"{emoji} Avg Sentiment",
        value=f"{sentiment:.3f}",
        delta=label_suffix,
        help="Average customer sentiment score from product reviews (-1.0 to +1.0)"
    )


def display_custom_kpi(label: str, value: Any, delta: str = None, 
                       emoji: str = "📊", help_text: str = None):
    """
    Display a custom KPI card
    
    Args:
        label: KPI label
        value: KPI value (will be formatted)
        delta: Optional delta text
        emoji: Emoji prefix
        help_text: Tooltip text
    """
    st.metric(
        label=f"{emoji} {label}",
        value=value,
        delta=delta,
        help=help_text
    )


def fetch_global_kpis(db_connector) -> Dict[str, float]:
    """
    Fetch all global KPIs from database
    
    Args:
        db_connector: DatabaseConnector instance
        
    Returns:
        Dictionary with KPI values
    """
    kpis = {}
    
    # Total Revenue
    query_revenue = "SELECT ROUND(SUM(Total_Revenue), 2) as Total_Revenue FROM Fact_Sales"
    result = db_connector.execute_query(query_revenue)
    kpis['total_revenue'] = float(result['Total_Revenue'].iloc[0])
    
    # Net Profit
    query_profit = "SELECT ROUND(SUM(Net_Profit), 2) as Net_Profit FROM Fact_Sales"
    result = db_connector.execute_query(query_profit)
    kpis['net_profit'] = float(result['Net_Profit'].iloc[0])
    
    # Target Achievement
    query_target = """
    SELECT 
        ROUND(SUM(fs.Total_Revenue), 2) as Actual_Sales,
        ROUND(SUM(ds.Monthly_Target * 12), 2) as Annual_Target,
        ROUND((SUM(fs.Total_Revenue) * 100.0 / NULLIF(SUM(ds.Monthly_Target * 12), 0)), 2) as Achievement_Percentage
    FROM Fact_Sales fs
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    WHERE ds.Monthly_Target IS NOT NULL
    """
    result = db_connector.execute_query(query_target)
    kpis['target_achievement'] = float(result['Achievement_Percentage'].iloc[0])
    
    # Average Sentiment
    query_sentiment = """
    SELECT ROUND(AVG(Sentiment_Score), 3) as Avg_Sentiment
    FROM Dim_Product
    WHERE Sentiment_Score IS NOT NULL
    """
    result = db_connector.execute_query(query_sentiment)
    kpis['avg_sentiment'] = float(result['Avg_Sentiment'].iloc[0])
    
    return kpis