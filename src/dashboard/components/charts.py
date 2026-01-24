"""
Chart Generation Functions
Reusable chart components for dashboard visualizations
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Optional, List

# Color schemes
COLOR_SCHEMES = {
    'revenue': ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6'],
    'profit': ['#27ae60', '#16a085', '#2ecc71', '#27ae60', '#1e8449'],
    'sentiment': ['#e74c3c', '#e67e22', '#f39c12', '#f1c40f', '#2ecc71'],
    'default': px.colors.qualitative.Set3
}


def create_revenue_trend_chart(df: pd.DataFrame, 
                                x_col: str = 'Period',
                                y_col: str = 'Revenue',
                                title: str = 'Revenue Trend') -> go.Figure:
    """
    Create time series line chart for revenue trends
    
    Args:
        df: DataFrame with time series data
        x_col: Column name for x-axis (time periods)
        y_col: Column name for y-axis (revenue values)
        title: Chart title
        
    Returns:
        Plotly Figure object
    """
    fig = px.line(
        df,
        x=x_col,
        y=y_col,
        title=title,
        markers=True,
        line_shape='spline'
    )
    
    fig.update_traces(
        line=dict(color='#3498db', width=3),
        marker=dict(size=8)
    )
    
    fig.update_layout(
        hovermode='x unified',
        xaxis_title=None,
        yaxis_title='Revenue (DZD)',
        font=dict(size=12),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig


def create_category_pie_chart(df: pd.DataFrame,
                               values_col: str = 'Revenue',
                               names_col: str = 'Category',
                               title: str = 'Revenue by Category') -> go.Figure:
    """
    Create pie/donut chart for categorical data
    
    Args:
        df: DataFrame with categorical data
        values_col: Column name for values
        names_col: Column name for category names
        title: Chart title
        
    Returns:
        Plotly Figure object
    """
    fig = px.pie(
        df,
        values=values_col,
        names=names_col,
        title=title,
        hole=0.4,
        color_discrete_sequence=COLOR_SCHEMES['revenue']
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>%{value:,.0f} DZD<br>%{percent}<extra></extra>'
    )
    
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.02),
        font=dict(size=11)
    )
    
    return fig


def create_horizontal_bar_chart(df: pd.DataFrame,
                                 x_col: str,
                                 y_col: str,
                                 title: str = 'Comparison',
                                 color_col: Optional[str] = None,
                                 color_scale: str = 'Blues') -> go.Figure:
    """
    Create horizontal bar chart for comparisons
    
    Args:
        df: DataFrame with comparison data
        x_col: Column name for x-axis (values)
        y_col: Column name for y-axis (categories)
        title: Chart title
        color_col: Optional column for color mapping
        color_scale: Plotly color scale name
        
    Returns:
        Plotly Figure object
    """
    fig = px.bar(
        df,
        x=x_col,
        y=y_col,
        orientation='h',
        title=title,
        color=color_col if color_col else x_col,
        color_continuous_scale=color_scale
    )
    
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title=None,
        yaxis_title=None,
        showlegend=False,
        font=dict(size=11),
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig


def create_multi_line_chart(df: pd.DataFrame,
                             x_col: str,
                             y_cols: List[str],
                             title: str = 'Multi-Metric Trend',
                             labels: Optional[List[str]] = None) -> go.Figure:
    """
    Create multi-line chart for comparing multiple metrics
    
    Args:
        df: DataFrame with time series data
        x_col: Column name for x-axis
        y_cols: List of column names for y-axes
        title: Chart title
        labels: Optional custom labels for lines
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6']
    
    for idx, col in enumerate(y_cols):
        label = labels[idx] if labels and idx < len(labels) else col
        
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[col],
            name=label,
            mode='lines+markers',
            line=dict(color=colors[idx % len(colors)], width=3),
            marker=dict(size=6)
        ))
    
    fig.update_layout(
        title=title,
        hovermode='x unified',
        xaxis_title=None,
        yaxis_title='Amount (DZD)',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(size=12),
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig


def create_stacked_bar_chart(df: pd.DataFrame,
                              x_col: str,
                              y_cols: List[str],
                              title: str = 'Stacked Comparison',
                              labels: Optional[List[str]] = None) -> go.Figure:
    """
    Create stacked bar chart for composition analysis
    
    Args:
        df: DataFrame with categorical data
        x_col: Column name for x-axis (categories)
        y_cols: List of column names for stacked values
        title: Chart title
        labels: Optional custom labels for stack segments
        
    Returns:
        Plotly Figure object
    """
    fig = go.Figure()
    
    colors = COLOR_SCHEMES['revenue']
    
    for idx, col in enumerate(y_cols):
        label = labels[idx] if labels and idx < len(labels) else col
        
        fig.add_trace(go.Bar(
            x=df[x_col],
            y=df[col],
            name=label,
            marker_color=colors[idx % len(colors)]
        ))
    
    fig.update_layout(
        title=title,
        barmode='stack',
        xaxis_title=None,
        yaxis_title='Amount (DZD)',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(size=12),
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig


def create_scatter_plot(df: pd.DataFrame,
                        x_col: str,
                        y_col: str,
                        size_col: Optional[str] = None,
                        color_col: Optional[str] = None,
                        title: str = 'Scatter Analysis',
                        hover_data: Optional[List[str]] = None) -> go.Figure:
    """
    Create scatter plot for correlation analysis
    
    Args:
        df: DataFrame with data points
        x_col: Column name for x-axis
        y_col: Column name for y-axis
        size_col: Optional column for bubble size
        color_col: Optional column for color mapping
        title: Chart title
        hover_data: Optional list of columns to show in hover
        
    Returns:
        Plotly Figure object
    """
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        size=size_col,
        color=color_col,
        title=title,
        hover_data=hover_data,
        color_continuous_scale='Viridis'
    )
    
    fig.update_traces(marker=dict(line=dict(width=0.5, color='white')))
    
    fig.update_layout(
        font=dict(size=12),
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
    
    return fig


def create_heatmap(df: pd.DataFrame,
                   x_col: str,
                   y_col: str,
                   value_col: str,
                   title: str = 'Heatmap',
                   color_scale: str = 'RdYlGn') -> go.Figure:
    """
    Create heatmap for matrix visualization
    
    Args:
        df: DataFrame with matrix data
        x_col: Column name for x-axis
        y_col: Column name for y-axis
        value_col: Column name for cell values
        title: Chart title
        color_scale: Plotly color scale name
        
    Returns:
        Plotly Figure object
    """
    # Pivot data for heatmap
    pivot_df = df.pivot(index=y_col, columns=x_col, values=value_col)
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=pivot_df.index,
        colorscale=color_scale,
        hovertemplate='%{y}<br>%{x}<br>Value: %{z:,.0f}<extra></extra>'
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title=None,
        yaxis_title=None,
        font=dict(size=11)
    )
    
    return fig


def create_gauge_chart(value: float,
                       max_value: float,
                       title: str = 'Performance',
                       suffix: str = '%') -> go.Figure:
    """
    Create gauge chart for KPI display
    
    Args:
        value: Current value
        max_value: Maximum value for gauge
        title: Chart title
        suffix: Suffix for value display
        
    Returns:
        Plotly Figure object
    """
    # Determine color based on value
    if value >= max_value * 0.9:
        color = "#2ecc71"
    elif value >= max_value * 0.7:
        color = "#f39c12"
    else:
        color = "#e74c3c"
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title, 'font': {'size': 20}},
        delta={'reference': max_value, 'increasing': {'color': "green"}},
        number={'suffix': suffix},
        gauge={
            'axis': {'range': [None, max_value], 'tickwidth': 1, 'tickcolor': "darkblue"},
            'bar': {'color': color},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, max_value * 0.5], 'color': '#ffebee'},
                {'range': [max_value * 0.5, max_value * 0.75], 'color': '#fff3e0'},
                {'range': [max_value * 0.75, max_value], 'color': '#e8f5e9'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * 0.9
            }
        }
    ))
    
    fig.update_layout(
        height=300,
        font={'color': "darkblue", 'family': "Arial"}
    )
    
    return fig