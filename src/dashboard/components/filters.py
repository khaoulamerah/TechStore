"""
Dashboard Filters Component
Provides interactive OLAP filtering capabilities
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date
from typing import Dict, List, Tuple, Any


class DashboardFilters:
    
    def __init__(self, db_connector):
        self.db = db_connector
        self._filter_values = {}
        
        # Initialize session state for filters
        if 'filter_version' not in st.session_state:
            st.session_state.filter_version = 0
    
    def render_sidebar_filters(self) -> Dict[str, Any]:
        """
        Render all filter controls in the sidebar
        
        Returns:
            Dictionary containing all filter values
        """
        st.sidebar.header("🔍 Filters (OLAP)")
        st.sidebar.markdown("---")
        
        filters = {}
        
        # Date Range Filter
        filters['date_range'] = self._render_date_filter()
        
        st.sidebar.markdown("---")
        
        # Geographic Filters
        filters['region'] = self._render_region_filter()
        filters['store'] = self._render_store_filter(filters['region'])
        
        st.sidebar.markdown("---")
        
        # Product Filters
        filters['category'] = self._render_category_filter()
        filters['subcategory'] = self._render_subcategory_filter(filters['category'])
        
        st.sidebar.markdown("---")
        
        # Reset button
        if st.sidebar.button("🔄 Reset All Filters", 
                            use_container_width=True,
                            key=f"reset_btn_{st.session_state.filter_version}"):
            self._reset_all_filters()
            st.rerun()
        
        return filters
    
    def _reset_all_filters(self):
        """Reset all filters to default values"""
        # Increment version to force widget recreation
        st.session_state.filter_version += 1
        
        # Remove all filter keys from session_state
        keys_to_remove = [
            'date_filter',
            'region_filter', 
            'store_filter',
            'category_filter',
            'subcat_filter'
        ]
        
        for key in keys_to_remove:
            if key in st.session_state:
                del st.session_state[key]
    
    def _render_date_filter(self) -> Tuple[date, date]:
        """Render date range filter"""
        st.sidebar.subheader("📅 Date Range")
        
        # Get min/max dates from database
        query = "SELECT MIN(Full_Date) as min_date, MAX(Full_Date) as max_date FROM Dim_Date"
        result = self.db.execute_query(query)
        
        min_date = pd.to_datetime(result['min_date'].iloc[0]).date()
        max_date = pd.to_datetime(result['max_date'].iloc[0]).date()
        
        # Date range selector with dynamic key
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            key=f"date_filter_{st.session_state.filter_version}"
        )
        
        # Handle single date selection
        if isinstance(date_range, tuple) and len(date_range) == 2:
            return date_range
        elif isinstance(date_range, date):
            return (date_range, date_range)
        else:
            return (min_date, max_date)
    
    def _render_region_filter(self) -> List[str]:
        """Render region multi-select filter"""
        st.sidebar.subheader("🌍 Region")
        
        # Get all regions
        query = "SELECT DISTINCT Region FROM Dim_Store WHERE Region IS NOT NULL ORDER BY Region"
        result = self.db.execute_query(query)
        regions = result['Region'].tolist()
        
        # With dynamic key
        selected_regions = st.sidebar.multiselect(
            "Select Regions",
            options=regions,
            default=regions,
            key=f"region_filter_{st.session_state.filter_version}"
        )
        
        return selected_regions if selected_regions else regions
    
    def _render_store_filter(self, selected_regions: List[str]) -> List[str]:
        """Render store multi-select filter (dependent on region)"""
        st.sidebar.subheader("🏪 Store")
        
        # Get stores in selected regions
        region_placeholders = ','.join(['?' for _ in selected_regions])
        query = f"""
        SELECT DISTINCT Store_Name 
        FROM Dim_Store 
        WHERE Region IN ({region_placeholders})
        ORDER BY Store_Name
        """
        result = self.db.execute_query(query, tuple(selected_regions))
        stores = result['Store_Name'].tolist()
        
        # With dynamic key
        selected_stores = st.sidebar.multiselect(
            "Select Stores",
            options=stores,
            default=stores,
            key=f"store_filter_{st.session_state.filter_version}"
        )
        
        return selected_stores if selected_stores else stores
    
    def _render_category_filter(self) -> List[str]:
        """Render category multi-select filter"""
        st.sidebar.subheader("📦 Product Category")
        
        # Get all categories
        query = """
        SELECT DISTINCT Category_Name 
        FROM Dim_Product 
        WHERE Category_Name IS NOT NULL 
        ORDER BY Category_Name
        """
        result = self.db.execute_query(query)
        categories = result['Category_Name'].tolist()
        
        # With dynamic key
        selected_categories = st.sidebar.multiselect(
            "Select Categories",
            options=categories,
            default=categories,
            key=f"category_filter_{st.session_state.filter_version}"
        )
        
        return selected_categories if selected_categories else categories
    
    def _render_subcategory_filter(self, selected_categories: List[str]) -> List[str]:
        """Render subcategory multi-select filter (dependent on category)"""
        st.sidebar.subheader("📋 Subcategory")
        
        # Get subcategories in selected categories
        category_placeholders = ','.join(['?' for _ in selected_categories])
        query = f"""
        SELECT DISTINCT Subcategory_Name 
        FROM Dim_Product 
        WHERE Category_Name IN ({category_placeholders})
          AND Subcategory_Name IS NOT NULL
        ORDER BY Subcategory_Name
        """
        result = self.db.execute_query(query, tuple(selected_categories))
        subcategories = result['Subcategory_Name'].tolist()
        
        if subcategories:
            # With dynamic key
            selected_subcats = st.sidebar.multiselect(
                "Select Subcategories",
                options=subcategories,
                default=subcategories,
                key=f"subcat_filter_{st.session_state.filter_version}"
            )
            return selected_subcats if selected_subcats else subcategories
        else:
            return []
    
    def build_filter_sql_conditions(self, filters: Dict[str, Any]) -> Tuple[str, List]:
        """
        Build SQL WHERE clause and parameters from filter values
        
        Args:
            filters: Dictionary of filter values
            
        Returns:
            Tuple of (where_clause, params_list)
        """
        conditions = []
        params = []
        
        # Date filter
        if filters.get('date_range'):
            start_date, end_date = filters['date_range']
            conditions.append("dd.Full_Date BETWEEN ? AND ?")
            params.extend([start_date, end_date])
        
        # Region filter
        if filters.get('region'):
            placeholders = ','.join(['?' for _ in filters['region']])
            conditions.append(f"ds.Region IN ({placeholders})")
            params.extend(filters['region'])
        
        # Store filter
        if filters.get('store'):
            placeholders = ','.join(['?' for _ in filters['store']])
            conditions.append(f"ds.Store_Name IN ({placeholders})")
            params.extend(filters['store'])
        
        # Category filter
        if filters.get('category'):
            placeholders = ','.join(['?' for _ in filters['category']])
            conditions.append(f"dp.Category_Name IN ({placeholders})")
            params.extend(filters['category'])
        
        # Subcategory filter
        if filters.get('subcategory') and len(filters['subcategory']) > 0:
            placeholders = ','.join(['?' for _ in filters['subcategory']])
            conditions.append(f"dp.Subcategory_Name IN ({placeholders})")
            params.extend(filters['subcategory'])
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        return where_clause, params
    
    def get_filter_summary(self, filters: Dict[str, Any]) -> str:
        """
        Generate human-readable filter summary
        
        Args:
            filters: Dictionary of filter values
            
        Returns:
            Summary string
        """
        summary_parts = []
        
        if filters.get('date_range'):
            start, end = filters['date_range']
            summary_parts.append(f"📅 {start} to {end}")
        
        if filters.get('region'):
            summary_parts.append(f"🌍 {len(filters['region'])} region(s)")
        
        if filters.get('store'):
            summary_parts.append(f"🏪 {len(filters['store'])} store(s)")
        
        if filters.get('category'):
            summary_parts.append(f"📦 {len(filters['category'])} category(ies)")
        
        return " | ".join(summary_parts) if summary_parts else "No filters applied"