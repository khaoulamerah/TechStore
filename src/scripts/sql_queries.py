

def apply_filters(base_query: str, where_clause: str = "1=1") -> str:
    """
    Apply filter conditions to a base query
    
    Args:
        base_query: Base SQL query
        where_clause: WHERE conditions from filters
        
    Returns:
        Complete query with filters applied
    """
    if "WHERE" in base_query.upper():

        return base_query.replace("WHERE", f"WHERE {where_clause} AND", 1)
    else:
        if "GROUP BY" in base_query.upper():
            return base_query.replace("GROUP BY", f"WHERE {where_clause}\nGROUP BY", 1)
        elif "ORDER BY" in base_query.upper():
            return base_query.replace("ORDER BY", f"WHERE {where_clause}\nORDER BY", 1)
        else:
            return f"{base_query}\nWHERE {where_clause}"


def get_total_revenue_query(where_clause: str = "1=1") -> str:
    """Get total revenue query with optional filters"""
    return f"""
    SELECT ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """

def get_net_profit_query(where_clause: str = "1=1") -> str:
    """Get net profit query with optional filters"""
    return f"""
    SELECT ROUND(SUM(fs.Net_Profit), 2) as Net_Profit
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """

def get_total_transactions_query(where_clause: str = "1=1") -> str:
    """Get total transactions query with optional filters"""
    return f"""
    SELECT COUNT(*) as Total_Transactions
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """

def get_avg_transaction_value_query(where_clause: str = "1=1") -> str:
    """Get average transaction value query with optional filters"""
    return f"""
    SELECT ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """

def get_target_achievement_query(where_clause: str = "1=1") -> str:
    """Get target achievement query with optional filters"""
    return f"""
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

def get_avg_sentiment_query(where_clause: str = "1=1") -> str:
    """Get average sentiment query with optional filters"""
    return f"""
    SELECT ROUND(AVG(dp.Sentiment_Score), 3) as Avg_Sentiment
    FROM Dim_Product dp
    JOIN Fact_Sales fs ON dp.Product_ID = fs.Product_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    WHERE {where_clause}
    """

def get_avg_sentiment_global_query() -> str:
    """Get global average sentiment (no filters, fallback)"""
    return """
    SELECT ROUND(AVG(Sentiment_Score), 3) as Avg_Sentiment
    FROM Dim_Product
    WHERE Sentiment_Score IS NOT NULL
    """

# ============================================
# TIME SERIES ANALYSIS
# ============================================

def get_daily_sales_query(where_clause: str = "1=1") -> str:
    """Get daily sales query with optional filters"""
    return f"""
    SELECT 
        dd.Full_Date,
        dd.Year,
        dd.Month_Name,
        dd.Day_Name,
        COUNT(*) as Transactions,
        ROUND(SUM(fs.Total_Revenue), 2) as Daily_Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Daily_Profit
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    GROUP BY dd.Full_Date, dd.Year, dd.Month_Name, dd.Day_Name
    ORDER BY dd.Full_Date
    """

def get_monthly_trends_query(where_clause: str = "1=1") -> str:
    """Get monthly trends query with optional filters"""
    return f"""
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
    ORDER BY dd.Year DESC, dd.Month DESC
    """

def get_ytd_revenue_query(where_clause: str = "1=1") -> str:
    """Get Year-to-Date revenue growth query with optional filters"""
    return f"""
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


def get_top_selling_products_query(where_clause: str = "1=1", limit: int = 15) -> str:
    """Get top selling products query with optional filters"""
    return f"""
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
    LIMIT {limit}
    """

def get_category_performance_query(where_clause: str = "1=1") -> str:
    """Get category performance query with optional filters"""
    return f"""
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


def get_store_ranking_query(where_clause: str = "1=1") -> str:
    """Get store ranking query with optional filters"""
    return f"""
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

def get_regional_performance_query(where_clause: str = "1=1") -> str:
    """Get regional performance query with optional filters"""
    return f"""
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


def get_top_customers_query(where_clause: str = "1=1", limit: int = 20) -> str:
    """Get top customers query with optional filters"""
    return f"""
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
    LIMIT {limit}
    """

def get_customer_geography_query(where_clause: str = "1=1") -> str:
    """Get customer geography query with optional filters"""
    return f"""
    SELECT 
        dc.Region,
        dc.City_Name,
        COUNT(DISTINCT dc.Customer_ID) as Customer_Count,
        COUNT(fs.Sale_ID) as Total_Purchases,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue
    FROM Fact_Sales fs
    JOIN Dim_Customer dc ON fs.Customer_ID = dc.Customer_ID
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    GROUP BY dc.Region, dc.City_Name
    ORDER BY Total_Revenue DESC
    """
def get_profit_margin_by_category_query(where_clause: str = "1=1") -> str:
    """Get profit margin by category query with optional filters"""
    return f"""
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

def get_marketing_roi_query(where_clause: str = "1=1") -> str:
    """Get marketing ROI query with optional filters"""
    return f"""
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

def get_sentiment_vs_sales_query(where_clause: str = "1=1", limit: int = 15) -> str:
    """Get sentiment vs sales query with optional filters"""
    return f"""
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
    LIMIT {limit}
    """


def get_price_competitiveness_query(where_clause: str = "1=1", limit: int = 10) -> str:
    """Get price competitiveness analysis query with optional filters"""
    return f"""
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
    LIMIT {limit}
    """
def get_dashboard_summary_query(where_clause: str = "1=1") -> str:
    """Get dashboard summary query with optional filters"""
    return f"""
    SELECT 
        -- Date Range
        MIN(dd.Full_Date) as Start_Date,
        MAX(dd.Full_Date) as End_Date,
        
        -- Transaction Stats
        COUNT(*) as Total_Transactions,
        COUNT(DISTINCT fs.Customer_ID) as Unique_Customers,
        COUNT(DISTINCT fs.Product_ID) as Unique_Products,
        COUNT(DISTINCT fs.Store_ID) as Unique_Stores,
        
        -- Financial Stats
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Total_Profit,
        ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value,
        
        -- Profitability
        ROUND((SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)), 2) as Overall_Profit_Margin,
        
        -- Quantity Stats
        SUM(fs.Quantity) as Total_Units_Sold,
        ROUND(AVG(fs.Quantity), 2) as Avg_Quantity_Per_Transaction
        
    FROM Fact_Sales fs
    JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
    JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    WHERE {where_clause}
    """

QUERY_TOTAL_REVENUE = """
SELECT ROUND(SUM(Total_Revenue), 2) as Total_Revenue
FROM Fact_Sales
"""

QUERY_NET_PROFIT = """
SELECT ROUND(SUM(Net_Profit), 2) as Net_Profit
FROM Fact_Sales
"""

QUERY_TOTAL_TRANSACTIONS = """
SELECT COUNT(*) as Total_Transactions
FROM Fact_Sales
"""

QUERY_AVG_TRANSACTION_VALUE = """
SELECT ROUND(AVG(Total_Revenue), 2) as Avg_Transaction_Value
FROM Fact_Sales
"""

QUERY_DAILY_SALES = get_daily_sales_query()
QUERY_MONTHLY_TRENDS = get_monthly_trends_query()
QUERY_TOP_SELLING_PRODUCTS = get_top_selling_products_query()
QUERY_CATEGORY_PERFORMANCE = get_category_performance_query()
QUERY_STORE_RANKING = get_store_ranking_query()
QUERY_REGIONAL_PERFORMANCE = get_regional_performance_query()
QUERY_TOP_CUSTOMERS = get_top_customers_query()
QUERY_CUSTOMER_GEOGRAPHY = get_customer_geography_query()
QUERY_PROFIT_MARGIN_BY_CATEGORY = get_profit_margin_by_category_query()
QUERY_MARKETING_ROI = get_marketing_roi_query()
QUERY_SENTIMENT_VS_SALES = get_sentiment_vs_sales_query()
QUERY_DASHBOARD_SUMMARY = get_dashboard_summary_query()
