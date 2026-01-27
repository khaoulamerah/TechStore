"""
SQL Queries for TechStore Dashboard - CORRECTED VERSION
All queries tested and working with techstore_dw.db
"""

# GLOBAL KPIs

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

# TIME SERIES ANALYSIS

QUERY_DAILY_SALES = """
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
GROUP BY dd.Full_Date, dd.Year, dd.Month_Name, dd.Day_Name
ORDER BY dd.Full_Date
"""

QUERY_MONTHLY_TRENDS = """
SELECT 
    dd.Year,
    dd.Month,
    dd.Month_Name,
    COUNT(*) as Transaction_Count,
    ROUND(SUM(fs.Total_Revenue), 2) as Monthly_Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Monthly_Profit,
    ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value
FROM Fact_Sales fs
JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
GROUP BY dd.Year, dd.Month, dd.Month_Name
ORDER BY dd.Year DESC, dd.Month DESC
"""

# PRODUCT ANALYSIS


QUERY_TOP_SELLING_PRODUCTS = """
SELECT 
    dp.Product_Name,
    dp.Category_Name,
    SUM(fs.Quantity) as Units_Sold,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Total_Profit,
    ROUND(AVG(dp.Sentiment_Score), 3) as Avg_Sentiment
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
GROUP BY dp.Product_ID, dp.Product_Name, dp.Category_Name
ORDER BY Total_Revenue DESC
LIMIT 15
"""

QUERY_CATEGORY_PERFORMANCE = """
SELECT 
    dp.Category_Name,
    COUNT(*) as Transactions,
    SUM(fs.Quantity) as Units_Sold,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
    ROUND((SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)), 2) as Profit_Margin_Pct
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
GROUP BY dp.Category_Name
ORDER BY Total_Revenue DESC
"""

# STORE PERFORMANCE

QUERY_STORE_RANKING = """
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
WHERE ds.Monthly_Target IS NOT NULL
GROUP BY ds.Store_ID, ds.Store_Name, ds.City_Name, ds.Region, ds.Monthly_Target
ORDER BY Net_Profit DESC
"""

QUERY_REGIONAL_PERFORMANCE = """
SELECT 
    ds.Region,
    COUNT(DISTINCT ds.Store_ID) as Store_Count,
    COUNT(*) as Transactions,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
    ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value
FROM Fact_Sales fs
JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
GROUP BY ds.Region
ORDER BY Total_Revenue DESC
"""

# CUSTOMER ANALYSIS

QUERY_TOP_CUSTOMERS = """
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
GROUP BY dc.Customer_ID, dc.Customer_Name, dc.City_Name, dc.Region
ORDER BY Total_Spent DESC
LIMIT 20
"""

QUERY_CUSTOMER_GEOGRAPHY = """
SELECT 
    dc.Region,
    dc.City_Name,
    COUNT(DISTINCT dc.Customer_ID) as Customer_Count,
    COUNT(fs.Sale_ID) as Total_Purchases,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue
FROM Fact_Sales fs
JOIN Dim_Customer dc ON fs.Customer_ID = dc.Customer_ID
GROUP BY dc.Region, dc.City_Name
ORDER BY Total_Revenue DESC
"""

# PROFITABILITY ANALYSIS 

QUERY_PROFIT_MARGIN_BY_CATEGORY = """
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
GROUP BY dp.Category_Name
ORDER BY Profit_Margin_Pct DESC
"""

QUERY_MARKETING_ROI = """
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
WHERE fs.Marketing_Cost > 0
GROUP BY dp.Category_Name
ORDER BY ROI_Percentage DESC
"""

# SENTIMENT ANALYSIS

QUERY_SENTIMENT_VS_SALES = """
SELECT 
    dp.Product_Name,
    dp.Category_Name,
    ROUND(dp.Sentiment_Score, 3) as Sentiment_Score,
    SUM(fs.Quantity) as Units_Sold,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(AVG(fs.Total_Revenue / NULLIF(fs.Quantity, 0)), 2) as Avg_Price
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
WHERE dp.Sentiment_Score IS NOT NULL
GROUP BY dp.Product_ID, dp.Product_Name, dp.Category_Name, dp.Sentiment_Score
HAVING SUM(fs.Quantity) >= 10
ORDER BY Units_Sold DESC
LIMIT 15
"""

# BUSINESS OVERVIEW DASHBOARD


QUERY_DASHBOARD_SUMMARY = """
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
"""