"""
SQL Queries for TechStore Dashboard
All queries are designed for the Star Schema in techstore_dw.db
Author: [Your Name] - Database Architect
"""

# ============================================
# GLOBAL KPIs
# ============================================

QUERY_TOTAL_REVENUE = """
SELECT ROUND(SUM(Total_Revenue), 2) as Total_Revenue
FROM Fact_Sales
"""

QUERY_NET_PROFIT = """
SELECT ROUND(SUM(Net_Profit), 2) as Net_Profit
FROM Fact_Sales
"""

QUERY_TARGET_ACHIEVEMENT = """
SELECT 
    ROUND(SUM(fs.Total_Revenue), 2) as Actual_Sales,
    ROUND(SUM(ds.Monthly_Target * 12), 2) as Annual_Target,
    ROUND((SUM(fs.Total_Revenue) * 100.0 / NULLIF(SUM(ds.Monthly_Target * 12), 0)), 2) as Achievement_Percentage
FROM Fact_Sales fs
JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
WHERE ds.Monthly_Target IS NOT NULL
"""

QUERY_AVG_SENTIMENT = """
SELECT ROUND(AVG(Sentiment_Score), 3) as Avg_Sentiment
FROM Dim_Product
WHERE Sentiment_Score IS NOT NULL
"""

# ============================================
# TIME SERIES ANALYSIS
# ============================================

QUERY_YTD_GROWTH = """
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
GROUP BY dd.Year, dd.Month
ORDER BY dd.Year, dd.Month
"""

QUERY_MONTHLY_TRENDS = """
SELECT 
    dd.Year || '-' || PRINTF('%02d', dd.Month) as Year_Month,
    ROUND(SUM(fs.Total_Revenue), 2) as Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Profit,
    COUNT(*) as Transaction_Count
FROM Fact_Sales fs
JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
GROUP BY dd.Year, dd.Month
ORDER BY dd.Year, dd.Month
"""

# ============================================
# PRODUCT ANALYSIS
# ============================================

QUERY_TOP_PRODUCTS_BY_CATEGORY = """
WITH Product_Rankings AS (
    SELECT 
        dp.Category_Name,
        dp.Product_Name,
        SUM(fs.Quantity) as Total_Quantity,
        ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
        ROUND(SUM(fs.Net_Profit), 2) as Total_Profit,
        ROW_NUMBER() OVER (
            PARTITION BY dp.Category_Name 
            ORDER BY SUM(fs.Total_Revenue) DESC
        ) as Rank
    FROM Fact_Sales fs
    JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
    GROUP BY dp.Category_Name, dp.Product_Name
)
SELECT 
    Category_Name,
    Product_Name,
    Total_Revenue,
    Total_Quantity,
    Total_Profit,
    Rank
FROM Product_Rankings
WHERE Rank <= 3
ORDER BY Category_Name, Rank
"""

QUERY_CATEGORY_PERFORMANCE = """
SELECT 
    dp.Category_Name,
    COUNT(DISTINCT fs.Sale_ID) as Total_Transactions,
    SUM(fs.Quantity) as Units_Sold,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
    ROUND((SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)), 2) as Profit_Margin_Pct
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
GROUP BY dp.Category_Name
ORDER BY Total_Revenue DESC
"""

# ============================================
# MARKETING ANALYSIS
# ============================================

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

# ============================================
# PRICE COMPETITIVENESS
# ============================================

QUERY_PRICE_COMPETITIVENESS = """
SELECT 
    dp.Product_Name,
    dp.Category_Name,
    ROUND(AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)), 2) as Our_Avg_Price,
    dp.Competitor_Price,
    ROUND(AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)) - dp.Competitor_Price, 2) as Price_Difference,
    ROUND(((AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)) - dp.Competitor_Price) * 100.0 / 
           NULLIF(dp.Competitor_Price, 0)), 2) as Price_Diff_Pct,
    CASE 
        WHEN AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)) > dp.Competitor_Price * 1.1 THEN 'Overpriced (>10%)'
        WHEN AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)) > dp.Competitor_Price THEN 'Slightly Higher'
        WHEN AVG(fs.Total_Revenue * 1.0 / NULLIF(fs.Quantity, 0)) < dp.Competitor_Price THEN 'Competitive Advantage'
        ELSE 'Same Price'
    END as Price_Status
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
WHERE dp.Competitor_Price IS NOT NULL
GROUP BY dp.Product_ID, dp.Product_Name, dp.Category_Name, dp.Competitor_Price
HAVING COUNT(*) >= 5
ORDER BY Price_Diff_Pct DESC
"""

# ============================================
# STORE PERFORMANCE
# ============================================

QUERY_STORE_PERFORMANCE = """
SELECT 
    ds.Store_Name,
    ds.City_Name,
    ds.Region,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
    ROUND(ds.Monthly_Target * 12, 2) as Annual_Target,
    ROUND((SUM(fs.Total_Revenue) * 100.0 / NULLIF(ds.Monthly_Target * 12, 0)), 2) as Target_Achievement_Pct,
    COUNT(DISTINCT fs.Customer_ID) as Unique_Customers,
    CASE 
        WHEN SUM(fs.Total_Revenue) >= ds.Monthly_Target * 12 THEN 'Target Met ✓'
        WHEN SUM(fs.Total_Revenue) >= ds.Monthly_Target * 12 * 0.9 THEN 'Close (>90%)'
        ELSE 'Below Target'
    END as Performance_Status
FROM Fact_Sales fs
JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
WHERE ds.Monthly_Target IS NOT NULL
GROUP BY ds.Store_ID, ds.Store_Name, ds.City_Name, ds.Region, ds.Monthly_Target
ORDER BY Net_Profit DESC
"""

QUERY_REGIONAL_ANALYSIS = """
SELECT 
    ds.Region,
    COUNT(DISTINCT ds.Store_ID) as Number_of_Stores,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
    ROUND(AVG(fs.Total_Revenue), 2) as Avg_Transaction_Value,
    COUNT(*) as Total_Transactions
FROM Fact_Sales fs
JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
GROUP BY ds.Region
ORDER BY Total_Revenue DESC
"""

# ============================================
# CUSTOM KPI 1: PROFIT MARGIN BY CATEGORY
# ============================================

QUERY_PROFIT_MARGIN_BY_CATEGORY = """
SELECT 
    dp.Category_Name,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(SUM(fs.Product_Cost), 2) as Total_Product_Cost,
    ROUND(SUM(fs.Marketing_Cost), 2) as Total_Marketing_Cost,
    ROUND(SUM(fs.Shipping_Cost), 2) as Total_Shipping_Cost,
    ROUND(SUM(fs.Net_Profit), 2) as Net_Profit,
    ROUND((SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)), 2) as Profit_Margin_Pct,
    CASE 
        WHEN (SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)) >= 30 THEN 'Excellent'
        WHEN (SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)) >= 20 THEN 'Good'
        WHEN (SUM(fs.Net_Profit) * 100.0 / NULLIF(SUM(fs.Total_Revenue), 0)) >= 10 THEN 'Average'
        ELSE 'Low Margin'
    END as Margin_Rating
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
GROUP BY dp.Category_Name
ORDER BY Profit_Margin_Pct DESC
"""

# ============================================
# CUSTOM KPI 2: SENTIMENT VS SALES CORRELATION
# ============================================

QUERY_SENTIMENT_VS_SALES = """
SELECT 
    dp.Product_Name,
    dp.Category_Name,
    ROUND(dp.Sentiment_Score, 3) as Sentiment_Score,
    SUM(fs.Quantity) as Units_Sold,
    ROUND(SUM(fs.Total_Revenue), 2) as Total_Revenue,
    ROUND(AVG(fs.Total_Revenue / NULLIF(fs.Quantity, 0)), 2) as Avg_Price,
    CASE 
        WHEN dp.Sentiment_Score >= 0.5 THEN 'Very Positive (≥0.5)'
        WHEN dp.Sentiment_Score >= 0.2 THEN 'Positive (0.2-0.5)'
        WHEN dp.Sentiment_Score >= 0 THEN 'Neutral (0-0.2)'
        WHEN dp.Sentiment_Score >= -0.2 THEN 'Slightly Negative'
        ELSE 'Negative (<-0.2)'
    END as Sentiment_Category
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
WHERE dp.Sentiment_Score IS NOT NULL
GROUP BY dp.Product_ID, dp.Product_Name, dp.Category_Name, dp.Sentiment_Score
HAVING SUM(fs.Quantity) >= 10
ORDER BY Units_Sold DESC
LIMIT 20
"""

# ============================================
# CUSTOM KPI 3: CUSTOMER VALUE SEGMENTATION
# ============================================

QUERY_CUSTOMER_SEGMENTATION = """
WITH Customer_Stats AS (
    SELECT 
        dc.Customer_ID,
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
)
SELECT 
    Customer_Name,
    City_Name,
    Region,
    Purchase_Count,
    Total_Spent,
    Avg_Transaction_Value,
    Last_Purchase_Date,
    CASE 
        WHEN Total_Spent >= 1000000 THEN 'VIP (>1M DZD)'
        WHEN Total_Spent >= 500000 THEN 'High Value (500K-1M)'
        WHEN Total_Spent >= 200000 THEN 'Medium Value (200K-500K)'
        ELSE 'Regular Customer'
    END as Customer_Segment
FROM Customer_Stats
ORDER BY Total_Spent DESC
LIMIT 50
"""