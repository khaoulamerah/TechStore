"""
Enhanced Data Quality & Feature Engineering Audit
Deep analysis of original vs transformed data with integrity checks
Author: Data Quality Team
Date: January 2026
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class EnhancedDataQualityAuditor:
    """Deep data quality analysis for ETL pipeline"""
    
    def __init__(self):
        """Initialize auditor"""
        self.project_root = Path(__file__).parent.parent
        self.extracted_dir = self.project_root / "Data" / "extracted"
        self.transformed_dir = self.project_root / "Data" / "transformed"
        self.db_path = self.project_root / "database" / "techstore_dw.db"
        
        self.report_lines = []
        self.quality_score = 0
        self.total_checks = 0
        
    def add_section(self, title: str, level: int = 2):
        """Add section header"""
        prefix = "#" * level
        self.report_lines.append(f"\n{prefix} {title}\n")
    
    def add_line(self, content: str):
        """Add line to report"""
        self.report_lines.append(content)
    
    def add_table(self, df: pd.DataFrame):
        """Add dataframe as markdown table"""
        self.report_lines.append(df.to_markdown(index=False))
        self.report_lines.append("")
    
    def check_pass(self, message: str):
        """Record passing check"""
        self.quality_score += 1
        self.total_checks += 1
        self.add_line(f"✅ **PASS**: {message}")
    
    def check_fail(self, message: str):
        """Record failing check"""
        self.total_checks += 1
        self.add_line(f"❌ **FAIL**: {message}")
    
    def check_warning(self, message: str):
        """Record warning"""
        self.quality_score += 0.5
        self.total_checks += 1
        self.add_line(f"⚠️ **WARNING**: {message}")
    
    # ============================================
    # ORIGINAL DATA ANALYSIS
    # ============================================
    
    def analyze_original_data(self):
        """Deep analysis of original extracted data"""
        self.add_section("📥 Original Data Analysis (Pre-Transformation)", 1)
        
        # Sales data analysis
        self.add_section("Sales Data Quality", 2)
        
        sales_df = pd.read_csv(self.extracted_dir / 'sales.csv')
        
        # Auto-detect column names (handle case variations)
        sales_cols = {col.lower(): col for col in sales_df.columns}
        
        # Get actual column names
        trans_id_col = sales_cols.get('trans_id', sales_cols.get('sale_id', 'trans_id'))
        date_col = sales_cols.get('date', 'Date')
        revenue_col = sales_cols.get('total_revenue', 'Total_Revenue')
        qty_col = sales_cols.get('quantity', 'Quantity')
        product_col = sales_cols.get('product_id', 'Product_ID')
        store_col = sales_cols.get('store_id', 'Store_ID')
        customer_col = sales_cols.get('customer_id', 'Customer_ID')
        
        stats = {
            'Metric': [
                'Total Records',
                'Date Range',
                'Null Values',
                'Duplicate trans_id',
                'Zero/Negative Revenue',
                'Zero Quantity',
                'Unique Products',
                'Unique Stores',
                'Unique Customers'
            ],
            'Value': [
                f"{len(sales_df):,}",
                f"{sales_df[date_col].min()} to {sales_df[date_col].max()}",
                f"{sales_df.isnull().sum().sum()} fields",
                f"{sales_df[trans_id_col].duplicated().sum()}" if trans_id_col in sales_df.columns else "N/A",
                f"{(sales_df[revenue_col] <= 0).sum()}" if revenue_col in sales_df.columns else "N/A",
                f"{(sales_df[qty_col] == 0).sum()}" if qty_col in sales_df.columns else "N/A",
                f"{sales_df[product_col].nunique()}" if product_col in sales_df.columns else "N/A",
                f"{sales_df[store_col].nunique()}" if store_col in sales_df.columns else "N/A",
                f"{sales_df[customer_col].nunique()}" if customer_col in sales_df.columns else "N/A"
            ]
        }
        
        self.add_table(pd.DataFrame(stats))
        
        # Quality checks
        if sales_df.isnull().sum().sum() == 0:
            self.check_pass("Sales data has no null values")
        else:
            self.check_fail(f"Sales data has {sales_df.isnull().sum().sum()} null values")
        
        if trans_id_col in sales_df.columns:
            if sales_df[trans_id_col].duplicated().sum() == 0:
                self.check_pass("All transaction IDs are unique")
            else:
                self.check_fail(f"{sales_df[trans_id_col].duplicated().sum()} duplicate transaction IDs")
        
        if revenue_col in sales_df.columns:
            if (sales_df[revenue_col] <= 0).sum() == 0:
                self.check_pass("No zero/negative revenues in original data")
            else:
                self.check_fail(f"{(sales_df[revenue_col] <= 0).sum()} transactions with invalid revenue")
        
        # Revenue distribution analysis
        if revenue_col in sales_df.columns:
            self.add_section("Revenue Distribution Analysis", 3)
            
            revenue_stats = sales_df[revenue_col].describe()
            outliers_upper = sales_df[revenue_col] > revenue_stats['75%'] + 3 * (revenue_stats['75%'] - revenue_stats['25%'])
            outliers_lower = sales_df[revenue_col] < revenue_stats['25%'] - 3 * (revenue_stats['75%'] - revenue_stats['25%'])
            
            dist_stats = {
                'Statistic': ['Mean', 'Median', 'Std Dev', 'Min', 'Max', 'Q1', 'Q3', 'Outliers (Upper)', 'Outliers (Lower)'],
                'Value': [
                    f"{revenue_stats['mean']:,.0f} DZD",
                    f"{revenue_stats['50%']:,.0f} DZD",
                    f"{revenue_stats['std']:,.0f} DZD",
                    f"{revenue_stats['min']:,.0f} DZD",
                    f"{revenue_stats['max']:,.0f} DZD",
                    f"{revenue_stats['25%']:,.0f} DZD",
                    f"{revenue_stats['75%']:,.0f} DZD",
                    f"{outliers_upper.sum()} ({outliers_upper.sum()/len(sales_df)*100:.1f}%)",
                    f"{outliers_lower.sum()} ({outliers_lower.sum()/len(sales_df)*100:.1f}%)"
                ]
            }
            
            self.add_table(pd.DataFrame(dist_stats))
            
            if outliers_upper.sum() < len(sales_df) * 0.05:
                self.check_pass("Revenue outliers within acceptable range (<5%)")
            else:
                self.check_warning(f"{outliers_upper.sum()/len(sales_df)*100:.1f}% revenue outliers detected")
        
        # Products analysis
        self.add_section("Product Data Quality", 2)
        
        products_df = pd.read_csv(self.extracted_dir / 'products.csv')
        
        # Auto-detect product column names
        prod_cols = {col.lower(): col for col in products_df.columns}
        cost_col = prod_cols.get('unit_cost', 'Unit_Cost')
        price_col = prod_cols.get('unit_price', 'Unit_Price')
        
        prod_stats = {
            'Metric': [
                'Total Products',
                'Null Unit_Cost',
                'Null Unit_Price',
                'Cost > Price (Invalid)',
                'Zero Cost',
                'Price Range',
                'Cost Range'
            ],
            'Value': [
                f"{len(products_df)}",
                f"{products_df[cost_col].isnull().sum()}" if cost_col in products_df.columns else "N/A",
                f"{products_df[price_col].isnull().sum()}" if price_col in products_df.columns else "N/A",
                f"{(products_df[cost_col] > products_df[price_col]).sum()}" if cost_col in products_df.columns and price_col in products_df.columns else "N/A",
                f"{(products_df[cost_col] == 0).sum()}" if cost_col in products_df.columns else "N/A",
                f"{products_df[price_col].min():,.0f} - {products_df[price_col].max():,.0f} DZD" if price_col in products_df.columns else "N/A",
                f"{products_df[cost_col].min():,.0f} - {products_df[cost_col].max():,.0f} DZD" if cost_col in products_df.columns else "N/A"
            ]
        }
        
        self.add_table(pd.DataFrame(prod_stats))
        
        # Product quality checks
        if cost_col in products_df.columns:
            if products_df[cost_col].isnull().sum() == 0:
                self.check_pass("All products have unit cost defined")
            else:
                self.check_fail(f"{products_df[cost_col].isnull().sum()} products missing unit cost")
            
            if price_col in products_df.columns:
                invalid_pricing = (products_df[cost_col] > products_df[price_col]).sum()
                if invalid_pricing == 0:
                    self.check_pass("All products have price > cost")
                else:
                    self.check_fail(f"{invalid_pricing} products have cost > price (negative margin)")
        
        # Reviews analysis
        self.add_section("Reviews Data Quality", 2)
        
        reviews_df = pd.read_csv(self.extracted_dir / 'reviews.csv')
        
        # Auto-detect review column names
        review_cols = {col.lower(): col for col in reviews_df.columns}
        review_text_col = review_cols.get('review_text', 'Review_Text')
        rating_col = review_cols.get('rating', 'Rating')
        review_prod_col = review_cols.get('product_id', 'Product_ID')
        
        review_stats = {
            'Metric': [
                'Total Reviews',
                'Products with Reviews',
                'Avg Reviews per Product',
                'Null Review Text',
                'Null Ratings',
                'Rating Distribution',
                'Empty Reviews'
            ],
            'Value': [
                f"{len(reviews_df):,}",
                f"{reviews_df[review_prod_col].nunique()}" if review_prod_col in reviews_df.columns else "N/A",
                f"{len(reviews_df)/reviews_df[review_prod_col].nunique():.1f}" if review_prod_col in reviews_df.columns else "N/A",
                f"{reviews_df[review_text_col].isnull().sum()}" if review_text_col in reviews_df.columns else "N/A",
                f"{reviews_df[rating_col].isnull().sum()}" if rating_col in reviews_df.columns else "N/A",
                f"1★: {(reviews_df[rating_col]==1).sum()}, 5★: {(reviews_df[rating_col]==5).sum()}" if rating_col in reviews_df.columns else "N/A",
                f"{(reviews_df[review_text_col].str.len() < 5).sum()}" if review_text_col in reviews_df.columns else "N/A"
            ]
        }
        
        self.add_table(pd.DataFrame(review_stats))
        
        if review_text_col in reviews_df.columns:
            if reviews_df[review_text_col].isnull().sum() == 0:
                self.check_pass("No missing review text")
            else:
                self.check_warning(f"{reviews_df[review_text_col].isnull().sum()} reviews missing text")
    
    # ============================================
    # TRANSFORMATION QUALITY ANALYSIS
    # ============================================
    
    def analyze_transformation_quality(self):
        """Analyze quality of transformation process"""
        self.add_section("🔄 Transformation Quality Analysis", 1)
        
        # Load original and transformed data
        sales_orig = pd.read_csv(self.extracted_dir / 'sales.csv')
        fact_sales = pd.read_csv(self.transformed_dir / 'Fact_Sales.csv')
        
        self.add_section("Record Count Integrity", 2)
        
        count_comparison = {
            'Dataset': ['Original Sales', 'Transformed Fact_Sales', 'Difference'],
            'Records': [
                f"{len(sales_orig):,}",
                f"{len(fact_sales):,}",
                f"{len(fact_sales) - len(sales_orig):,}"
            ],
            'Status': [
                '✅',
                '✅' if len(fact_sales) >= len(sales_orig) else '❌',
                '✅' if abs(len(fact_sales) - len(sales_orig)) < 10 else '⚠️'
            ]
        }
        
        self.add_table(pd.DataFrame(count_comparison))
        
        if len(fact_sales) >= len(sales_orig):
            self.check_pass(f"Record count preserved ({len(fact_sales):,} >= {len(sales_orig):,})")
        else:
            self.check_fail(f"Lost {len(sales_orig) - len(fact_sales):,} records during transformation!")
        
        # Revenue integrity check
        self.add_section("Revenue Integrity Check", 2)
        
        orig_total = sales_orig['Total_Revenue'].sum()
        trans_total = fact_sales['total_revenue'].sum()
        revenue_diff_pct = abs(orig_total - trans_total) / orig_total * 100
        
        revenue_check = {
            'Source': ['Original', 'Transformed', 'Difference', 'Difference %'],
            'Total Revenue (DZD)': [
                f"{orig_total:,.0f}",
                f"{trans_total:,.0f}",
                f"{trans_total - orig_total:,.0f}",
                f"{revenue_diff_pct:.2f}%"
            ]
        }
        
        self.add_table(pd.DataFrame(revenue_check))
        
        if revenue_diff_pct < 0.01:
            self.check_pass(f"Revenue preserved perfectly (diff: {revenue_diff_pct:.4f}%)")
        elif revenue_diff_pct < 1:
            self.check_warning(f"Revenue difference: {revenue_diff_pct:.2f}%")
        else:
            self.check_fail(f"Significant revenue discrepancy: {revenue_diff_pct:.2f}%!")
        
        # Feature engineering analysis
        self.add_section("Feature Engineering Quality", 2)
        
        # Check calculated fields
        self.add_line("**Calculated Fields Validation:**\n")
        
        # Cost calculation
        products_df = pd.read_csv(self.extracted_dir / 'products.csv')
        fact_with_products = fact_sales.merge(
            products_df[['Product_ID', 'Unit_Cost']], 
            left_on='product_id', 
            right_on='Product_ID', 
            how='left'
        )
        
        fact_with_products['expected_cost'] = fact_with_products['quantity'] * fact_with_products['Unit_Cost']
        cost_errors = abs(fact_with_products['cost'] - fact_with_products['expected_cost']) > 0.01
        
        if cost_errors.sum() == 0:
            self.check_pass("Cost calculation is accurate (quantity × unit_cost)")
        else:
            self.check_fail(f"{cost_errors.sum()} transactions have incorrect cost calculations")
        
        # Gross profit calculation
        gross_profit_errors = abs(
            fact_sales['gross_profit'] - (fact_sales['total_revenue'] - fact_sales['cost'])
        ) > 0.01
        
        if gross_profit_errors.sum() == 0:
            self.check_pass("Gross profit calculation is accurate (revenue - cost)")
        else:
            self.check_fail(f"{gross_profit_errors.sum()} transactions have incorrect gross profit")
        
        # Net profit validation
        self.add_section("Net Profit Calculation Validation", 3)
        
        # Recalculate net profit to verify
        fact_sales['verified_net_profit'] = (
            fact_sales['total_revenue'] - 
            fact_sales['cost'] - 
            fact_sales['shipping_cost_total'] - 
            fact_sales['allocated_marketing_dzd']
        )
        
        net_profit_errors = abs(fact_sales['net_profit'] - fact_sales['verified_net_profit']) > 0.01
        
        if net_profit_errors.sum() == 0:
            self.check_pass("Net profit formula verified: Revenue - Cost - Shipping - Marketing")
        else:
            self.check_fail(f"{net_profit_errors.sum()} transactions have incorrect net profit calculation")
        
        # Profit distribution analysis
        profit_dist = {
            'Category': [
                'Positive Profit',
                'Negative Profit',
                'Break-even (±1%)',
                'Total'
            ],
            'Count': [
                (fact_sales['net_profit'] > 0).sum(),
                (fact_sales['net_profit'] < 0).sum(),
                ((fact_sales['net_profit'] >= -fact_sales['total_revenue']*0.01) & 
                 (fact_sales['net_profit'] <= fact_sales['total_revenue']*0.01)).sum(),
                len(fact_sales)
            ],
            'Percentage': [
                f"{(fact_sales['net_profit'] > 0).sum()/len(fact_sales)*100:.1f}%",
                f"{(fact_sales['net_profit'] < 0).sum()/len(fact_sales)*100:.1f}%",
                f"{((fact_sales['net_profit'] >= -fact_sales['total_revenue']*0.01) & (fact_sales['net_profit'] <= fact_sales['total_revenue']*0.01)).sum()/len(fact_sales)*100:.1f}%",
                "100.0%"
            ]
        }
        
        self.add_table(pd.DataFrame(profit_dist))
        
        negative_pct = (fact_sales['net_profit'] < 0).sum() / len(fact_sales) * 100
        
        if negative_pct < 10:
            self.check_pass(f"Negative profit transactions: {negative_pct:.1f}% (acceptable)")
        elif negative_pct < 20:
            self.check_warning(f"Negative profit transactions: {negative_pct:.1f}% (moderate)")
        else:
            self.check_fail(f"Negative profit transactions: {negative_pct:.1f}% (too high!)")
        
        # Marketing allocation analysis
        self.add_section("Marketing Cost Allocation Analysis", 3)
        
        marketing_stats = fact_sales['allocated_marketing_dzd'].describe()
        
        # Check if marketing exceeds 30% of revenue (safety cap)
        excessive_marketing = (
            fact_sales['allocated_marketing_dzd'] > fact_sales['total_revenue'] * 0.30
        ).sum()
        
        marketing_summary = {
            'Metric': [
                'Avg Marketing per Transaction',
                'Max Marketing Allocated',
                'Transactions with Marketing',
                'Excessive Marketing (>30% revenue)',
                'Marketing as % of Revenue'
            ],
            'Value': [
                f"{marketing_stats['mean']:,.0f} DZD",
                f"{marketing_stats['max']:,.0f} DZD",
                f"{(fact_sales['allocated_marketing_dzd'] > 0).sum():,}",
                f"{excessive_marketing} ({excessive_marketing/len(fact_sales)*100:.1f}%)",
                f"{(fact_sales['allocated_marketing_dzd'].sum() / fact_sales['total_revenue'].sum() * 100):.2f}%"
            ]
        }
        
        self.add_table(pd.DataFrame(marketing_summary))
        
        if excessive_marketing == 0:
            self.check_pass("Marketing allocation capped properly (≤30% of revenue)")
        else:
            self.check_fail(f"{excessive_marketing} transactions exceed 30% marketing cap!")
        
        avg_marketing_pct = fact_sales['allocated_marketing_dzd'].sum() / fact_sales['total_revenue'].sum() * 100
        
        if 5 <= avg_marketing_pct <= 20:
            self.check_pass(f"Overall marketing ratio: {avg_marketing_pct:.2f}% (industry standard: 5-20%)")
        else:
            self.check_warning(f"Marketing ratio {avg_marketing_pct:.2f}% outside typical range (5-20%)")
    
    # ============================================
    # DIMENSION TABLE QUALITY
    # ============================================
    
    def analyze_dimension_tables(self):
        """Analyze quality of dimension tables"""
        self.add_section("📊 Dimension Tables Quality", 1)
        
        # Dim_Product analysis
        self.add_section("Dim_Product Analysis", 2)
        
        dim_product = pd.read_csv(self.transformed_dir / 'Dim_Product.csv')
        
        product_quality = {
            'Check': [
                'Total Products',
                'Unique Product IDs',
                'Products with Sentiment',
                'Avg Sentiment Score',
                'Products with Competitor Price',
                'Avg Price Difference',
                'Missing Category',
                'Missing Subcategory'
            ],
            'Result': [
                f"{len(dim_product)}",
                f"{dim_product['product_id'].nunique()}",
                f"{dim_product['avg_sentiment'].notna().sum()} ({dim_product['avg_sentiment'].notna().sum()/len(dim_product)*100:.1f}%)",
                f"{dim_product['avg_sentiment'].mean():.3f}",
                f"{dim_product['competitor_price'].notna().sum()} ({dim_product['competitor_price'].notna().sum()/len(dim_product)*100:.1f}%)",
                f"{dim_product['price_difference_pct'].mean():.2f}%",
                f"{dim_product['category_name'].isnull().sum()}",
                f"{dim_product['subcat_name'].isnull().sum()}"
            ]
        }
        
        self.add_table(pd.DataFrame(product_quality))
        
        # Sentiment analysis quality
        sentiment_coverage = dim_product['avg_sentiment'].notna().sum() / len(dim_product) * 100
        
        if sentiment_coverage >= 90:
            self.check_pass(f"Sentiment analysis coverage: {sentiment_coverage:.1f}%")
        elif sentiment_coverage >= 70:
            self.check_warning(f"Sentiment analysis coverage: {sentiment_coverage:.1f}%")
        else:
            self.check_fail(f"Low sentiment coverage: {sentiment_coverage:.1f}%")
        
        # Competitor price matching quality
        competitor_coverage = dim_product['competitor_price'].notna().sum() / len(dim_product) * 100
        
        if competitor_coverage >= 80:
            self.check_pass(f"Competitor price matching: {competitor_coverage:.1f}%")
        elif competitor_coverage >= 30:
            self.check_warning(f"Competitor price matching: {competitor_coverage:.1f}%")
        else:
            self.check_fail(f"Low competitor price matching: {competitor_coverage:.1f}%")
        
        # Dim_Date analysis
        self.add_section("Dim_Date Analysis", 2)
        
        dim_date = pd.read_csv(self.transformed_dir / 'Dim_Date.csv')
        
        # Check date continuity
        dim_date['date'] = pd.to_datetime(dim_date['date'])
        dim_date_sorted = dim_date.sort_values('date')
        
        date_gaps = (dim_date_sorted['date'].diff() > pd.Timedelta(days=1)).sum()
        
        date_quality = {
            'Check': [
                'Total Dates',
                'Date Range',
                'Date Gaps (>1 day)',
                'Weekdays',
                'Weekends',
                'Duplicates'
            ],
            'Result': [
                f"{len(dim_date)}",
                f"{dim_date['date'].min()} to {dim_date['date'].max()}",
                f"{date_gaps}",
                f"{dim_date['day_of_week'].isin([0,1,2,3,4]).sum()}",
                f"{dim_date['day_of_week'].isin([5,6]).sum()}",
                f"{dim_date.duplicated(subset=['date']).sum()}"
            ]
        }
        
        self.add_table(pd.DataFrame(date_quality))
        
        if dim_date.duplicated(subset=['date']).sum() == 0:
            self.check_pass("All dates are unique")
        else:
            self.check_fail(f"{dim_date.duplicated(subset=['date']).sum()} duplicate dates found!")
    
    # ============================================
    # DATABASE INTEGRITY
    # ============================================
    
    def analyze_database_integrity(self):
        """Analyze database vs CSV integrity"""
        self.add_section("🗄️ Database vs CSV Integrity", 1)
        
        if not self.db_path.exists():
            self.check_fail("Database file not found!")
            return
        
        conn = sqlite3.connect(self.db_path)
        
        # Compare CSV vs Database row counts
        tables = {
            'Dim_Customer': 'Dim_Customer.csv',
            'Dim_Date': 'Dim_Date.csv',
            'Dim_Product': 'Dim_Product.csv',
            'Dim_Store': 'Dim_Store.csv',
            'Fact_Sales': 'Fact_Sales.csv'
        }
        
        comparison = []
        
        for table_name, csv_file in tables.items():
            csv_path = self.transformed_dir / csv_file
            
            if csv_path.exists():
                csv_rows = len(pd.read_csv(csv_path))
            else:
                csv_rows = 0
            
            try:
                db_rows = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", conn)['count'].iloc[0]
            except:
                db_rows = 0
            
            match = '✅' if csv_rows == db_rows else '❌'
            
            comparison.append({
                'Table': table_name,
                'CSV Rows': f"{csv_rows:,}",
                'DB Rows': f"{db_rows:,}",
                'Match': match
            })
            
            if csv_rows == db_rows:
                self.check_pass(f"{table_name}: CSV and DB match ({csv_rows:,} rows)")
            else:
                self.check_fail(f"{table_name}: CSV ({csv_rows:,}) != DB ({db_rows:,})")
        
        self.add_table(pd.DataFrame(comparison))
        
        conn.close()
    
    # ============================================
    # GENERATE REPORT
    # ============================================
    
    def generate_report(self):
        """Generate comprehensive quality report"""
        self.add_section("🔍 Enhanced Data Quality & Feature Engineering Audit", 1)
        self.add_line(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.add_line("---\n")
        
        # Run all analyses
        print("[1/5] Analyzing original data...")
        self.analyze_original_data()
        
        print("[2/5] Analyzing transformation quality...")
        self.analyze_transformation_quality()
        
        print("[3/5] Analyzing dimension tables...")
        self.analyze_dimension_tables()
        
        print("[4/5] Analyzing database integrity...")
        self.analyze_database_integrity()
        
        # Generate summary
        print("[5/5] Generating summary...")
        
        score_pct = (self.quality_score / self.total_checks * 100) if self.total_checks > 0 else 0
        
        summary_lines = []
        summary_lines.append(f"\n## 📋 Quality Score Summary\n")
        summary_lines.append(f"**Total Checks**: {self.total_checks}")
        summary_lines.append(f"**Quality Score**: {self.quality_score:.1f} / {self.total_checks}")
        summary_lines.append(f"**Percentage**: {score_pct:.1f}%\n")
        
        if score_pct >= 95:
            grade = "A+ (Excellent)"
            emoji = "🌟"
        elif score_pct >= 85:
            grade = "A (Very Good)"
            emoji = "✅"
        elif score_pct >= 75:
            grade = "B (Good)"
            emoji = "👍"
        elif score_pct >= 60:
            grade = "C (Acceptable)"
            emoji = "⚠️"
        else:
            grade = "D (Needs Improvement)"
            emoji = "❌"
        
        summary_lines.append(f"{emoji} **Overall Quality Grade**: {grade}\n")
        summary_lines.append("---\n")
        
        # Insert summary at beginning
        self.report_lines = self.report_lines[:3] + summary_lines + self.report_lines[3:]
        
        return "\n".join(self.report_lines)
    
    def save_report(self, filename: str = "DATA_QUALITY_REPORT.md"):
        """Save report to file"""
        report_content = self.generate_report()
        
        output_path = self.project_root / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print("\n" + "="*70)
        print("DATA QUALITY AUDIT COMPLETED")
        print("="*70)
        print(f"✅ Report saved to: {output_path}")
        print(f"📊 Quality Score: {self.quality_score:.1f} / {self.total_checks}")
        print(f"📈 Percentage: {self.quality_score/self.total_checks*100:.1f}%")
        print("="*70)
        
        return output_path


def main():
    """Main execution"""
    auditor = EnhancedDataQualityAuditor()
    report_path = auditor.save_report()
    
    print(f"\n📄 Quality report generated: {report_path}\n")


if __name__ == "__main__":
    main()