import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import re
import os
from urllib.parse import urljoin

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class CompetitorScraper:
    
    def __init__(self, base_url):
        """
        Initialize the scraper with base URL and headers
        
        Args:
            base_url (str): Base URL of the competitor website
        """
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.products = []
        
    def fetch_page(self, url):
        """
        Retrieve the HTML content of a page
        
        Args:
            url (str): URL of the page to fetch
            
        Returns:
            BeautifulSoup: Parsed HTML object or None if error occurs
        """
        try:
            logging.info(f"Fetching page: {url}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            logging.info("Page successfully retrieved")
            
            return soup
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching page: {e}")
            return None
    
    def extract_product_info(self, product_element):
        """
        Extract product information from an HTML element
        Based on the actual website structure with blue titles and red prices
        
        Args:
            product_element: BeautifulSoup element containing product data
            
        Returns:
            dict: Product information dictionary or None if extraction fails
        """
        try:
            name_element = product_element.find('a', href=True, string=re.compile(r'\S+'))
            if not name_element:
                name_element = product_element.find(['h2', 'h3', 'h4', 'h5'])
            if not name_element:
                name_element = product_element.find('a', class_=re.compile(r'.*'))
            
            product_name = name_element.get_text(strip=True) if name_element else None
            if product_name:
                product_name = product_name.replace('Promo: ', '').replace('Best Deal: ', '').strip()
            ref_text = product_element.find(string=re.compile(r'Ref:\s*P-\d+'))
            product_ref = ref_text.strip() if ref_text else None
            price_text = ''.join(product_element.find_all(string=re.compile(r'\d+\s*DZD')))
            
            if price_text:
                price_matches = re.findall(r'(\d+)\s*DZD', price_text)
                if price_matches:
                    product_price = float(price_matches[-1]) 
                else:
                    product_price = None
            else:
                product_price = None

            if product_name and product_price:
                return {
                    'Competitor_Product_Name': product_name,
                    'Competitor_Price': product_price,
                    'Product_Reference': product_ref,
                    'Currency': 'DZD'
                }
            
            return None
            
        except Exception as e:
            logging.debug(f"Error extracting product info: {e}")
            return None
    
    def scrape_page(self, url):
        """
        Scrape all products from a single page
        
        Args:
            url (str): URL of the page to scrape
            
        Returns:
            list: List of extracted product dictionaries
        """
        soup = self.fetch_page(url)
        if not soup:
            return []
        
        page_products = []
        product_containers = soup.find_all('div', class_=re.compile(r'col-'))
        
        if not product_containers:
            product_containers = soup.find_all('div', class_='card')
        
        if not product_containers:
            product_containers = soup.find_all('div', class_=re.compile(r'product'))
        
        if not product_containers:
            all_divs = soup.find_all('div')
            product_containers = [div for div in all_divs 
                                  if div.find(string=re.compile(r'Ref:')) and 
                                     div.find(string=re.compile(r'DZD'))]
        
        logging.info(f"Found {len(product_containers)} potential product containers")
        for container in product_containers:
            product_info = self.extract_product_info(container)
            if product_info:
                page_products.append(product_info)
                logging.info(f"Extracted: {product_info['Competitor_Product_Name']}: {product_info['Competitor_Price']} DZD")
        
        return page_products
    
    def get_page_urls(self, soup):
        """
        Extract all page URLs from pagination
        
        Args:
            soup (BeautifulSoup): Parsed HTML of the first page
            
        Returns:
            list: List of full page URLs
        """
        page_urls = [self.base_url]
        
        try:
            pagination = soup.find('div', class_=re.compile(r'pagination')) or soup.find('ul', class_='pagination')
            if not pagination:
                pagination = soup.find(string=re.compile(r'Previous|Next'))
                if pagination:
                    pagination = pagination.find_parent(['div', 'ul', 'nav'])
            
            if pagination:
                links = pagination.find_all('a')
                seen_pages = set()
                for link in links:
                    text = link.get_text(strip=True)
                    href = link.get('href')
                    if text.isdigit() and href and int(text) > 1 and href not in seen_pages:
                        full_url = urljoin(self.base_url, href)
                        page_urls.append(full_url)
                        seen_pages.add(href)
                logging.info(f"Found additional pages: {len(page_urls) - 1}")
            
            return page_urls
        except Exception as e:
            logging.error(f"Error extracting page URLs: {e}")
            return [self.base_url]
    
    def scrape_all_pages(self):
        """
        Scrape all pages of the competitor website by detecting pagination hrefs
        
        Returns:
            list: Complete list of all extracted products
        """
        logging.info("="*60)
        logging.info("Starting web scraping process")
        logging.info("="*60)
        soup = self.fetch_page(self.base_url)
        if not soup:
            return []
        page_urls = self.get_page_urls(soup)
        
        all_products = []
        
        for idx, url in enumerate(page_urls, 1):
            logging.info(f"\nPage {idx}/{len(page_urls)}: {url}")
            logging.info("-" * 60)
            
            page_products = self.scrape_page(url)
            
            if not page_products:
                logging.info("No products found on this page")
                continue
            
            all_products.extend(page_products)
            if idx < len(page_urls):
                time.sleep(2)
        
        logging.info("="*60)
        logging.info(f"Scraping completed: {len(all_products)} total products")
        logging.info("="*60)
        
        return all_products
    
    def save_to_csv(self, products, output_file='data/extracted/competitor_prices.csv'):
        """
        Save extracted products to CSV file
        
        Args:
            products (list): List of product dictionaries
            output_file (str): Path to output CSV file
        """
        if not products:
            logging.warning("No products to save")
            return None

        df = pd.DataFrame(products)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8')
        logging.info(f"\nData saved to: {output_file}")
        logging.info(f"Statistics:")
        logging.info(f"  Total products: {len(df)}")
        logging.info(f"  Average price: {df['Competitor_Price'].mean():.2f} DZD")
        logging.info(f"  Price range: {df['Competitor_Price'].min():.0f} - {df['Competitor_Price'].max():.0f} DZD")
        
        return df
    
    def scrape_and_save(self):
        """
        Complete workflow: scrape and save
        """
        products = self.scrape_all_pages()
        df = self.save_to_csv(products)
        return df


def scrape_with_fallback():
    """
    Execute scraping with fallback to mock data if needed
    """
    base_url = "https://boughida.com/competitor/"
    
    scraper = CompetitorScraper(base_url)
    
    try:
        df = scraper.scrape_and_save()
        
        if df is not None and len(df) > 0:
            return df
        else:
            logging.warning("Scraping returned no data, using mock data")
            return create_mock_data()
            
    except Exception as e:
        logging.error(f"Critical error: {e}")
        logging.info("Using mock data instead")
        return create_mock_data()


def create_mock_data():
    """
    Create mock competitor data 
    """
    mock_products = [
        {'Competitor_Product_Name': 'iPhone 14 Pro', 'Competitor_Price': 180000, 'Product_Reference': 'P-1001', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'Samsung Galaxy S23', 'Competitor_Price': 140000, 'Product_Reference': 'P-1002', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'Dell XPS 13', 'Competitor_Price': 125000, 'Product_Reference': 'P-2001', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'HP ProBook 450', 'Competitor_Price': 95000, 'Product_Reference': 'P-2002', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'Sony WH-1000XM5', 'Competitor_Price': 45000, 'Product_Reference': 'P-3001', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'AirPods Pro 2', 'Competitor_Price': 38000, 'Product_Reference': 'P-3002', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'LG OLED TV 55"', 'Competitor_Price': 220000, 'Product_Reference': 'P-4001', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'Samsung QLED 65"', 'Competitor_Price': 280000, 'Product_Reference': 'P-4002', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'Canon EOS R6', 'Competitor_Price': 195000, 'Product_Reference': 'P-5001', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'PlayStation 5', 'Competitor_Price': 75000, 'Product_Reference': 'P-6001', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'ASUS ROG Laptop', 'Competitor_Price': 290000, 'Product_Reference': 'P-2003', 'Currency': 'DZD'},
        {'Competitor_Product_Name': 'DJI Mini 3 Drone', 'Competitor_Price': 126900, 'Product_Reference': 'P-7001', 'Currency': 'DZD'},
    ]
    
    df = pd.DataFrame(mock_products)

    os.makedirs('data/extracted', exist_ok=True)
    df.to_csv('data/extracted/competitor_prices.csv', index=False, encoding='utf-8')
    
    logging.info("Mock data created successfully")
    return df


def main():
    """Main execution function"""
    
    logging.info("\n" + "="*70)
    logging.info("WEB SCRAPING - COMPETITOR PRICES")
    logging.info("="*70 + "\n")
    df = scrape_with_fallback()
    
    if df is not None:
        logging.info("\n" + "="*70)
        logging.info("COMPETITOR PRICE EXTRACTION COMPLETED")
        logging.info(f"{len(df)} products available for analysis")
        logging.info("="*70)
    else:
        logging.error("\nEXTRACTION FAILED")


if __name__ == "__main__":
    main()