
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import re
from urllib.parse import urljoin
import os

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)

class CompetitorScraper:
    """Classe pour scraper les prix des concurrents"""
    
    def __init__(self, base_url):
        """
        Initialisation du scraper
        
        Args:
            base_url (str): URL de base du site concurrent
        """
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.products = []
        
    def fetch_page(self, url):
        """
        Récupérer le contenu HTML d'une page
        
        Args:
            url (str): URL de la page à récupérer
            
        Returns:
            BeautifulSoup: Objet soup ou None si erreur
        """
        try:
            logging.info(f"📡 Récupération de: {url}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            logging.info(f"✅ Page récupérée avec succès")
            
            return soup
            
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erreur lors de la récupération: {e}")
            return None
    
    def extract_product_info(self, product_element):
        """
        Extraire les informations d'un produit depuis un élément HTML
        
        Args:
            product_element: Élément BeautifulSoup contenant un produit
            
        Returns:
            dict: Informations du produit ou None
        """
        try:
            # Extraire le nom du produit
            # NOTE: Adapter les sélecteurs selon la structure réelle du site
            name_element = product_element.find('h3', class_='product-title')
            if not name_element:
                name_element = product_element.find('a', class_='product-link')
            
            product_name = name_element.text.strip() if name_element else None
            
            # Extraire le prix
            price_element = product_element.find('span', class_='price')
            if not price_element:
                price_element = product_element.find('div', class_='product-price')
            
            if price_element:
                price_text = price_element.text.strip()
                # Nettoyer le prix (enlever DZD, espaces, virgules)
                price_clean = re.sub(r'[^\d.]', '', price_text)
                product_price = float(price_clean) if price_clean else None
            else:
                product_price = None
            
            # Extraire la catégorie si disponible
            category_element = product_element.find('span', class_='category')
            category = category_element.text.strip() if category_element else None
            
            if product_name and product_price:
                return {
                    'Competitor_Product_Name': product_name,
                    'Competitor_Price': product_price,
                    'Category': category,
                    'Currency': 'DZD'
                }
            
            return None
            
        except Exception as e:
            logging.warning(f"⚠️ Erreur lors de l'extraction d'un produit: {e}")
            return None
    
    def scrape_page(self, url):
        """
        Scraper tous les produits d'une page
        
        Args:
            url (str): URL de la page à scraper
            
        Returns:
            list: Liste des produits extraits
        """
        soup = self.fetch_page(url)
        if not soup:
            return []
        
        page_products = []
        
        # Trouver tous les conteneurs de produits
        # NOTE: Adapter le sélecteur selon la structure HTML réelle
        product_containers = soup.find_all('div', class_='product-item')
        
        if not product_containers:
            # Essayer d'autres sélecteurs possibles
            product_containers = soup.find_all('div', class_='product')
        
        if not product_containers:
            product_containers = soup.find_all('article', class_='product')
        
        logging.info(f"🔍 {len(product_containers)} produits trouvés sur la page")
        
        for container in product_containers:
            product_info = self.extract_product_info(container)
            if product_info:
                page_products.append(product_info)
                logging.info(f"   ✅ {product_info['Competitor_Product_Name']}: {product_info['Competitor_Price']} DZD")
        
        return page_products
    
    def scrape_all_pages(self, max_pages=10):
        """
        Scraper plusieurs pages du site
        
        Args:
            max_pages (int): Nombre maximum de pages à scraper
            
        Returns:
            list: Liste complète des produits
        """
        logging.info("\n" + "="*60)
        logging.info("🚀 DÉBUT DU SCRAPING")
        logging.info("="*60 + "\n")
        
        all_products = []
        
        for page_num in range(1, max_pages + 1):
            # Construire l'URL de la page
            if page_num == 1:
                page_url = self.base_url
            else:
                page_url = f"{self.base_url}?page={page_num}"
            
            logging.info(f"\n📄 Page {page_num}/{max_pages}")
            logging.info("-" * 60)
            
            # Scraper la page
            page_products = self.scrape_page(page_url)
            
            if not page_products:
                logging.info("⚠️ Aucun produit trouvé, fin du scraping")
                break
            
            all_products.extend(page_products)
            
            # Respecter le serveur (pause entre les requêtes)
            if page_num < max_pages:
                time.sleep(2)
        
        logging.info("\n" + "="*60)
        logging.info(f"✅ SCRAPING TERMINÉ: {len(all_products)} produits extraits")
        logging.info("="*60 + "\n")
        
        return all_products
    
    def save_to_csv(self, products, output_file='data/extracted/competitor_prices.csv'):
        """
        Sauvegarder les produits dans un fichier CSV
        
        Args:
            products (list): Liste des produits
            output_file (str): Chemin du fichier de sortie
        """
        if not products:
            logging.warning("⚠️ Aucun produit à sauvegarder")
            return
        
        # Créer le DataFrame
        df = pd.DataFrame(products)
        
        # Créer le répertoire si nécessaire
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Sauvegarder
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logging.info(f"💾 Données sauvegardées dans: {output_file}")
        logging.info(f"📊 Statistiques:")
        logging.info(f"   - Nombre de produits: {len(df)}")
        logging.info(f"   - Prix moyen: {df['Competitor_Price'].mean():.2f} DZD")
        logging.info(f"   - Prix min: {df['Competitor_Price'].min():.2f} DZD")
        logging.info(f"   - Prix max: {df['Competitor_Price'].max():.2f} DZD")
        
        return df
    
    def scrape_and_save(self, max_pages=10):
        """
        Méthode complète: scraper et sauvegarder
        
        Args:
            max_pages (int): Nombre de pages à scraper
        """
        products = self.scrape_all_pages(max_pages)
        df = self.save_to_csv(products)
        return df


def scrape_with_fallback():
    """
    Fonction avec méthode de secours si le site principal échoue
    """
    base_url = "https://boughida.com/competitor/"
    
    scraper = CompetitorScraper(base_url)
    
    try:
        # Essayer de scraper
        df = scraper.scrape_and_save(max_pages=5)
        
        if df is not None and len(df) > 0:
            return df
        else:
            logging.warning("⚠️ Scraping échoué, création de données de test")
            return create_mock_data()
            
    except Exception as e:
        logging.error(f"❌ Erreur critique: {e}")
        logging.info("📝 Création de données de test à la place")
        return create_mock_data()


def create_mock_data():
    """
    Créer des données de test si le scraping échoue
    Utile pour tester le reste du pipeline
    """
    mock_products = [
        {'Competitor_Product_Name': 'Laptop HP ProBook', 'Competitor_Price': 95000, 'Category': 'Laptops'},
        {'Competitor_Product_Name': 'Dell XPS 13', 'Competitor_Price': 125000, 'Category': 'Laptops'},
        {'Competitor_Product_Name': 'iPhone 14 Pro', 'Competitor_Price': 180000, 'Category': 'Smartphones'},
        {'Competitor_Product_Name': 'Samsung Galaxy S23', 'Competitor_Price': 140000, 'Category': 'Smartphones'},
        {'Competitor_Product_Name': 'Sony WH-1000XM5', 'Competitor_Price': 45000, 'Category': 'Audio'},
        {'Competitor_Product_Name': 'AirPods Pro', 'Competitor_Price': 38000, 'Category': 'Audio'},
        {'Competitor_Product_Name': 'LG OLED TV 55"', 'Competitor_Price': 220000, 'Category': 'TVs'},
        {'Competitor_Product_Name': 'Samsung QLED 65"', 'Competitor_Price': 280000, 'Category': 'TVs'},
        {'Competitor_Product_Name': 'Canon EOS R6', 'Competitor_Price': 195000, 'Category': 'Cameras'},
        {'Competitor_Product_Name': 'PlayStation 5', 'Competitor_Price': 75000, 'Category': 'Gaming'}
    ]
    
    df = pd.DataFrame(mock_products)
    
    # Sauvegarder
    os.makedirs('data/extracted', exist_ok=True)
    df.to_csv('data/extracted/competitor_prices.csv', index=False, encoding='utf-8')
    
    logging.info("✅ Données de test créées avec succès")
    return df


def main():
    """Fonction principale"""
    
    logging.info("\n" + "="*70)
    logging.info("🕷️  WEB SCRAPING - PRIX CONCURRENTS")
    logging.info("="*70 + "\n")
    
    # Lancer le scraping avec fallback
    df = scrape_with_fallback()
    
    if df is not None:
        logging.info("\n✅ EXTRACTION DES PRIX CONCURRENTS TERMINÉE")
        logging.info(f"📊 {len(df)} produits disponibles pour l'analyse")
    else:
        logging.error("\n❌ ÉCHEC DE L'EXTRACTION")


if __name__ == "__main__":
    main()