# extract_mysql.py
import mysql.connector
import pandas as pd
import os
from datetime import datetime
import logging
import sys

# Configuration du logging avec support UTF-8 pour Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extraction.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Force UTF-8 pour la console Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

class MySQLExtractor:
    """Classe pour gérer l'extraction des données MySQL"""
    
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        
    def connect(self):
        """Établir la connexion à la base de données"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=30,
                charset='utf8mb4'
            )
            logging.info(f"✓ Connexion réussie à {self.database}")
            return True
        except mysql.connector.Error as err:
            logging.error(f"✗ Erreur de connexion: {err}")
            return False
    
    def extract_table(self, table_name, output_dir='data/extracted'):
        """
        Extraire une table complète et la sauvegarder en CSV
        
        Args:
            table_name (str): Nom de la table à extraire
            output_dir (str): Répertoire de destination
            
        Returns:
            pd.DataFrame: DataFrame contenant les données extraites
        """
        try:
            logging.info(f"[EXTRACTION] Table: {table_name}")
            
            # Requête SQL pour extraire toutes les données
            query = f"SELECT * FROM {table_name}"
            
            # Charger dans un DataFrame Pandas
            df = pd.read_sql(query, self.connection)
            
            # Créer le répertoire si nécessaire
            os.makedirs(output_dir, exist_ok=True)
            
            # Nom du fichier de sortie
            output_file = f"{output_dir}/{table_name.replace('table_', '')}.csv"
            
            # Sauvegarder en CSV
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logging.info(f"  → {len(df)} lignes extraites")
            logging.info(f"  → Fichier: {output_file}")
            
            return df
            
        except Exception as e:
            logging.error(f"✗ Erreur lors de l'extraction de {table_name}: {e}")
            return None
    
    def extract_with_query(self, query, output_file, description=""):
        """
        Extraire des données avec une requête personnalisée
        
        Args:
            query (str): Requête SQL personnalisée
            output_file (str): Chemin du fichier de sortie
            description (str): Description de l'extraction
        """
        try:
            logging.info(f"[REQUÊTE PERSONNALISÉE] {description}")
            
            df = pd.read_sql(query, self.connection)
            
            # Créer le répertoire si nécessaire
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logging.info(f"  → {len(df)} lignes extraites")
            logging.info(f"  → Fichier: {output_file}")
            
            return df
            
        except Exception as e:
            logging.error(f"✗ Erreur: {e}")
            return None
    
    def get_table_info(self, table_name):
        """
        Obtenir des informations sur une table
        
        Args:
            table_name (str): Nom de la table
        """
        try:
            # Nombre de lignes
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count = pd.read_sql(count_query, self.connection).iloc[0]['count']
            
            # Structure de la table
            structure_query = f"DESCRIBE {table_name}"
            structure = pd.read_sql(structure_query, self.connection)
            
            logging.info(f"\n[INFO] {table_name}:")
            logging.info(f"  → Lignes: {count}")
            logging.info(f"  → Colonnes: {', '.join(structure['Field'].tolist())}")
            
            return count, structure
            
        except Exception as e:
            logging.error(f"✗ Erreur: {e}")
            return None, None
    
    def extract_all_tables(self):
        """Extraire toutes les tables nécessaires du projet"""
        
        tables = [
            'table_sales',
            'table_products',
            'table_reviews',
            'table_customers',
            'table_stores',
            'table_cities',
            'table_categories',
            'table_subcategories'
        ]
        
        logging.info("\n" + "="*70)
        logging.info(" DÉBUT DE L'EXTRACTION COMPLÈTE")
        logging.info("="*70 + "\n")
        
        extraction_summary = []
        
        for table in tables:
            # Obtenir les infos avant extraction
            count, structure = self.get_table_info(table)
            
            # Extraire la table
            df = self.extract_table(table)
            
            if df is not None:
                extraction_summary.append({
                    'Table': table,
                    'Lignes': len(df),
                    'Colonnes': len(df.columns),
                    'Statut': '✓ Succès'
                })
            else:
                extraction_summary.append({
                    'Table': table,
                    'Lignes': 0,
                    'Colonnes': 0,
                    'Statut': '✗ Échec'
                })
            
            logging.info("\n" + "-"*70 + "\n")
        
        # Afficher le résumé
        logging.info("\n" + "="*70)
        logging.info(" RÉSUMÉ DE L'EXTRACTION")
        logging.info("="*70 + "\n")
        
        summary_df = pd.DataFrame(extraction_summary)
        logging.info(f"\n{summary_df.to_string(index=False)}\n")
        
        # Sauvegarder le résumé
        summary_df.to_csv('data/extracted/extraction_summary.csv', index=False)
        
        return extraction_summary
    
    def close(self):
        """Fermer la connexion"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("✓ Connexion fermée")


def main():
    """Fonction principale d'extraction"""
    
    # Configuration de la connexion
    CONFIG = {
        'host': 'boughida.com',
        'database': 'techstore_erp',
        'user': 'student_user_4ing',
        'password': 'bi_guelma_2025'
    }
    
    # Créer l'extracteur
    extractor = MySQLExtractor(**CONFIG)
    
    # Se connecter
    if extractor.connect():
        
        # Extraire toutes les tables
        extractor.extract_all_tables()
        
        # Extraction personnalisée : Ventes avec détails produits
        # CORRECTION: Trans_ID au lieu de Sale_ID, Customer_Name -> Full_Name
        custom_query = """
        SELECT 
            s.Trans_ID,
            s.Date,
            s.Quantity,
            s.Total_Revenue,
            p.Product_Name,
            p.Unit_Cost,
            sc.SubCat_Name,
            c.Category_Name,
            st.Store_Name,
            cu.Full_Name as Customer_Name,
            ci.City_Name as Customer_City,
            ci.Region as Customer_Region
        FROM table_sales s
        JOIN table_products p ON s.Product_ID = p.Product_ID
        JOIN table_subcategories sc ON p.SubCat_ID = sc.SubCat_ID
        JOIN table_categories c ON sc.Category_ID = c.Category_ID
        JOIN table_stores st ON s.Store_ID = st.Store_ID
        JOIN table_customers cu ON s.Customer_ID = cu.Customer_ID
        JOIN table_cities ci ON cu.City_ID = ci.City_ID
        LIMIT 1000
        """
        
        extractor.extract_with_query(
            custom_query,
            'data/extracted/sales_detailed.csv',
            'Ventes avec détails complets (1000 premières lignes)'
        )
        
        # Fermer la connexion
        extractor.close()
        
        logging.info("\n" + "="*70)
        logging.info("✓ EXTRACTION TERMINÉE AVEC SUCCÈS")
        logging.info("="*70)
        
    else:
        logging.error("✗ Impossible de se connecter à la base de données")


if __name__ == "__main__":
    main()