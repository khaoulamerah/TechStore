
import mysql.connector
import pandas as pd
import os
from datetime import datetime
import logging

# Configuration du logging pour suivre l'exécution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extraction.log'),
        logging.StreamHandler()
    ]
)

class MySQLExtractor:
    # Classe pour gérer l'extraction des données MySQL
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        
    def connect(self):
        # Établir la connexion à la base de données
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=30
            )
            logging.info(f"Connexion réussie à {self.database}")
            return True
        except mysql.connector.Error as err:
            logging.error(f"Erreur de connexion: {err}")
            return False
    



    def extract_table(self, table_name, output_dir='data/extracted'):
       
        # Extraire une table complète et la sauvegarder en CSV
        # Args:
        #     table_name (str): Nom de la table à extraire
        #     output_dir (str): Répertoire de destination    
        # Returns:
        #     pd.DataFrame: DataFrame contenant les données extraites
        
        try:
            logging.info(f"📊 Extraction de la table: {table_name}")
            
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
            
            logging.info(f"{len(df)} lignes extraites de {table_name}")
            logging.info(f"Fichier sauvegardé: {output_file}")
            
            return df
            
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction de {table_name}: {e}")
            return None
    


    def extract_with_query(self, query, output_file, description=""):

        # Extraire des données avec une requête personnalisée  
        # Args:
        #     query (str): Requête SQL personnalisée
        #     output_file (str): Chemin du fichier de sortie
        #     description (str): Description de l'extraction

        try:
            logging.info(f"Extraction personnalisée: {description}")
            
            df = pd.read_sql(query, self.connection)
            
            # Créer le répertoire si nécessaire
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            logging.info(f"{len(df)} lignes extraites")
            logging.info(f"Fichier sauvegardé: {output_file}")
            
            return df
            
        except Exception as e:
            logging.error(f"Erreur: {e}")
            return None
        

    
    def get_table_info(self, table_name):

        # Obtenir des informations sur une table
        # Args:
        #     table_name (str): Nom de la table

        try:
            # Nombre de lignes
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count = pd.read_sql(count_query, self.connection).iloc[0]['count']
            
            # Structure de la table
            structure_query = f"DESCRIBE {table_name}"
            structure = pd.read_sql(structure_query, self.connection)
            
            logging.info(f"\nInformations sur {table_name}:")
            logging.info(f"   Nombre de lignes: {count}")
            logging.info(f"   Colonnes: {', '.join(structure['Field'].tolist())}")
            
            return count, structure
            
        except Exception as e:
            logging.error(f"Erreur: {e}")
            return None, None
    

    def extract_all_tables(self):
        # Extraire toutes les tables nécessaires du projet  
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
        
        logging.info("\n" + "="*60)
        logging.info(" DÉBUT DE L'EXTRACTION COMPLÈTE")
        logging.info("="*60 + "\n")
        
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
                    'Statut': 'Succès'
                })
            else:
                extraction_summary.append({
                    'Table': table,
                    'Lignes': 0,
                    'Colonnes': 0,
                    'Statut': 'Échec'
                })
            
            logging.info("\n" + "-"*60 + "\n")
        
        # Afficher le résumé
        logging.info("\n" + "="*60)
        logging.info(" RÉSUMÉ DE L'EXTRACTION")
        logging.info("="*60 + "\n")
        
        summary_df = pd.DataFrame(extraction_summary)
        logging.info(f"\n{summary_df.to_string(index=False)}\n")
        
        # Sauvegarder le résumé
        summary_df.to_csv('data/extracted/extraction_summary.csv', index=False)
        
        return extraction_summary
    
    def close(self):
        # Fermer la connexion
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("Connexion fermée")


def main():

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
        
        # Exemple d'extraction personnalisée : Ventes avec détails produits
        custom_query = """
        SELECT 
            s.Sale_ID,
            s.Date,
            s.Quantity,
            s.Total_Revenue,
            p.Product_Name,
            p.Unit_Cost,
            c.Category_Name,
            st.Store_Name,
            cu.Customer_Name
        FROM table_sales s
        JOIN table_products p ON s.Product_ID = p.Product_ID
        JOIN table_categories c ON p.Category_ID = c.Category_ID
        JOIN table_stores st ON s.Store_ID = st.Store_ID
        JOIN table_customers cu ON s.Customer_ID = cu.Customer_ID
        LIMIT 1000
        """
        
        extractor.extract_with_query(
            custom_query,
            'data/extracted/sales_detailed.csv',
            'Ventes avec détails complets'
        )
        
        # Fermer la connexion
        extractor.close()
        
        logging.info("\n" + "="*60)
        logging.info("EXTRACTION TERMINÉE AVEC SUCCÈS")
        logging.info("="*60)
        
    else:
        logging.error("Impossible de se connecter à la base de données")


if __name__ == "__main__":
    main()