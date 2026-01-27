
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, List, Any


class DatabaseConnector:
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize database connector
        
        Args:
            db_path: Path to SQLite database file. If None, uses default path.
        """
        if db_path is None:
            base_dir = Path(__file__).parent.parent.parent
            db_path = base_dir / 'database' / 'techstore_dw.db'
        
        self.db_path = Path(db_path)
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found at: {self.db_path}")
        self._test_connection()
    
    def _test_connection(self):
        """Test database connection on initialization"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            conn.close()
            
            if table_count == 0:
                raise ValueError("Database is empty (no tables found)")
                
        except Exception as e:
            raise ConnectionError(f"Database connection failed: {e}")
    
    def _get_connection(self) -> sqlite3.Connection:
        """
        Create a new database connection (thread-safe)
        
        Returns:
            sqlite3.Connection object
        """
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row 
        return conn
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """
        Execute a SELECT query and return results as DataFrame
        
        Args:
            query: SQL SELECT statement
            params: Query parameters for parameterized queries
            
        Returns:
            pd.DataFrame with query results
        """
        conn = self._get_connection()
        
        try:
            if params:
                df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)
            
            return df
            
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}\nQuery: {query}")
        
        finally:
            conn.close()
    
    def execute_non_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute INSERT/UPDATE/DELETE query
        
        Args:
            query: SQL statement
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            conn.commit()
            return cursor.rowcount
            
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Non-query execution failed: {e}")
        
        finally:
            conn.close()
    
    def get_table_list(self) -> List[str]:
        """
        Get list of all tables in database
        
        Returns:
            List of table names
        """
        query = """
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
        """
        
        df = self.execute_query(query)
        return df['name'].tolist()
    
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        Get schema information for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            DataFrame with columns: cid, name, type, notnull, dflt_value, pk
        """
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get total row count for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return int(result['count'].iloc[0])
    
    def get_table_data(self, table_name: str, limit: int = 100) -> pd.DataFrame:
        """
        Get sample data from a table
        
        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            
        Returns:
            DataFrame with table data
        """
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_query(query)
    
    def test_star_schema(self) -> pd.DataFrame:
        """
        Test Star Schema integrity with a sample join
        
        Returns:
            DataFrame with sample joined data
        """
        query = """
        SELECT 
            fs.Sale_ID,
            dd.Full_Date,
            dp.Product_Name,
            dp.Category_Name,
            ds.Store_Name,
            dc.Customer_Name,
            fs.Quantity,
            fs.Total_Revenue,
            fs.Net_Profit
        FROM Fact_Sales fs
        JOIN Dim_Date dd ON fs.Date_ID = dd.Date_ID
        JOIN Dim_Product dp ON fs.Product_ID = dp.Product_ID
        JOIN Dim_Store ds ON fs.Store_ID = ds.Store_ID
        JOIN Dim_Customer dc ON fs.Customer_ID = dc.Customer_ID
        LIMIT 10
        """
        
        return self.execute_query(query)


def get_db_connection() -> DatabaseConnector:
    """
    Factory function to get database connector instance
    
    Returns:
        DatabaseConnector instance
    """
    return DatabaseConnector()