"""
Database Connector for TechStore Dashboard
Handles all SQLite connections and query execution
FIXED: Thread-safe connections for Streamlit
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import threading

class DatabaseConnector:
    """Manages thread-safe connection to the TechStore Data Warehouse"""
    
    def __init__(self, db_path: str = None):
        """
        Initialize database connector
        
        Args:
            db_path: Path to SQLite database file
        """
        if db_path is None:
            # Auto-detect database path (relative to project root)
            current_dir = Path(__file__).parent.parent.parent
            db_path = current_dir / "database" / "techstore_dw.db"
        
        self.db_path = Path(db_path)
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        # Thread-local storage for connections
        self._local = threading.local()
    
    def connect(self) -> sqlite3.Connection:
        """
        Establish thread-safe database connection
        Each thread gets its own connection
        """
        # Check if current thread has a connection
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            # Create new connection for this thread
            self._local.connection = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False  # Allow usage across threads
            )
        
        return self._local.connection
    
    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame (THREAD-SAFE)
        
        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries
            
        Returns:
            pd.DataFrame: Query results
        """
        # Get thread-specific connection
        conn = self.connect()
        
        try:
            if params:
                df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            raise Exception(f"Query execution failed: {e}\nQuery: {query}")
    
    def get_table_list(self) -> List[str]:
        """Get list of all tables in database"""
        query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        result = self.execute_query(query)
        return result['name'].tolist()
    
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema information for a specific table"""
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)
    
    def get_table_data(self, table_name: str, limit: int = None) -> pd.DataFrame:
        """
        Retrieve all data from a specific table
        
        Args:
            table_name: Name of the table
            limit: Optional row limit
            
        Returns:
            pd.DataFrame: Table data
        """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query)
    
    def get_row_count(self, table_name: str) -> int:
        """Get total row count for a table"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return int(result['count'].iloc[0])
    
    def close(self):
        """Close thread-specific database connection"""
        if hasattr(self._local, 'connection') and self._local.connection is not None:
            self._local.connection.close()
            self._local.connection = None
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Create a new connection for each request (Streamlit-friendly)
def get_db_connection() -> DatabaseConnector:
    """
    Get a new database connector instance
    DO NOT cache this in Streamlit - create fresh each time
    """
    return DatabaseConnector()