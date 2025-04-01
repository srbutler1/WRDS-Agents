from __future__ import annotations
import os
import sqlite3
import pandas as pd
import datetime
from typing import Dict, Any, List, Optional

# Constants
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
DB_FILE = os.path.join(DATA_DIR, "wrds_data.db")

class DataStorage:
    """Class for handling data storage in both CSV files and SQLite database."""
    
    def __init__(self):
        """Initialize the DataStorage class."""
        self._ensure_data_directory()
        self._initialize_database()
    
    def _ensure_data_directory(self):
        """Ensure the data directory exists."""
        os.makedirs(DATA_DIR, exist_ok=True)
    
    def _initialize_database(self):
        """Initialize the SQLite database with necessary tables."""
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_text TEXT,
            timestamp TEXT,
            user_query TEXT,
            enriched_query TEXT
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER,
            date TEXT,
            ticker TEXT,
            price REAL,
            return_value REAL,
            volume INTEGER,
            FOREIGN KEY (query_id) REFERENCES queries(id)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER,
            fiscal_year INTEGER,
            ticker TEXT,
            total_assets REAL,
            total_liabilities REAL,
            net_sales REAL,
            net_income REAL,
            FOREIGN KEY (query_id) REFERENCES queries(id)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS analyst_estimates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id INTEGER,
            ticker TEXT,
            forecast_date TEXT,
            mean_estimate REAL,
            median_estimate REAL,
            num_estimates INTEGER,
            FOREIGN KEY (query_id) REFERENCES queries(id)
        )
        """)
        
        conn.commit()
        conn.close()
    
    def save_query(self, sql_query: str, user_query: str, enriched_query: str = "") -> int:
        """Save the query to the database and return the query ID."""
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        timestamp = datetime.datetime.now().isoformat()
        cursor.execute(
            "INSERT INTO queries (query_text, timestamp, user_query, enriched_query) VALUES (?, ?, ?, ?)",
            (sql_query, timestamp, user_query, enriched_query)
        )
        
        query_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return query_id
    
    def save_to_csv(self, data_type: str, ticker: str, df: pd.DataFrame) -> str:
        """Save data to a CSV file and return the file path."""
        # Generate a filename based on data type, ticker, and timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{data_type}_{ticker}_{timestamp}.csv"
        filepath = os.path.join(DATA_DIR, filename)
        
        # Save to CSV
        df.to_csv(filepath, index=False)
        print(f"Data saved to {filepath}")
        
        return filepath
    
    def save_to_sqlite(self, data_type: str, df: pd.DataFrame, query_id: int) -> None:
        """Save data to the SQLite database."""
        conn = sqlite3.connect(DB_FILE)
        
        if data_type == "stock_prices":
            # Prepare data for stock_prices table
            for _, row in df.iterrows():
                conn.execute(
                    "INSERT INTO stock_prices (query_id, date, ticker, price, return_value, volume) VALUES (?, ?, ?, ?, ?, ?)",
                    (query_id, row.get("date"), row.get("ticker"), row.get("prc"), row.get("ret"), row.get("vol", 0))
                )
        elif data_type == "fundamentals":
            # Prepare data for fundamentals table
            for _, row in df.iterrows():
                conn.execute(
                    "INSERT INTO fundamentals (query_id, fiscal_year, ticker, total_assets, total_liabilities, net_sales, net_income) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (query_id, row.get("fyear"), row.get("ticker"), row.get("at"), row.get("lt"), row.get("sale"), row.get("ni"))
                )
        elif data_type == "analyst_estimates":
            # Prepare data for analyst_estimates table
            for _, row in df.iterrows():
                conn.execute(
                    "INSERT INTO analyst_estimates (query_id, ticker, forecast_date, mean_estimate, median_estimate, num_estimates) VALUES (?, ?, ?, ?, ?, ?)",
                    (query_id, row.get("ticker"), row.get("fpedats"), row.get("meanest"), row.get("medest"), row.get("numest"))
                )
        
        conn.commit()
        conn.close()
    
    def get_query_by_id(self, query_id: int) -> Dict[str, Any]:
        """Get a query by its ID."""
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM queries WHERE id = ?", (query_id,))
        row = cursor.fetchone()
        
        if row:
            query = dict(row)
        else:
            query = None
        
        conn.close()
        return query
    
    def get_data_by_query_id(self, query_id: int, data_type: str) -> List[Dict[str, Any]]:
        """Get data by query ID and data type."""
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if data_type == "stock_prices":
            cursor.execute("SELECT * FROM stock_prices WHERE query_id = ?", (query_id,))
        elif data_type == "fundamentals":
            cursor.execute("SELECT * FROM fundamentals WHERE query_id = ?", (query_id,))
        elif data_type == "analyst_estimates":
            cursor.execute("SELECT * FROM analyst_estimates WHERE query_id = ?", (query_id,))
        else:
            conn.close()
            return []
        
        rows = cursor.fetchall()
        data = [dict(row) for row in rows]
        
        conn.close()
        return data
