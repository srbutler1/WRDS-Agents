import psycopg2
import openai
import os
import json
import pandas as pd
import sqlite3
from datetime import datetime
from typing import Dict, Any, List
from pydantic_ai import RunContext

# Define the data directory and database file
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
DB_FILE = os.path.join(DATA_DIR, 'wrds_data.db')

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Initialize SQLite database
def init_sqlite_db():
    """Initialize the SQLite database with necessary tables."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create a table to track queries
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS queries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        query_text TEXT,
        query_date TIMESTAMP,
        result_file TEXT,
        table_name TEXT
    )
    """)
    
    # Create tables for different data types
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        ticker TEXT,
        prc REAL,
        ret REAL,
        query_id INTEGER,
        FOREIGN KEY (query_id) REFERENCES queries(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fundamentals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fyear INTEGER,
        ticker TEXT,
        at REAL,
        lt REAL,
        sale REAL,
        ni REAL,
        query_id INTEGER,
        FOREIGN KEY (query_id) REFERENCES queries(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS analyst_estimates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        fpedats TEXT,
        meanest REAL,
        medest REAL,
        numest INTEGER,
        query_id INTEGER,
        FOREIGN KEY (query_id) REFERENCES queries(id)
    )
    """)
    
    conn.commit()
    conn.close()

# Initialize the database
init_sqlite_db()

async def parse_user_intent(user_query: str) -> Dict[str, Any]:
    """
    Parse the user's natural language intent into SQL query parameters using OpenAI.

    Args:
        user_query: The input query from the user.
    """
    # Get API key from environment with fallback to dummy key for testing
    api_key = os.getenv("OPENAI_API_KEY")
    
    # Check if API key is valid (should start with 'sk-')
    if not api_key or not api_key.startswith("sk-"):
        print("Warning: Invalid or missing OpenAI API key. Using dummy key for testing.")
        # Use a dummy key for testing that has the correct format
        api_key = "sk-dummy1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefgh"
    
    try:
        # Set up OpenAI client with API key
        client = openai.OpenAI(api_key=api_key)
        
        # Define the system prompt for parsing financial queries
        system_prompt = """
        You are a financial data expert specialized in WRDS (Wharton Research Data Services) database.
        Your task is to parse natural language queries about financial data and extract key parameters.
        
        For each query, identify and extract the following information in JSON format:
        1. Tables: Which WRDS tables are needed (e.g., crsp.dsf, comp.funda)
        2. Tickers/Companies: Company identifiers mentioned (e.g., AAPL, MSFT)
        3. Date Range: Start and end dates for the data
        4. Metrics: Financial metrics requested (e.g., returns, price, PE ratio)
        5. Filters: Any additional filtering conditions
        6. Grouping: How data should be grouped (e.g., by company, by year)
        7. Sorting: How data should be sorted
        8. Limit: Maximum number of records to return
        
        Return ONLY a valid JSON object with these fields.
        """
        
        # Call OpenAI API to parse the query
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            response_format={"type": "json_object"}
        )
        
        # Extract and parse the JSON response
        parsed_json = json.loads(response.choices[0].message.content)
        return parsed_json
    except openai.APIConnectionError as e:
        print(f"OpenAI API connection error: {str(e)}")
        # Return a basic structure if connection fails
        return create_fallback_intent(user_query)
    except openai.APIError as e:
        print(f"OpenAI API error: {str(e)}")
        return create_fallback_intent(user_query)
    except Exception as e:
        print(f"Error parsing user intent: {str(e)}")
        return create_fallback_intent(user_query)

def create_fallback_intent(user_query: str) -> Dict[str, Any]:
    """Create a fallback intent structure based on basic parsing of the query."""
    # Basic fallback structure
    intent = {
        "tables": [],
        "tickers": [],
        "date_range": {"start": None, "end": None},
        "metrics": [],
        "filters": [],
        "grouping": [],
        "sorting": [],
        "limit": 100
    }
    
    # Try to extract some basic information from the query
    query_lower = user_query.lower()
    
    # Check for common tickers
    common_tickers = ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA"]
    for ticker in common_tickers:
        if ticker.lower() in query_lower:
            intent["tickers"].append(ticker)
    
    # Check for table types
    if any(word in query_lower for word in ["price", "stock", "daily", "return"]):
        intent["tables"].append("crsp.dsf")
    elif any(word in query_lower for word in ["fundamental", "balance", "sheet", "income", "statement"]):
        intent["tables"].append("comp.funda")
    elif any(word in query_lower for word in ["analyst", "estimate", "forecast", "eps"]):
        intent["tables"].append("ibes.statsum")
    else:
        # Default to stock prices
        intent["tables"].append("crsp.dsf")
    
    # Set default metrics based on table
    if "crsp.dsf" in intent["tables"]:
        intent["metrics"] = ["date", "ticker", "prc", "ret"]
    elif "comp.funda" in intent["tables"]:
        intent["metrics"] = ["fyear", "ticker", "at", "lt", "sale", "ni"]
    elif "ibes" in intent["tables"][0]:
        intent["metrics"] = ["ticker", "fpedats", "meanest", "medest", "numest"]
    
    return intent

def construct_sql_query(parsed_intent: Dict[str, Any]) -> str:
    """
    Construct a SQL query based on parsed intent.

    Args:
        parsed_intent: Dictionary containing SQL fields and conditions.
    """
    # Extract components from parsed intent
    tables = parsed_intent.get("tables", [])
    tickers = parsed_intent.get("tickers", [])
    date_range = parsed_intent.get("date_range", {"start": None, "end": None})
    metrics = parsed_intent.get("metrics", [])
    filters = parsed_intent.get("filters", [])
    grouping = parsed_intent.get("grouping", [])
    sorting = parsed_intent.get("sorting", [])
    limit = parsed_intent.get("limit", 100)
    
    # Handle empty tables
    if not tables:
        # Default to CRSP daily stock file if no tables specified
        tables = ["crsp.dsf"]
    
    # Handle empty metrics
    if not metrics:
        # Default to basic stock metrics if none specified
        metrics = ["date", "ticker", "prc", "ret"]
    
    # Construct SELECT clause
    select_clause = "SELECT " + ", ".join(metrics)
    
    # Construct FROM clause
    from_clause = "FROM " + tables[0]
    
    # Add any JOINs for multiple tables
    # This is a simplified implementation - real joins would need more logic
    for i in range(1, len(tables)):
        from_clause += f" JOIN {tables[i]} USING (permno)"
    
    # Construct WHERE clause
    where_conditions = []
    
    # Add ticker filter if present
    if tickers:
        ticker_list = "', '".join(tickers)
        where_conditions.append(f"ticker IN ('{ticker_list}')")
    
    # Add date range if present
    if date_range.get("start"):
        where_conditions.append(f"date >= '{date_range['start']}'")
    if date_range.get("end"):
        where_conditions.append(f"date <= '{date_range['end']}'")
    
    # Add any additional filters
    where_conditions.extend(filters)
    
    # Combine WHERE conditions
    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)
    
    # Construct GROUP BY clause
    group_by_clause = ""
    if grouping:
        group_by_clause = "GROUP BY " + ", ".join(grouping)
    
    # Construct ORDER BY clause
    order_by_clause = ""
    if sorting:
        order_by_clause = "ORDER BY " + ", ".join(sorting)
    else:
        # Default sorting by date if it's in the metrics
        if "date" in metrics:
            order_by_clause = "ORDER BY date"
    
    # Construct LIMIT clause
    limit_clause = f"LIMIT {limit}"
    
    # Combine all clauses
    sql_parts = [select_clause, from_clause]
    if where_clause:
        sql_parts.append(where_clause)
    if group_by_clause:
        sql_parts.append(group_by_clause)
    if order_by_clause:
        sql_parts.append(order_by_clause)
    sql_parts.append(limit_clause)
    
    # Join with newlines for readability
    sql_query = "\n".join(sql_parts)
    
    return sql_query

def save_to_sqlite(data_type: str, df: pd.DataFrame, query_id: int) -> None:
    """Save data to SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    
    if data_type == 'stock_prices':
        # Save to stock_prices table
        for _, row in df.iterrows():
            conn.execute(
                "INSERT INTO stock_prices (date, ticker, prc, ret, query_id) VALUES (?, ?, ?, ?, ?)",
                (row.get('date'), row.get('ticker'), row.get('prc'), row.get('ret'), query_id)
            )
    elif data_type == 'fundamentals':
        # Save to fundamentals table
        for _, row in df.iterrows():
            conn.execute(
                "INSERT INTO fundamentals (fyear, ticker, at, lt, sale, ni, query_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (row.get('fyear'), row.get('ticker'), row.get('at'), row.get('lt'), 
                 row.get('sale'), row.get('ni'), query_id)
            )
    elif data_type == 'analyst_estimates':
        # Save to analyst_estimates table
        for _, row in df.iterrows():
            conn.execute(
                "INSERT INTO analyst_estimates (ticker, fpedats, meanest, medest, numest, query_id) VALUES (?, ?, ?, ?, ?, ?)",
                (row.get('ticker'), row.get('fpedats'), row.get('meanest'), 
                 row.get('medest'), row.get('numest'), query_id)
            )
    
    conn.commit()
    conn.close()

async def execute_sql_query(ctx: RunContext, sql_query: str) -> Dict[str, Any]:
    """
    Connect to WRDS and execute the given SQL query.

    Args:
        ctx: RunContext containing the dependencies.
        sql_query: The SQL query to be executed.
    """
    # For demonstration purposes, we'll use mock data instead of connecting to the actual database
    print(f"Executing SQL query (MOCK): {sql_query}")
    
    # Extract ticker from query if present
    ticker = "AAPL"  # Default ticker
    if "ticker IN" in sql_query or "ticker =" in sql_query:
        # Try to extract ticker from the query
        try:
            if "ticker IN" in sql_query:
                start_idx = sql_query.find("ticker IN ('")
                if start_idx != -1:
                    start_idx += len("ticker IN ('")
                    end_idx = sql_query.find("')", start_idx)
                    ticker = sql_query[start_idx:end_idx]
            elif "ticker =" in sql_query:
                start_idx = sql_query.find("ticker = '")
                if start_idx != -1:
                    start_idx += len("ticker = '")
                    end_idx = sql_query.find("'", start_idx)
                    ticker = sql_query[start_idx:end_idx]
        except:
            pass
    
    # Create mock data based on the query
    mock_data = []
    data_type = 'generic'
    
    # Stock price data for different tickers
    if "crsp.dsf" in sql_query or "price" in sql_query.lower() or "prc" in sql_query:
        data_type = 'stock_prices'
        if ticker == "AAPL":
            mock_data = [
                {"date": "2022-01-03", "ticker": "AAPL", "prc": 182.01, "ret": 0.0297},
                {"date": "2022-01-04", "ticker": "AAPL", "prc": 179.70, "ret": -0.0127},
                {"date": "2022-01-05", "ticker": "AAPL", "prc": 174.92, "ret": -0.0266},
                {"date": "2022-01-06", "ticker": "AAPL", "prc": 172.19, "ret": -0.0156},
                {"date": "2022-01-07", "ticker": "AAPL", "prc": 172.17, "ret": -0.0001},
            ]
        elif ticker == "MSFT":
            mock_data = [
                {"date": "2022-01-03", "ticker": "MSFT", "prc": 334.75, "ret": 0.0199},
                {"date": "2022-01-04", "ticker": "MSFT", "prc": 329.01, "ret": -0.0171},
                {"date": "2022-01-05", "ticker": "MSFT", "prc": 316.38, "ret": -0.0384},
                {"date": "2022-01-06", "ticker": "MSFT", "prc": 313.88, "ret": -0.0079},
                {"date": "2022-01-07", "ticker": "MSFT", "prc": 314.04, "ret": 0.0005},
            ]
        else:
            mock_data = [
                {"date": "2022-01-03", "ticker": ticker, "prc": 100.00, "ret": 0.01},
                {"date": "2022-01-04", "ticker": ticker, "prc": 101.00, "ret": 0.01},
                {"date": "2022-01-05", "ticker": ticker, "prc": 102.01, "ret": 0.01},
                {"date": "2022-01-06", "ticker": ticker, "prc": 103.03, "ret": 0.01},
                {"date": "2022-01-07", "ticker": ticker, "prc": 104.06, "ret": 0.01},
            ]
    
    # Fundamentals data
    elif "comp.funda" in sql_query or "fundamentals" in sql_query.lower():
        data_type = 'fundamentals'
        if ticker == "AAPL":
            mock_data = [
                {"fyear": 2021, "ticker": "AAPL", "at": 351002, "lt": 287912, "sale": 365817, "ni": 94680},
                {"fyear": 2020, "ticker": "AAPL", "at": 323888, "lt": 258549, "sale": 274515, "ni": 57411},
                {"fyear": 2019, "ticker": "AAPL", "at": 338516, "lt": 248028, "sale": 260174, "ni": 55256},
            ]
        elif ticker == "MSFT":
            mock_data = [
                {"fyear": 2021, "ticker": "MSFT", "at": 333779, "lt": 130042, "sale": 168088, "ni": 61271},
                {"fyear": 2020, "ticker": "MSFT", "at": 301311, "lt": 118943, "sale": 143015, "ni": 44281},
                {"fyear": 2019, "ticker": "MSFT", "at": 286556, "lt": 106856, "sale": 125843, "ni": 39240},
            ]
        else:
            mock_data = [
                {"fyear": 2021, "ticker": ticker, "at": 100000, "lt": 50000, "sale": 75000, "ni": 25000},
                {"fyear": 2020, "ticker": ticker, "at": 90000, "lt": 45000, "sale": 70000, "ni": 22000},
                {"fyear": 2019, "ticker": ticker, "at": 80000, "lt": 40000, "sale": 65000, "ni": 20000},
            ]
    
    # Analyst estimates
    elif "ibes" in sql_query or "analyst" in sql_query.lower() or "estimates" in sql_query.lower():
        data_type = 'analyst_estimates'
        if ticker == "AAPL":
            mock_data = [
                {"ticker": "AAPL", "fpedats": "2022-03-31", "meanest": 1.43, "medest": 1.45, "numest": 28},
                {"ticker": "AAPL", "fpedats": "2022-06-30", "meanest": 1.32, "medest": 1.33, "numest": 30},
                {"ticker": "AAPL", "fpedats": "2022-09-30", "meanest": 1.27, "medest": 1.26, "numest": 25},
            ]
        elif ticker == "MSFT":
            mock_data = [
                {"ticker": "MSFT", "fpedats": "2022-03-31", "meanest": 2.19, "medest": 2.20, "numest": 32},
                {"ticker": "MSFT", "fpedats": "2022-06-30", "meanest": 2.28, "medest": 2.30, "numest": 30},
                {"ticker": "MSFT", "fpedats": "2022-09-30", "meanest": 2.35, "medest": 2.36, "numest": 28},
            ]
        else:
            mock_data = [
                {"ticker": ticker, "fpedats": "2022-03-31", "meanest": 1.00, "medest": 1.00, "numest": 10},
                {"ticker": ticker, "fpedats": "2022-06-30", "meanest": 1.10, "medest": 1.10, "numest": 10},
                {"ticker": ticker, "fpedats": "2022-09-30", "meanest": 1.20, "medest": 1.20, "numest": 10},
            ]
    
    # Default data if no specific data type is detected
    else:
        data_type = 'generic'
        mock_data = [
            {"date": "2022-01-03", "ticker": ticker, "value": 100},
            {"date": "2022-01-04", "ticker": ticker, "value": 101},
            {"date": "2022-01-05", "ticker": ticker, "value": 102},
        ]
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(mock_data)
    
    # Generate a unique filename based on timestamp and ticker
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{data_type}_{ticker}_{timestamp}.csv"
    filepath = os.path.join(DATA_DIR, filename)
    
    # Save to CSV file
    df.to_csv(filepath, index=False)
    print(f"Data saved to {filepath}")
    
    # Save to SQLite database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Record the query in the queries table
    cursor.execute(
        "INSERT INTO queries (query_text, query_date, result_file, table_name) VALUES (?, ?, ?, ?)",
        (sql_query, datetime.now().isoformat(), filename, data_type)
    )
    query_id = cursor.lastrowid
    conn.commit()
    conn.close()
    
    # Save the data to the appropriate table
    save_to_sqlite(data_type, df, query_id)
    
    # Get column names
    columns = df.columns.tolist()
    
    # Return results as dictionary, without the DataFrame to avoid serialization issues
    return {
        "query": sql_query,
        "row_count": len(df),
        "columns": columns,
        "data": mock_data,
        "file_path": filepath,
        "database": DB_FILE,
        "query_id": query_id
    }