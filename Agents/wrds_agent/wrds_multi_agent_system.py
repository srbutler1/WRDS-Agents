#!/usr/bin/env python
"""
WRDS Multi-Agent System for financial data retrieval.

This system uses multiple agents to process natural language queries,
generate SQL, and retrieve data from the WRDS database.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WRDSMultiAgentSystem:
    """Multi-agent system for WRDS data retrieval."""

    def __init__(self, docs_dir: Optional[str] = None):
        """Initialize the WRDS Multi-Agent System.
        
        Args:
            docs_dir: Directory containing WRDS documentation files
        """
        # Load environment variables
        load_dotenv()
        
        # Add the parent directory to sys.path to allow imports
        base_dir = Path(__file__).parent.parent
        sys.path.insert(0, str(base_dir))
        
        # Import the AdministratorAgent
        from Agents.wrds_agent.agents.administrator_agent import AdministratorAgent
        
        # Initialize the AdministratorAgent
        self.admin_agent = AdministratorAgent(docs_dir)
        logger.info("WRDS Multi-Agent System initialized")
    
    def connect_to_wrds(self, username: Optional[str] = None, password: Optional[str] = None) -> bool:
        """Connect to the WRDS database.
        
        Args:
            username: WRDS username (if not provided, will use environment variable)
            password: WRDS password (if not provided, will use environment variable)
            
        Returns:
            True if connection is successful, False otherwise
        """
        # Get credentials from environment variables if not provided
        if not username:
            username = os.environ.get("WRDS_USERNAME")
        if not password:
            password = os.environ.get("WRDS_PASSWORD")
        
        if not username or not password:
            logger.error("WRDS credentials not found")
            return False
        
        return self.admin_agent.connect_to_wrds(username, password)
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """Process a natural language query and return the results.
        
        Args:
            query: Natural language query
            
        Returns:
            Dictionary containing results, SQL query, and explanation
        """
        try:
            results, sql_query, explanation = self.admin_agent.process_query(query)
            
            return {
                "success": True,
                "results": results,
                "sql_query": sql_query,
                "explanation": explanation,
                "row_count": len(results) if not results.empty else 0
            }
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                "success": False,
                "error": str(e)
            }

def main():
    """Main function to run the WRDS Multi-Agent System."""
    # Get the documentation directory
    base_dir = Path(__file__).parent.parent
    docs_dir = base_dir / "WRDS Documentation"
    
    if not docs_dir.exists():
        logger.error(f"Documentation directory not found: {docs_dir}")
        logger.info("Using default schema information")
        docs_dir = None
    
    # Initialize the WRDS Multi-Agent System
    system = WRDSMultiAgentSystem(str(docs_dir) if docs_dir else None)
    
    # Connect to WRDS database
    connected = system.connect_to_wrds()
    
    if not connected:
        logger.warning("Failed to connect to WRDS database")
        logger.info("Continuing with limited functionality (SQL generation only)")
    
    # Example query
    query = "Get daily stock returns for Apple (ticker: AAPL) for the year 2022"
    logger.info(f"Processing query: {query}")
    
    result = system.process_query(query)
    
    if result["success"]:
        logger.info(f"SQL Query: {result['sql_query']}")
        logger.info(f"Explanation: {result['explanation']}")
        
        if result["row_count"] > 0:
            logger.info(f"Results (first 5 rows):\n{result['results'].head()}")
            logger.info(f"Total rows: {result['row_count']}")
        else:
            logger.info("No results returned")
    else:
        logger.error(f"Error: {result['error']}")

if __name__ == "__main__":
    main()
