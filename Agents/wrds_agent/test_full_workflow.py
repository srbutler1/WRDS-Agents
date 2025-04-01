#!/usr/bin/env python
"""
Test script for the full WRDS Multi-Agent System workflow.

This script demonstrates the complete process from user query to data retrieval,
showing how the DocumentationAgent, SQLAgent, and AdministratorAgent work together.
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fix import paths
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, parent_dir)

# Import the agents directly
from Agents.wrds_agent.agents.documentation_agent import DocumentationAgent
from Agents.wrds_agent.agents.sql_agent import SQLAgent
from Agents.wrds_agent.agents.administrator_agent import AdministratorAgent

def main():
    # Load environment variables
    load_dotenv()
    
    # Get WRDS credentials from environment variables
    wrds_username = os.environ.get("WRDS_USERNAME")
    wrds_password = os.environ.get("WRDS_PASSWORD")
    
    if not wrds_username or not wrds_password:
        logger.error("WRDS credentials not found in environment variables")
        logger.info("Please set WRDS_USERNAME and WRDS_PASSWORD in the .env file")
        return
    
    # Get the documentation directory
    docs_dir = os.path.join(parent_dir, "WRDS Documentation")
    
    if not os.path.exists(docs_dir):
        logger.error(f"Documentation directory not found: {docs_dir}")
        return
    
    logger.info(f"Initializing AdministratorAgent with documentation directory: {docs_dir}")
    
    # Initialize the AdministratorAgent
    admin_agent = AdministratorAgent(docs_dir)
    
    # Connect to WRDS database
    logger.info("Connecting to WRDS database...")
    connected = admin_agent.connect_to_wrds(wrds_username, wrds_password)
    
    if not connected:
        logger.error("Failed to connect to WRDS database")
        logger.info("Continuing with limited functionality (SQL generation only)")
    else:
        logger.info("Successfully connected to WRDS database")
    
    # Test queries
    test_queries = [
        "Get daily stock returns for Apple (ticker: AAPL) for the year 2022",
        "Find the average monthly returns for Microsoft (ticker: MSFT) in 2021",
        "What were the quarterly earnings for Amazon (ticker: AMZN) in 2020?"
    ]
    
    for i, query in enumerate(test_queries):
        logger.info(f"\n\nProcessing test query {i+1}: {query}")
        
        # Process the query
        results, sql_query, explanation = admin_agent.process_query(query)
        
        # Display results
        logger.info(f"\nGenerated SQL Query:\n{sql_query}")
        logger.info(f"\nExplanation:\n{explanation}")
        
        if not results.empty:
            logger.info(f"\nResults (first 5 rows):\n{results.head()}")
            logger.info(f"Total rows: {len(results)}")
        else:
            logger.info("No results returned or query execution failed")
            
            if i == 2:
                # Try with the correct column name
                corrected_sql = sql_query.replace('ni', 'niq')
                logger.info(f"\nTrying with corrected SQL query:\n{corrected_sql}")
                results = admin_agent.sql_agent.execute_sql(corrected_sql)
                
                if results is not None and not results.empty:
                    logger.info(f"\nResults with corrected query (first 5 rows):\n{results.head()}")
                    logger.info(f"Total rows: {len(results)}")

if __name__ == "__main__":
    main()
