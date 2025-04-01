#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
WRDS Agent Demo Script

This script demonstrates the capabilities of the WRDS Semantic Agent by running
example queries and providing an interactive mode for exploring WRDS data.
"""

import asyncio
import os
import sys
import json
from agent import get_data_from_wrds, Deps
import logfire
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logfire
logfire.configure()

# Example queries to demonstrate the agent
EXAMPLE_QUERIES = [
    "Get daily stock prices for AAPL from 2022-01-01 to 2022-12-31",
    "Show me the fundamentals for MSFT for 2021",
    "What are the analyst estimates for AMZN in the last 6 months?",
    "Get the return on assets for tech companies in 2022",
    "Show me the debt to equity ratio for TSLA over the last 3 years"
]

async def run_example_query(query: str):
    """Run an example query and display the results."""
    print("\n" + "=" * 80)
    print(f"QUERY: {query}")
    print("-" * 80)
    
    # Create dependencies object
    deps = Deps(
        wrds_username=os.getenv("WRDS_USERNAME", ""),
        wrds_password=os.getenv("WRDS_PASSWORD", ""),
        wrds_host=os.getenv("WRDS_HOST", "wrds-pgdata.wharton.upenn.edu"),
        wrds_port=int(os.getenv("WRDS_PORT", "9737")),
        wrds_db=os.getenv("WRDS_DB", "wrds"),
        wrds_sslmode=os.getenv("WRDS_SSLMODE", "require")
    )
    
    try:
        # Format the user query
        user_prompt = f"User requests financial data: {query}. Please help retrieve and analyze this data from WRDS."
        
        # Run the agent
        result = await get_data_from_wrds(None, user_prompt)
        
        # Check if the query was successful
        if result.get("success", False):
            # Display data location
            if "data_location" in result:
                print(f"\nData saved to:")
                print(f"  - File: {result['data_location'].get('file')}")
                print(f"  - Database: {result['data_location'].get('database')}")
                print(f"  - Query ID: {result['data_location'].get('query_id')}")
            
            # Display data summary
            print(f"\nRetrieved {result.get('row_count', 0)} rows with columns: {', '.join(result.get('columns', []))}")
            
            # Display sample data
            if result.get('sample_data'):
                print("\nSample data:")
                for i, row in enumerate(result['sample_data']):
                    print(f"Row {i+1}: {json.dumps(row)}")
        else:
            print(f"\nError: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"Error: {str(e)}")

async def run_interactive_mode():
    """Run the agent in interactive mode, accepting user queries."""
    print("\n\nInteractive Mode")
    print("Type 'exit' or 'quit' to end the session\n")
    
    # Create dependencies object
    deps = Deps(
        wrds_username=os.getenv("WRDS_USERNAME", ""),
        wrds_password=os.getenv("WRDS_PASSWORD", ""),
        wrds_host=os.getenv("WRDS_HOST", "wrds-pgdata.wharton.upenn.edu"),
        wrds_port=int(os.getenv("WRDS_PORT", "9737")),
        wrds_db=os.getenv("WRDS_DB", "wrds"),
        wrds_sslmode=os.getenv("WRDS_SSLMODE", "require")
    )
    
    while True:
        # Get user query
        query = input("Enter your query: ")
        
        # Check if user wants to exit
        if query.lower() in ["exit", "quit"]:
            break
        
        try:
            # Format the user query
            user_prompt = f"User requests financial data: {query}. Please help retrieve and analyze this data from WRDS."
            
            # Run the agent
            result = await get_data_from_wrds(None, user_prompt)
            
            # Check if the query was successful
            if result.get("success", False):
                # Display data location
                if "data_location" in result:
                    print(f"\nData saved to:")
                    print(f"  - File: {result['data_location'].get('file')}")
                    print(f"  - Database: {result['data_location'].get('database')}")
                    print(f"  - Query ID: {result['data_location'].get('query_id')}")
                
                # Display data summary
                print(f"\nRetrieved {result.get('row_count', 0)} rows with columns: {', '.join(result.get('columns', []))}")
                
                # Display sample data
                if result.get('sample_data'):
                    print("\nSample data:")
                    for i, row in enumerate(result['sample_data']):
                        print(f"Row {i+1}: {json.dumps(row)}")
            else:
                print(f"\nError: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"Error: {str(e)}")

async def main():
    """Main function to run the demo."""
    print("WRDS Semantic Agent Demo")
    print("=======================")
    
    # Run example queries
    print("\nRunning Example Queries...\n")
    for query in EXAMPLE_QUERIES:
        await run_example_query(query)
    
    # Run interactive mode
    await run_interactive_mode()

if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
