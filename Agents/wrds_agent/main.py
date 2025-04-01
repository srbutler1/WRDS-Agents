from __future__ import annotations
import asyncio
import os
import json
from typing import Dict, Any, List, Optional
import pandas as pd
from dotenv import load_dotenv
import logfire

# Import agent modules
from agents.administrator_agent import AdministratorAgent
from agents.documentation_agent import DocumentationAgent
from agents.sql_agent import SQLAgent
from agents.validator_agent import ValidatorAgent

# Load environment variables
load_dotenv()

# Configure logger
logfire.configure()

class WRDSMultiAgentSystem:
    """WRDS Multi-Agent System that coordinates multiple specialized agents."""
    
    def __init__(self, docs_dir: Optional[str] = None):
        """Initialize the WRDS Multi-Agent System with all required agents.
        
        Args:
            docs_dir: Optional directory containing WRDS documentation files
        """
        # Initialize agents
        self.administrator_agent = AdministratorAgent(docs_dir=docs_dir)
        self.sql_agent = self.administrator_agent.sql_agent
        self.documentation_agent = self.administrator_agent.documentation_agent
        self.validator_agent = ValidatorAgent()
        
        # Set the validator agent if needed
        self.administrator_agent.validator_agent = self.validator_agent
    
    async def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a user query through the multi-agent system."""
        return await self.administrator_agent.process_query(user_query)

async def run_example_query(wrds_system: WRDSMultiAgentSystem, query: str):
    """Run an example query and display the results."""
    print("\n" + "=" * 80)
    print(f"QUERY: {query}")
    print("-" * 80)
    
    try:
        # Process the query
        result = await wrds_system.process_query(query)
        
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

async def run_interactive_mode(wrds_system: WRDSMultiAgentSystem):
    """Run the system in interactive mode, accepting user queries."""
    print("\n\nInteractive Mode")
    print("Type 'exit' or 'quit' to end the session\n")
    
    while True:
        # Get user query
        query = input("Enter your query: ")
        
        # Check if user wants to exit
        if query.lower() in ["exit", "quit"]:
            break
        
        try:
            # Process the query
            result = await wrds_system.process_query(query)
            
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
    """Main function to run the WRDS Multi-Agent System demo."""
    print("WRDS Multi-Agent System Demo")
    print("=============================")
    
    # Define the path to the documentation directory
    docs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "WRDS Documentation")
    if not os.path.exists(docs_dir):
        print(f"Warning: Documentation directory not found at {docs_dir}")
        print("Falling back to hardcoded schema information")
        docs_dir = None
    else:
        print(f"Using documentation from: {docs_dir}")
    
    # Initialize the WRDS Multi-Agent System
    wrds_system = WRDSMultiAgentSystem(docs_dir=docs_dir)
    
    # Example queries to demonstrate the system
    example_queries = [
        "Get daily stock prices for AAPL from 2022-01-01 to 2022-12-31",
        "Show me the fundamentals for MSFT for 2021",
        "What are the analyst estimates for AMZN in the last 6 months?",
        "Get the return on assets for tech companies in 2022",
        "Show me the debt to equity ratio for TSLA over the last 3 years"
    ]
    
    # Run example queries
    print("\nRunning Example Queries...\n")
    for query in example_queries:
        await run_example_query(wrds_system, query)
    
    # Run interactive mode
    await run_interactive_mode(wrds_system)

if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
