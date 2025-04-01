#!/usr/bin/env python
"""
Simple script to query the autonomous WRDS system.

This script provides a command-line interface for sending natural language queries
to the autonomous WRDS agent system.
"""

import sys
import argparse
import pandas as pd
from pathlib import Path
import os

from autonomous_wrds_system import AutonomousWRDSSystem

def main():
    """Main function to run the WRDS query script."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Query the WRDS database using natural language")
    parser.add_argument("query", nargs="*", help="Natural language query for the WRDS database")
    parser.add_argument("--output", "-o", help="Output file path (CSV format)")
    parser.add_argument("--username", "-u", help="WRDS username (optional, will use environment variable if not provided)")
    parser.add_argument("--password", "-p", help="WRDS password (optional, will use environment variable if not provided)")
    
    args = parser.parse_args()
    
    # Ensure data directory exists
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(data_dir, exist_ok=True)
    
    # Get the documentation directory
    base_dir = Path(__file__).parent.parent
    docs_dir = base_dir / "WRDS Documentation"
    
    if not docs_dir.exists():
        print(f"Documentation directory not found: {docs_dir}")
        print("Using default schema information")
        docs_dir = None
    
    # Initialize the Autonomous WRDS System
    system = AutonomousWRDSSystem(str(docs_dir) if docs_dir else None)
    
    # Connect to WRDS database
    connected = system.connect_to_wrds(args.username, args.password)
    
    if not connected:
        print("Warning: Failed to connect to WRDS database")
        print("Continuing with limited functionality (SQL generation only)")
    
    # Get the query from command-line arguments or prompt the user
    if args.query:
        query = " ".join(args.query)
    else:
        print("Enter your natural language query for the WRDS database:")
        query = input("> ")
    
    print(f"\nProcessing query: {query}")
    print("This may take a moment...\n")
    
    # Process the query
    result = system.process_query(query)
    
    if result["success"]:
        print(f"SQL Query:\n{result['sql_query']}\n")
        print(f"Explanation:\n{result['explanation']}\n")
        
        if result["row_count"] > 0:
            # Convert results back to DataFrame if needed
            if isinstance(result["results"], dict):
                results_df = pd.DataFrame(result["results"])
            else:
                results_df = result["results"]
                
            print(f"Results (first 5 rows):\n{results_df.head()}\n")
            print(f"Total rows: {result['row_count']}")
            
            # Display CSV file path if available
            if "csv_path" in result and result["csv_path"]:
                print(f"\nResults saved to CSV: {result['csv_path']}")
            
            # Save results to file if specified
            if args.output:
                results_df.to_csv(args.output, index=False)
                print(f"Results also saved to: {args.output}")
        else:
            print("No results returned")
    else:
        print(f"Error: {result['error']}")

if __name__ == "__main__":
    main()
