#!/usr/bin/env python
"""
Test script for the schema extraction functionality.
This script extracts schema information from WRDS documentation files and prints the results.
"""

import os
import json
import sys
from pathlib import Path

# Add the parent directory to the path so we can import the utils module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wrds_agent.utils.schema_extractor import extract_schema

def main():
    # Get the path to the WRDS Documentation directory
    base_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    docs_dir = base_dir.parent / "WRDS Documentation"
    
    if not docs_dir.exists():
        print(f"Error: Documentation directory not found at {docs_dir}")
        return 1
    
    print(f"Extracting schema information from {docs_dir}")
    print("Files found:")
    for file in os.listdir(docs_dir):
        print(f"  - {file}")
    
    # Extract schema information
    try:
        schema = extract_schema(docs_dir)
        
        # Save the extracted schema to a JSON file
        output_file = base_dir / "wrds_schema.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=4)
        
        print(f"\nExtracted schema information saved to {output_file}")
        print(f"Found information for {len(schema)} tables:")
        for table_name in schema.keys():
            field_count = len(schema[table_name].get('fields', {}))
            print(f"  - {table_name}: {field_count} fields")
        
        return 0
    except Exception as e:
        print(f"Error extracting schema: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
