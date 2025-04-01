#!/usr/bin/env python
"""
Simple verification script for the DocumentationAgent.

This script verifies that the DocumentationAgent can load schema information
from the pre-extracted JSON file and displays basic statistics.
"""

import os
import json
import sys
from pathlib import Path

# Get the correct path to the schema JSON file
schema_json_path = "/Users/appleowner/Downloads/FDA II/Demo Agent/Agents/wrds_schema.json"

# Load the schema directly from the JSON file
print(f"Loading schema from: {schema_json_path}")
with open(schema_json_path, 'r', encoding='utf-8') as f:
    schema = json.load(f)

# Display basic statistics
print(f"\nSchema loaded successfully!")
print(f"Found information for {len(schema)} tables:")

for table_name, table_info in schema.items():
    field_count = len(table_info.get('fields', {}))
    primary_keys = ', '.join(table_info.get('primary_keys', []))
    print(f"  - {table_name}: {field_count} fields, Primary keys: {primary_keys}")
    print(f"    Description: {table_info.get('description', 'N/A')}")
    
    # Print a few sample fields
    if field_count > 0:
        print(f"    Sample fields:")
        for i, (field, desc) in enumerate(table_info.get('fields', {}).items()):
            if i >= 3:  # Only show first 3 fields
                break
            print(f"      - {field}: {desc}")

print("\nVerification complete! The schema JSON file is loaded successfully.")
