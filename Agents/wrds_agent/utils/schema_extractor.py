"""
Utility to extract schema information from WRDS documentation files.
This module parses HTML and other documentation files to extract table and field information.
"""

import os
import re
import json
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from pathlib import Path

class SchemaExtractor:
    """Extract schema information from WRDS documentation files."""
    
    def __init__(self, docs_dir: str):
        """Initialize the SchemaExtractor.
        
        Args:
            docs_dir: Directory containing WRDS documentation files
        """
        self.docs_dir = Path(docs_dir)
        self.knowledge_base = {}
        
        # Define known tables and their descriptions
        self.known_tables = {
            "crsp.dsf": {
                "description": "CRSP Daily Stock File - contains daily price and volume data for stocks",
                "fields": {},
                "primary_keys": ["permno", "date"],
                "linking_info": "Use permno to link with other CRSP tables. Use cusip to link with other databases."
            },
            "crsp.msf": {
                "description": "CRSP Monthly Stock File - contains monthly price and volume data for stocks",
                "fields": {},
                "primary_keys": ["permno", "date"],
                "linking_info": "Use permno to link with other CRSP tables. Use cusip to link with other databases."
            },
            "crsp.dsenames": {
                "description": "CRSP Daily Stock Event Names - contains name history and identifier information",
                "fields": {},
                "primary_keys": ["permno", "namedt", "nameendt"],
                "linking_info": "Use permno to link with dsf table. namedt and nameendt define the effective date range."
            },
            "crsp.msenames": {
                "description": "CRSP Monthly Stock Event Names - contains name history and identifier information",
                "fields": {},
                "primary_keys": ["permno", "namedt", "nameendt"],
                "linking_info": "Use permno to link with msf table. namedt and nameendt define the effective date range."
            },
            "comp.funda": {
                "description": "Compustat Fundamentals Annual - contains annual financial statement data",
                "fields": {},
                "primary_keys": ["gvkey", "datadate"],
                "linking_info": "Use cusip to link with CRSP. Note that Compustat uses 9-digit CUSIPs while CRSP uses 8-digit CUSIPs."
            },
            "comp.fundq": {
                "description": "Compustat Fundamentals Quarterly - contains quarterly financial statement data",
                "fields": {},
                "primary_keys": ["gvkey", "datadate", "fyearq", "fqtr"],
                "linking_info": "Use cusip to link with CRSP. Note that Compustat uses 9-digit CUSIPs while CRSP uses 8-digit CUSIPs."
            },
            "ibes.statsum": {
                "description": "IBES Summary Statistics - contains analyst estimates and forecasts",
                "fields": {},
                "primary_keys": ["ticker", "fpedats", "statpers", "measure"],
                "linking_info": "Use cusip to link with CRSP and Compustat."
            }
        }
    
    def extract_from_html(self, html_file: str) -> Dict[str, Any]:
        """Extract schema information from an HTML file.
        
        Args:
            html_file: Path to the HTML file
            
        Returns:
            Dictionary containing extracted schema information
        """
        file_path = self.docs_dir / html_file
        if not file_path.exists():
            print(f"File not found: {file_path}")
            return {}
        
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract field information from code blocks
        code_blocks = soup.find_all('code')
        for block in code_blocks:
            field_name = block.get_text().strip().lower()
            # Check if this is a field reference
            if field_name and len(field_name) <= 15 and not ' ' in field_name:
                # Look for the description in the parent paragraph
                parent = block.parent
                if parent and parent.name == 'p':
                    paragraph_text = parent.get_text()
                    # Extract the description after the field name
                    field_pattern = re.escape(field_name) + r'\s*[-:]?\s*([^\.,]+)'
                    match = re.search(field_pattern, paragraph_text, re.IGNORECASE)
                    if match:
                        field_desc = match.group(1).strip()
                    else:
                        # Try to get the whole paragraph as description
                        field_desc = paragraph_text.replace(field_name, '').strip()
                    
                    # Add the field to all known tables that might use it
                    for table_name, table_info in self.known_tables.items():
                        if field_name not in table_info['fields']:
                            table_info['fields'][field_name] = field_desc
        
        # Extract table descriptions from paragraphs
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            p_text = p.get_text()
            # Look for table references like 'table dsf' or 'crsp.dsf'
            table_patterns = [
                r'table\s+([a-zA-Z0-9_]+)',
                r'([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)'
            ]
            
            for pattern in table_patterns:
                matches = re.findall(pattern, p_text, re.IGNORECASE)
                for match in matches:
                    table_name = match.lower()
                    # Check if this is a known table
                    for known_table in self.known_tables.keys():
                        if table_name in known_table:
                            # Extract a description from the paragraph
                            description = p_text.replace(match, '').strip()
                            if len(description) > 10:  # Only use meaningful descriptions
                                self.known_tables[known_table]['description'] = description
        
        return self.known_tables
    
    def extract_field_descriptions(self) -> None:
        """Extract field descriptions from documentation files and add them to the knowledge base."""
        # Define common CRSP fields and their descriptions
        common_fields = {
            "permno": "Permanent security identification number assigned by CRSP to each security",
            "permco": "Permanent company identification number assigned by CRSP to all companies",
            "date": "Trading date",
            "cusip": "CUSIP identifier (8-digit)",
            "ticker": "Ticker symbol",
            "prc": "Closing price or negative bid/ask average for non-traded stocks",
            "ret": "Holding period return",
            "vol": "Trading volume",
            "shrout": "Number of shares outstanding (in thousands)",
            "bid": "Closing bid price",
            "ask": "Closing ask price",
            "openprc": "Opening price",
            "bidlo": "Low bid price",
            "askhi": "High ask price",
            "numtrd": "Number of trades",
            "namedt": "Date when the name becomes effective",
            "nameendt": "Date when the name is no longer effective",
            "ncusip": "Historical CUSIP identifier",
            "comnam": "Company name",
            "shrcls": "Share class",
            "siccd": "Standard Industrial Classification code",
            "naics": "North American Industry Classification System code",
            "exchcd": "Exchange code",
            "shrcd": "Share code",
            "cfacpr": "Cumulative factor to adjust price",
            "cfacshr": "Cumulative factor to adjust shares outstanding"
        }
        
        # Define common Compustat fields and their descriptions
        compustat_fields = {
            "gvkey": "Global company key - Compustat identifier",
            "datadate": "Date of financial data",
            "fyear": "Fiscal year",
            "fyr": "Fiscal year-end month",
            "tic": "Ticker symbol",
            "conm": "Company name",
            "at": "Total assets",
            "lt": "Total liabilities",
            "sale": "Net sales/revenue",
            "ni": "Net income (loss)",
            "ceq": "Common/ordinary equity - total",
            "oibdp": "Operating income before depreciation",
            "dltt": "Long-term debt - total",
            "dlc": "Debt in current liabilities",
            "che": "Cash and short-term investments",
            "capx": "Capital expenditures",
            "xrd": "Research and development expense",
            "cogs": "Cost of goods sold",
            "xsga": "Selling, general and administrative expense",
            "fyearq": "Fiscal year of quarter",
            "fqtr": "Fiscal quarter"
        }
        
        # Define common IBES fields and their descriptions
        ibes_fields = {
            "ticker": "IBES ticker",
            "cusip": "CUSIP identifier",
            "fpedats": "Forecast period end date",
            "statpers": "Statistical period",
            "meanest": "Mean estimate",
            "medest": "Median estimate",
            "numest": "Number of estimates",
            "stdev": "Standard deviation of estimates",
            "highest": "Highest estimate",
            "lowest": "Lowest estimate",
            "actual": "Actual value",
            "anndats": "Announcement date",
            "measure": "Estimate measure (e.g., EPS)"
        }
        
        # Add common fields to the appropriate tables
        for table_name, table_info in self.known_tables.items():
            if 'crsp' in table_name:
                for field, desc in common_fields.items():
                    if field not in table_info['fields']:
                        table_info['fields'][field] = desc
            elif 'comp' in table_name:
                for field, desc in compustat_fields.items():
                    if field not in table_info['fields']:
                        table_info['fields'][field] = desc
            elif 'ibes' in table_name:
                for field, desc in ibes_fields.items():
                    if field not in table_info['fields']:
                        table_info['fields'][field] = desc
    
    def extract_all(self) -> Dict[str, Any]:
        """Extract schema information from all documentation files.
        
        Returns:
            Dictionary containing all extracted schema information
        """
        # Initialize the knowledge base with known tables
        self.knowledge_base = self.known_tables.copy()
        
        # Get all HTML files in the documentation directory
        html_files = [f for f in os.listdir(self.docs_dir) if f.endswith('.html')]
        
        for html_file in html_files:
            try:
                self.extract_from_html(html_file)
            except Exception as e:
                print(f"Error processing {html_file}: {e}")
        
        # Add field descriptions from our predefined lists
        self.extract_field_descriptions()
        
        return self.knowledge_base
    
    def save_to_json(self, output_file: str):
        """Save the extracted schema information to a JSON file.
        
        Args:
            output_file: Path to the output JSON file
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.knowledge_base, f, indent=4)


def extract_schema(docs_dir: str, output_file: str = None) -> Dict[str, Any]:
    """Extract schema information from WRDS documentation files.
    
    Args:
        docs_dir: Directory containing WRDS documentation files
        output_file: Optional path to save the extracted schema to a JSON file
        
    Returns:
        Dictionary containing extracted schema information
    """
    extractor = SchemaExtractor(docs_dir)
    knowledge_base = extractor.extract_all()
    
    if output_file:
        extractor.save_to_json(output_file)
    
    return knowledge_base
