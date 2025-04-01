from __future__ import annotations
import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union

from .base_agent import BaseAgent, Message

class DocumentationAgent(BaseAgent):
    """Agent responsible for providing documentation and schema information about WRDS tables."""
    
    def __init__(self, docs_dir: str = None, schema_json_path: str = None):
        """Initialize the DocumentationAgent.
        
        Args:
            docs_dir: Path to the directory containing WRDS documentation files
            schema_json_path: Path to the JSON file containing schema information
        """
        super().__init__("documentation_agent")
        
        # Default paths
        if docs_dir is None:
            docs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "WRDS Documentation")
        
        if schema_json_path is None:
            schema_json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "wrds_schema.json")
        
        self.docs_dir = docs_dir
        self.schema_json_path = schema_json_path
        self.schema = self._load_schema()
        self.logger.info(f"DocumentationAgent initialized with {len(self.schema)} tables")
    
    def process_message(self, message: Message):
        """Process a message from another agent.
        
        Args:
            message: The message to process
        """
        self.logger.info(f"Processing message: {message}")
        
        if message.message_type == "request":
            content = message.content
            
            if "query" in content:
                # Identify relevant tables based on the query
                query = content["query"]
                relevant_tables = self.identify_relevant_tables(query)
                
                # Get detailed information about the relevant tables
                tables_info = {}
                for table_name in relevant_tables:
                    table_info = self.get_table_info(table_name)
                    if table_info:
                        tables_info[table_name] = table_info
                
                # Send response back to the sender or a specified callback
                callback = content.get("callback", message.sender)
                
                response_content = {
                    "tables_info": tables_info,
                    "query": query,
                    "relevant_tables": relevant_tables
                }
                
                self.send_message(callback, response_content, "response")
            
            elif "table_name" in content:
                # Get information about a specific table
                table_name = content["table_name"]
                table_info = self.get_table_info(table_name)
                
                # Send response back to the sender or a specified callback
                callback = content.get("callback", message.sender)
                
                response_content = {
                    "table_info": table_info,
                    "table_name": table_name
                }
                
                self.send_message(callback, response_content, "response")
    
    def identify_relevant_tables(self, query: str) -> List[str]:
        """Identify relevant tables based on a natural language query.
        
        Args:
            query: Natural language query
            
        Returns:
            List of relevant table names
        """
        self.logger.info(f"Identifying relevant tables for query: {query}")
        
        # Use OpenAI to identify relevant tables
        from wrds_agent.utils.openai_utils import get_completion
        
        # Get all available tables
        available_tables = list(self.schema.keys())
        
        # Create a prompt to identify relevant tables
        prompt = f"""
        You are an expert in financial data and the WRDS (Wharton Research Data Services) database.
        Your task is to identify the most relevant tables for the following query:
        
        Query: {query}
        
        Available tables:
        {', '.join(available_tables)}
        
        For each available table, I will provide a brief description:
        """
        
        # Add table descriptions to the prompt
        for table_name in available_tables:
            table_info = self.get_table_info(table_name)
            if table_info and 'description' in table_info:
                prompt += f"\n- {table_name}: {table_info['description']}"
        
        prompt += "\n\nPlease list the names of the most relevant tables for this query, separated by commas. Only include tables that are directly relevant to answering the query."
        
        # Get the response from OpenAI
        response = get_completion(prompt)
        
        # Parse the response to get the relevant tables
        relevant_tables = [table.strip() for table in response.split(',')]
        
        # Filter out any tables that are not in the available tables
        relevant_tables = [table for table in relevant_tables if table in available_tables]
        
        self.logger.info(f"Identified relevant tables: {relevant_tables}")
        
        return relevant_tables
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table information
        """
        if table_name in self.schema:
            return self.schema[table_name]
        else:
            self.logger.warning(f"Table {table_name} not found in schema")
            return {}
    
    def get_tables_info(self, tables: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get information about specified tables.
        
        Args:
            tables: List of table names
            
        Returns:
            Dictionary mapping table names to their information
        """
        tables_info = {}
        
        for table in tables:
            if table in self.schema:
                tables_info[table] = self.schema[table]
            else:
                self.logger.warning(f"Table {table} not found in schema")
        
        return tables_info
    
    def _load_schema(self) -> Dict[str, Dict[str, Any]]:
        """Load schema information from JSON file or extract from documentation files.
        
        Returns:
            Dictionary containing schema information
        """
        schema = {}
        
        # Try to load schema from JSON file
        if os.path.exists(self.schema_json_path):
            try:
                with open(self.schema_json_path, 'r', encoding='utf-8') as f:
                    schema = json.load(f)
                self.logger.info(f"Loaded schema from JSON file: {self.schema_json_path}")
                return schema
            except Exception as e:
                self.logger.error(f"Error loading schema from JSON file: {e}")
        
        # If JSON file doesn't exist or loading failed, use hardcoded schema
        # This is a simplified version for demonstration purposes
        schema = {
            "crsp.dsf": {
                "description": "CRSP Daily Stock File",
                "fields": {
                    "permno": "CRSP Permanent Number",
                    "date": "Trading Date",
                    "ret": "Return"
                },
                "primary_keys": ["permno", "date"],
                "linking_info": {
                    "crsp.dsenames": {"from": "permno", "to": "permno"}
                }
            },
            "crsp.dsenames": {
                "description": "CRSP Daily Stock Events Names",
                "fields": {
                    "permno": "CRSP Permanent Number",
                    "ticker": "Ticker Symbol",
                    "namedt": "Name Start Date",
                    "nameendt": "Name End Date"
                },
                "primary_keys": ["permno", "namedt"],
                "linking_info": {
                    "crsp.dsf": {"from": "permno", "to": "permno"}
                }
            },
            "crsp.msf": {
                "description": "CRSP Monthly Stock File",
                "fields": {
                    "permno": "CRSP Permanent Number",
                    "date": "Trading Date",
                    "ret": "Return"
                },
                "primary_keys": ["permno", "date"],
                "linking_info": {
                    "crsp.msenames": {"from": "permno", "to": "permno"}
                }
            },
            "crsp.msenames": {
                "description": "CRSP Monthly Stock Events Names",
                "fields": {
                    "permno": "CRSP Permanent Number",
                    "ticker": "Ticker Symbol",
                    "namedt": "Name Start Date",
                    "nameendt": "Name End Date"
                },
                "primary_keys": ["permno", "namedt"],
                "linking_info": {
                    "crsp.msf": {"from": "permno", "to": "permno"}
                }
            },
            "comp.fundq": {
                "description": "Compustat Quarterly Fundamentals",
                "fields": {
                    "gvkey": "Global Company Key",
                    "datadate": "Data Date",
                    "fyearq": "Fiscal Year of Quarter",
                    "fqtr": "Fiscal Quarter",
                    "tic": "Ticker Symbol",
                    "niq": "Net Income (Quarterly)"
                },
                "primary_keys": ["gvkey", "datadate"],
                "linking_info": {}
            },
            "comp.funda": {
                "description": "Compustat Annual Fundamentals",
                "fields": {
                    "gvkey": "Global Company Key",
                    "datadate": "Data Date",
                    "fyear": "Fiscal Year",
                    "tic": "Ticker Symbol",
                    "ni": "Net Income (Annual)"
                },
                "primary_keys": ["gvkey", "datadate"],
                "linking_info": {}
            },
            "comp.company": {
                "description": "Compustat Company Information",
                "fields": {
                    "gvkey": "Global Company Key",
                    "conm": "Company Name",
                    "tic": "Ticker Symbol",
                    "sic": "Standard Industry Classification Code"
                },
                "primary_keys": ["gvkey"],
                "linking_info": {
                    "comp.fundq": {"from": "gvkey", "to": "gvkey"},
                    "comp.funda": {"from": "gvkey", "to": "gvkey"}
                }
            }
        }
        
        self.logger.info("Using hardcoded schema information")
        return schema
