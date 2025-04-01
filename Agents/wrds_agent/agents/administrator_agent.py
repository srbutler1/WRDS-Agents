"""AdministratorAgent for WRDS Multi-Agent System.

This agent is responsible for coordinating the other agents in the system.
"""

import os
import logging
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

from .base_agent import BaseAgent
from .documentation_agent import DocumentationAgent
from .sql_agent import SQLAgent
from ..utils.openai_utils import get_completion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdministratorAgent(BaseAgent):
    """Agent responsible for coordinating the other agents in the system."""

    def __init__(self, docs_dir: Optional[str] = None):
        """Initialize the AdministratorAgent.
        
        Args:
            docs_dir: Directory containing WRDS documentation files
        """
        super().__init__("AdministratorAgent")
        self.documentation_agent = DocumentationAgent(docs_dir)
        self.sql_agent = SQLAgent(self.documentation_agent)
        logger.info("AdministratorAgent initialized with DocumentationAgent and SQLAgent")
    
    def connect_to_wrds(self, username: str, password: str) -> bool:
        """Connect to the WRDS database.
        
        Args:
            username: WRDS username
            password: WRDS password
            
        Returns:
            True if connection is successful, False otherwise
        """
        return self.sql_agent.connect_to_wrds(username, password)
    
    def process_query(self, query: str) -> Tuple[pd.DataFrame, str, str]:
        """Process a natural language query and return the results.
        
        Args:
            query: Natural language query
            
        Returns:
            Tuple of (results DataFrame, SQL query, explanation)
        """
        logger.info(f"Processing query: {query}")
        
        # Identify relevant tables for the query
        relevant_tables = self._identify_relevant_tables(query)
        logger.info(f"Identified relevant tables: {', '.join(relevant_tables)}")
        
        # Generate SQL query
        sql_query, explanation = self.sql_agent.generate_sql(query)
        logger.info(f"Generated SQL query: {sql_query}")
        
        # Execute SQL query
        results = self.sql_agent.execute_sql(sql_query)
        
        return results, sql_query, explanation
    
    def _identify_relevant_tables(self, query: str) -> List[str]:
        """Identify tables that are relevant to the query.
        
        Args:
            query: Natural language query
            
        Returns:
            List of relevant table names
        """
        # Get all available tables
        all_tables = self.documentation_agent.get_all_tables()
        
        # Prepare context for the LLM
        context = "Available tables and their descriptions:\n\n"
        for table in all_tables:
            table_info = self.documentation_agent.get_table_info(table)
            context += f"Table: {table}\n"
            context += f"Description: {table_info.get('description', '')}\n"
            context += f"Primary Keys: {', '.join(self.documentation_agent.get_primary_keys(table))}\n"
            context += f"Linking Info: {self.documentation_agent.get_linking_info(table)}\n\n"
        
        # Prepare the prompt for the LLM
        prompt = f"""
        You are an expert in financial databases, particularly WRDS (Wharton Research Data Services).
        Your task is to identify which tables are relevant to the following natural language query,
        using the available tables information provided below.
        
        Natural Language Query: {query}
        
        {context}
        
        Please list only the table names that are relevant to answering this query.
        Format your response as a comma-separated list of table names, e.g., "crsp.dsf, comp.funda".
        Do not include any other text in your response.
        """
        
        try:
            # Generate response using OpenAI API
            response = get_completion(prompt)
            
            # Parse the response to get table names
            table_names = [table.strip() for table in response.split(',')]
            
            # Validate table names
            valid_tables = [table for table in table_names if table in all_tables]
            
            return valid_tables
        except Exception as e:
            logger.error(f"Error identifying relevant tables: {e}")
            # Return all tables as a fallback
            return all_tables
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get the schema for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table schema information
        """
        return self.documentation_agent.get_table_info(table_name)
