from __future__ import annotations
import os
import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Union
import wrds
import json

from .base_agent import BaseAgent, Message
from ..utils.openai_utils import get_completion

class SQLAgent(BaseAgent):
    """Agent responsible for generating and executing SQL queries."""
    
    def __init__(self, wrds_username: str = None, wrds_password: str = None):
        """Initialize the SQLAgent.
        
        Args:
            wrds_username: WRDS username
            wrds_password: WRDS password
        """
        super().__init__("sql_agent")
        
        # Get WRDS credentials from environment variables if not provided
        if wrds_username is None:
            wrds_username = os.environ.get("WRDS_USERNAME")
        if wrds_password is None:
            wrds_password = os.environ.get("WRDS_PASSWORD")
        
        self.wrds_username = wrds_username
        self.wrds_password = wrds_password
        self.db = None
        self.connected = False
        self.last_results = None
        self.last_sql_query = ""
        self.last_explanation = ""
        self.last_csv_path = ""
        
        # Connect to WRDS database if credentials are available
        if self.wrds_username and self.wrds_password:
            self.connect_to_db()
    
    def process_message(self, message: Message):
        """Process a message from another agent.
        
        Args:
            message: The message to process
        """
        self.logger.info(f"Processing message: {message}")
        
        if message.message_type == "request":
            content = message.content
            
            if "query" in content:
                # Generate and execute SQL based on the query
                query = content["query"]
                
                # Send a request to the documentation agent for table information
                self.send_message("documentation_agent", {"query": query}, "request")
                
            elif "tables_info" in content and "query" in content:
                # Generate and execute SQL based on the query and tables_info
                query = content["query"]
                tables_info = content["tables_info"]
                
                results, sql_query, explanation = self.generate_and_execute_sql(query, tables_info)
                
                # Store the results for later retrieval
                self.last_results = results
                self.last_sql_query = sql_query
                self.last_explanation = explanation
                
                # Save the results to CSV
                self.save_results_to_csv(results, query)
                
                # Send response back to the sender or a specified callback
                callback = content.get("callback", message.sender)
                
                response_content = {
                    "results": results.to_dict() if isinstance(results, pd.DataFrame) else results,
                    "sql_query": sql_query,
                    "explanation": explanation,
                    "original_query": query,
                    "csv_path": self.last_csv_path
                }
                
                self.send_message(callback, response_content, "response")
            
            elif "action" in content and content["action"] == "get_results":
                # Send the last results back to the sender or a specified callback
                callback = content.get("callback", message.sender)
                
                response_content = {
                    "results": self.last_results.to_dict() if isinstance(self.last_results, pd.DataFrame) else self.last_results,
                    "sql_query": self.last_sql_query,
                    "explanation": self.last_explanation
                }
                
                self.send_message(callback, response_content, "response")
        
        elif message.message_type == "response":
            content = message.content
            
            if "relevant_tables" in content and "tables_info" in content:
                # Got table information from the documentation agent
                query = content.get("query", "")
                tables_info = content["tables_info"]
                
                # Log the tables_info for debugging
                self.logger.info(f"Received tables_info: {type(tables_info)}")
                
                # Ensure tables_info is a dictionary
                if not isinstance(tables_info, dict):
                    try:
                        if isinstance(tables_info, str):
                            # Try to parse it as JSON if it's a string
                            import json
                            tables_info = json.loads(tables_info)
                        else:
                            self.logger.error(f"Unexpected tables_info type: {type(tables_info)}")
                            return
                    except Exception as e:
                        self.logger.error(f"Error parsing tables_info: {e}")
                        return
                
                # Generate and execute SQL based on the query and tables_info
                try:
                    results, sql_query, explanation = self.generate_and_execute_sql(query, tables_info)
                    
                    # Store the results for later retrieval
                    self.last_results = results
                    self.last_sql_query = sql_query
                    self.last_explanation = explanation
                    
                    # Save the results to CSV
                    self.save_results_to_csv(results, query)
                    
                    # Send response back to the original sender
                    response_content = {
                        "results": results.to_dict() if isinstance(results, pd.DataFrame) else results,
                        "sql_query": sql_query,
                        "explanation": explanation,
                        "original_query": query,
                        "csv_path": self.last_csv_path
                    }
                    
                    # Get the original sender from the message content
                    original_sender = content.get("original_sender", "")
                    if original_sender and original_sender in self.connections:
                        self.send_message(original_sender, response_content, "response")
                    else:
                        self.logger.warning(f"Cannot send response: original sender not found or not connected")
                except Exception as e:
                    self.logger.error(f"Error generating and executing SQL: {e}")
                    # Log the error details for debugging
                    self.logger.error(f"Query: {query}")
                    self.logger.error(f"Tables info type: {type(tables_info)}")
                    self.logger.error(f"Tables info: {tables_info}")
    
    def handle_request(self, message: Message) -> Optional[Dict[str, Any]]:
        """Handle a request message.
        
        Args:
            message: The request message
            
        Returns:
            Response content or None if no response is needed
        """
        request_type = message.content.get("request_type")
        
        if request_type == "generate_sql":
            query = message.content.get("query")
            tables_info = message.content.get("tables_info", {})
            results, sql_query, explanation = self.generate_sql(query, tables_info)
            return {
                "results": results,
                "sql_query": sql_query,
                "explanation": explanation
            }
        
        elif request_type == "execute_sql":
            sql_query = message.content.get("sql_query")
            results = self.execute_sql(sql_query)
            return {"results": results}
        
        return None
    
    def connect_to_db(self) -> bool:
        """Connect to the WRDS database.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.logger.info(f"Connecting to WRDS database with username: {self.wrds_username}")
            self.db = wrds.Connection(wrds_username=self.wrds_username, wrds_password=self.wrds_password)
            self.connected = True
            self.logger.info("Successfully connected to WRDS database")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to WRDS database: {e}")
            self.connected = False
            return False
    
    def generate_sql(self, query: str, tables_info: Dict[str, Dict[str, Any]] = None) -> Tuple[pd.DataFrame, str, str]:
        """Generate a SQL query based on a natural language query and table information.

        Args:
            query: The natural language query
            tables_info: Dictionary of table information

        Returns:
            Tuple of (results, sql_query, explanation)
        """
        if tables_info is None:
            tables_info = {}

        # Prepare the prompt for the OpenAI API
        prompt = f"""
        You are a SQL expert specializing in the WRDS (Wharton Research Data Services) database.
        Your task is to generate a SQL query based on the following natural language query:

        {query}

        Here is information about the tables that might be relevant:

        {json.dumps(tables_info, indent=2)}

        Important notes for WRDS database:
        1. If you need to join tables, make sure to use the appropriate join conditions
        2. If you need to filter by date, use the appropriate date format (YYYY-MM-DD)
        3. If you need to filter by company name or ticker, make sure to use the exact spelling
        4. For tables like crsp.dsenames and crsp.msenames, use the columns exactly as they appear in the schema
        5. The column for name end date is 'nameendt' (not 'nameenddt')
        
        Provide your response in the following format:
        ```sql
        <your SQL query here>
        ```

        Explanation:
        <your explanation of the query here>
        """
        
        response = get_completion(prompt)
        
        # Extract SQL query and explanation from response
        sql_query, explanation = self._extract_sql_and_explanation(response)
        
        # Execute the SQL query
        if sql_query and self.connected:
            results = self.execute_sql(sql_query)
            return results, sql_query, explanation
        else:
            return pd.DataFrame(), sql_query, explanation
    
    def generate_and_execute_sql(self, query: str, tables_info: Dict[str, Dict[str, Any]]) -> Tuple[pd.DataFrame, str, str]:
        """Generate and execute a SQL query based on a natural language query.
        
        Args:
            query: Natural language query
            tables_info: Dictionary mapping table names to their information
            
        Returns:
            Tuple of (results DataFrame, SQL query string, explanation string)
        """
        # Generate the SQL query
        results, sql_query, explanation = self.generate_sql(query, tables_info)
        
        # If we already have results from generate_sql, use those
        if not results.empty:
            # Store the results for later retrieval
            self.last_results = results
            self.last_sql_query = sql_query
            self.last_explanation = explanation
            
            return results, sql_query, explanation
        
        # If we have a SQL query but no results, execute it
        if sql_query:
            # Execute the SQL query
            results = self.execute_sql(sql_query)
            
            # Store the results for later retrieval
            self.last_results = results
            self.last_sql_query = sql_query
            self.last_explanation = explanation
        
        return results, sql_query, explanation
    
    def execute_sql(self, sql_query: str) -> pd.DataFrame:
        """Execute a SQL query against the WRDS database.
        
        Args:
            sql_query: SQL query to execute
            
        Returns:
            Pandas DataFrame containing the query results
        """
        if not self.connected:
            self.logger.error("Not connected to WRDS database")
            return pd.DataFrame()
        
        try:
            # Clean the SQL query by removing markdown formatting
            cleaned_query = sql_query.strip()
            # Remove markdown code block formatting if present
            if cleaned_query.startswith('```'):
                # Find the end of the first line
                first_line_end = cleaned_query.find('\n')
                if first_line_end != -1:
                    # Remove the first line (```sql)
                    cleaned_query = cleaned_query[first_line_end + 1:]
            
            # Remove trailing markdown code block closing if present
            if cleaned_query.endswith('```'):
                cleaned_query = cleaned_query[:-3].strip()
            
            self.logger.info(f"Executing cleaned SQL query: {cleaned_query}")
            
            # Execute the SQL query
            result = self.db.raw_sql(cleaned_query)
            return result
        except Exception as e:
            self.logger.error(f"Error executing SQL query: {e}")
            return pd.DataFrame()
    
    def _format_tables_info(self, tables_info: Dict[str, Dict[str, Any]]) -> str:
        """Format table information for use in the prompt.
        
        Args:
            tables_info: Dictionary mapping table names to their information
            
        Returns:
            Formatted string containing table information
        """
        formatted_info = ""
        
        # Ensure tables_info is a dictionary
        if not isinstance(tables_info, dict):
            self.logger.error(f"tables_info is not a dictionary: {type(tables_info)}")
            return "No valid table information available"
        
        for table_name, table_info in tables_info.items():
            formatted_info += f"Table: {table_name}\n"
            
            # Handle different formats of table_info
            if isinstance(table_info, dict):
                formatted_info += f"Description: {table_info.get('description', 'No description')}\n"
                
                # Format fields
                formatted_info += "Fields:\n"
                fields = table_info.get('fields', {})
                
                if isinstance(fields, dict):
                    for field_name, field_desc in fields.items():
                        formatted_info += f"  - {field_name}: {field_desc}\n"
                elif isinstance(fields, list):
                    for field in fields:
                        if isinstance(field, dict) and 'name' in field and 'description' in field:
                            formatted_info += f"  - {field['name']}: {field['description']}\n"
                        elif isinstance(field, str):
                            formatted_info += f"  - {field}\n"
            elif isinstance(table_info, str):
                formatted_info += f"Description: {table_info}\n"
            
            formatted_info += "\n"
        
        return formatted_info
    
    def _extract_sql_and_explanation(self, response: str) -> Tuple[str, str]:
        """Extract SQL query and explanation from the API response.
        
        Args:
            response: Response from the OpenAI API
            
        Returns:
            Tuple of (SQL query string, explanation string)
        """
        sql_query = ""
        explanation = ""
        
        # Extract SQL query - check for both formats
        if "```sql" in response:
            # New format with markdown code blocks
            sql_parts = response.split("```sql", 1)[1].split("```", 1)
            sql_query = sql_parts[0].strip()
        elif "SQL QUERY:" in response:
            # Old format
            sql_parts = response.split("SQL QUERY:", 1)[1].split("EXPLANATION:", 1)
            sql_query = sql_parts[0].strip()
        
        # Extract explanation
        if "Explanation:" in response:
            explanation = response.split("Explanation:", 1)[1].strip()
        elif "EXPLANATION:" in response:
            explanation = response.split("EXPLANATION:", 1)[1].strip()
        
        # Fix any column name issues that might still occur
        # This is a fallback in case the model doesn't follow instructions
        if "nameenddt" in sql_query and "crsp.dsenames" in sql_query:
            sql_query = sql_query.replace("nameenddt", "nameendt")
        if "nameenddt" in sql_query and "crsp.msenames" in sql_query:
            sql_query = sql_query.replace("nameenddt", "nameendt")
        if "nameenddt" in sql_query and "crsp.stocknames" in sql_query:
            sql_query = sql_query.replace("nameenddt", "nameendt")
            
        return sql_query, explanation
    
    def save_results_to_csv(self, results: pd.DataFrame, query: str) -> str:
        """Save query results to a CSV file in the data folder.
        
        Args:
            results: DataFrame containing query results
            query: The original natural language query
            
        Returns:
            Path to the saved CSV file
        """
        # Initialize the last_csv_path
        self.last_csv_path = ""
        
        # Create a filename based on the query
        import re
        import os
        import datetime
        
        # Clean the query to create a valid filename
        clean_query = re.sub(r'[^\w\s]', '', query)[:30].strip().replace(' ', '_').lower()
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{clean_query}_{timestamp}.csv"
        
        # Ensure data directory exists
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)
        
        # Save to CSV
        filepath = os.path.join(data_dir, filename)
        results.to_csv(filepath, index=False)
        self.logger.info(f"Saved query results to {filepath}")
        
        # Store the CSV path for later retrieval
        self.last_csv_path = filepath
        
        return filepath
    
    def send_message(self, recipient: str, content: Dict[str, Any], message_type: str):
        """Send a message to another agent.
        
        Args:
            recipient: The recipient agent's name
            content: The message content
            message_type: The message type (e.g. "request", "response")
        """
        if recipient not in self.connections:
            self.logger.error(f"Cannot send message to unknown agent: {recipient}")
            return ""
            
        message = Message(self.agent_name, content, message_type, recipient)
        self.logger.info(f"Sending message to {recipient}: {message}")
        self.connections[recipient].receive_message(message)
        return message.id
    
    def receive_message(self, message: Message):
        """Receive a message from another agent.
        
        Args:
            message: The received message
        """
        self.logger.info(f"Received message from {message.sender}: {message}")
        self.process_message(message)
    
    def start(self):
        """Start the agent."""
        self.logger.info("Starting SQLAgent")
        # Implement agent startup logic here
    
    def stop(self):
        """Stop the agent."""
        self.logger.info("Stopping SQLAgent")
        # Implement agent shutdown logic here
