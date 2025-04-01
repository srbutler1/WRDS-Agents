#!/usr/bin/env python
"""
Autonomous WRDS Multi-Agent System for financial data retrieval.

This system uses autonomous agents with message passing to process natural language queries,
generate SQL, and retrieve data from the WRDS database without central coordination.
"""

import os
import sys
import time
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
from queue import Queue
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AutonomousWRDSSystem:
    """Autonomous multi-agent system for WRDS data retrieval."""

    def __init__(self, docs_dir: Optional[str] = None):
        """Initialize the Autonomous WRDS System.
        
        Args:
            docs_dir: Directory containing WRDS documentation files
        """
        # Load environment variables
        load_dotenv()
        
        # Add the parent directory to sys.path to allow imports
        base_dir = Path(__file__).parent.parent
        sys.path.insert(0, str(base_dir))
        
        # Import the agents
        from wrds_agent.agents.documentation_agent import DocumentationAgent
        from wrds_agent.agents.sql_agent import SQLAgent
        
        # Initialize the agents
        self.doc_agent = DocumentationAgent(docs_dir)
        self.sql_agent = SQLAgent()
        
        # Set up message queues for each agent
        self.doc_agent_queue = Queue()
        self.sql_agent_queue = Queue()
        
        # Register agents with their message queues
        self.agents = {
            "documentation_agent": {
                "agent": self.doc_agent,
                "queue": self.doc_agent_queue
            },
            "sql_agent": {
                "agent": self.sql_agent,
                "queue": self.sql_agent_queue
            }
        }
        
        # Register agents with each other
        self.doc_agent.connect("sql_agent", self.sql_agent)
        self.sql_agent.connect("documentation_agent", self.doc_agent)
        
        logger.info("Autonomous WRDS System initialized")
    
    def connect_to_wrds(self, username: Optional[str] = None, password: Optional[str] = None) -> bool:
        """Connect to the WRDS database.
        
        Args:
            username: WRDS username (if not provided, will use environment variable)
            password: WRDS password (if not provided, will use environment variable)
            
        Returns:
            True if connection is successful, False otherwise
        """
        # Get credentials from environment variables if not provided
        if not username:
            username = os.environ.get("WRDS_USERNAME")
        if not password:
            password = os.environ.get("WRDS_PASSWORD")
        
        if not username or not password:
            logger.error("WRDS credentials not found")
            return False
        
        # Update the SQL agent's credentials
        self.sql_agent.wrds_username = username
        self.sql_agent.wrds_password = password
        
        # Connect to the WRDS database
        return self.sql_agent.connect_to_db()
    
    def send_message(self, recipient: str, content: Dict[str, Any], message_type: str, sender: str = "system"):
        """Send a message to an agent.
        
        Args:
            recipient: The recipient agent's name
            content: The message content
            message_type: The message type (e.g. "request", "response")
            sender: The sender's name
        """
        from wrds_agent.agents.base_agent import Message
        
        if recipient not in self.agents:
            logger.error(f"Unknown recipient: {recipient}")
            return
        
        message = Message(sender, content, message_type, recipient)
        self.agents[recipient]["queue"].put(message)
        logger.info(f"Message sent to {recipient}: {message}")
    
    def process_messages(self, max_iterations: int = 10, timeout: int = 30):
        """Process messages in the queues.
        
        Args:
            max_iterations: Maximum number of iterations to process messages
            timeout: Maximum time to wait for messages (in seconds)
        """
        start_time = time.time()
        iteration = 0
        
        while iteration < max_iterations and (time.time() - start_time) < timeout:
            # Process messages for each agent
            messages_processed = 0
            
            for agent_name, agent_info in self.agents.items():
                queue = agent_info["queue"]
                agent = agent_info["agent"]
                
                if not queue.empty():
                    message = queue.get()
                    logger.info(f"Processing message for {agent_name}: {message}")
                    
                    # Process the message
                    agent.process_message(message)
                    messages_processed += 1
            
            # If no messages were processed, wait a bit
            if messages_processed == 0:
                time.sleep(0.1)
            
            iteration += 1
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """Process a natural language query and return the results.
        
        Args:
            query: Natural language query
            
        Returns:
            Dictionary containing results, SQL query, and explanation
        """
        try:
            # Send a request directly to the SQL agent with the query
            self.send_message(
                recipient="sql_agent",
                content={
                    "query": query
                },
                message_type="request"
            )
            
            # Process messages to allow agents to communicate
            self.process_messages(max_iterations=20, timeout=60)
            
            # Check if the SQL agent has results
            results = None
            sql_query = ""
            explanation = ""
            csv_path = ""
            
            # The SQL agent should have stored results
            if hasattr(self.sql_agent, "last_results") and self.sql_agent.last_results is not None:
                results = self.sql_agent.last_results
                sql_query = self.sql_agent.last_sql_query
                explanation = self.sql_agent.last_explanation
                csv_path = getattr(self.sql_agent, "last_csv_path", "")
            
            if results is not None:
                return {
                    "success": True,
                    "results": results,
                    "sql_query": sql_query,
                    "explanation": explanation,
                    "row_count": len(results) if isinstance(results, pd.DataFrame) and not results.empty else 0,
                    "csv_path": csv_path
                }
            else:
                return {
                    "success": False,
                    "error": "No results returned from agents"
                }
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                "success": False,
                "error": str(e)
            }

def main():
    """Main function to run the Autonomous WRDS System."""
    # Get the documentation directory
    base_dir = Path(__file__).parent.parent
    docs_dir = base_dir / "WRDS Documentation"
    
    if not docs_dir.exists():
        logger.error(f"Documentation directory not found: {docs_dir}")
        logger.info("Using default schema information")
        docs_dir = None
    
    # Initialize the Autonomous WRDS System
    system = AutonomousWRDSSystem(str(docs_dir) if docs_dir else None)
    
    # Connect to WRDS database
    connected = system.connect_to_wrds()
    
    if not connected:
        logger.warning("Failed to connect to WRDS database")
        logger.info("Continuing with limited functionality (SQL generation only)")
    
    # Process a query from the command line or use a default example
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = "Get daily stock returns for Apple (ticker: AAPL) for the year 2022"
    
    logger.info(f"Processing query: {query}")
    
    result = system.process_query(query)
    
    if result["success"]:
        logger.info(f"SQL Query: {result['sql_query']}")
        logger.info(f"Explanation: {result['explanation']}")
        
        if result["row_count"] > 0:
            logger.info(f"Results (first 5 rows):\n{result['results'].head()}")
            logger.info(f"Total rows: {result['row_count']}")
        else:
            logger.info("No results returned")
    else:
        logger.error(f"Error: {result['error']}")

if __name__ == "__main__":
    main()
