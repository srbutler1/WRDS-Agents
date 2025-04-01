from __future__ import annotations as _annotations
import asyncio
import os
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from pydantic_ai import Agent, RunContext
from agent_tools import execute_sql_query, parse_user_intent, construct_sql_query
from agent_prompts import SYSTEM_PROMPT, USER_QUERY_PROMPT_TEMPLATE
import logfire
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logger
logfire.configure()

@dataclass
class Deps:
    wrds_username: str
    wrds_password: str
    wrds_host: str
    wrds_port: int
    wrds_db: str
    wrds_sslmode: str

wrds_agent = Agent(
    model="openai:gpt-4o",
    system_prompt=SYSTEM_PROMPT,
    deps_type=Deps,
    retries=3,
)

@wrds_agent.tool
async def get_data_from_wrds(ctx: RunContext[Deps], user_query: str) -> Dict[str, Any]:
    """
    Extract data from WRDS database based on user query.

    Args:
        ctx: The context including WRDS credentials and connection details
        user_query: User's natural language query describing the necessary data
    """
    # Parse user intent and retrieve metadata
    parsed_intent = await parse_user_intent(user_query)

    # Construct and execute SQL query
    sql_query = construct_sql_query(parsed_intent)
    query_result = await execute_sql_query(ctx, sql_query)

    # Handle new return value format from execute_sql_query
    if "error" in query_result:
        return {
            "success": False,
            "error": query_result['error'],
            "query": sql_query
        }
    
    # Add information about where the data is stored
    data_description = f"Data saved to file: {query_result.get('file_path')}\n"
    data_description += f"Data saved to database: {query_result.get('database')}\n"
    data_description += f"Query ID: {query_result.get('query_id')}\n\n"
    
    # Add data summary
    data_description += f"Retrieved {query_result['row_count']} rows with columns: {', '.join(query_result['columns'])}\n\n"
    
    # Add sample data (first few rows)
    sample_size = min(5, query_result['row_count'])
    if sample_size > 0:
        data_description += "Sample data:\n"
        for i in range(sample_size):
            row = query_result['data'][i]
            data_description += f"Row {i+1}: {json.dumps(row)}\n"
    
    return {
        "success": True,
        "query": sql_query,
        "data_location": {
            "file": query_result.get('file_path'),
            "database": query_result.get('database'),
            "query_id": query_result.get('query_id')
        },
        "row_count": query_result['row_count'],
        "columns": query_result['columns'],
        "sample_data": query_result['data'][:sample_size],
        "data_description": data_description
    }

async def main():
    """Main function to start the agent and run the query."""
    # Load credentials from environment variables
    deps = Deps(
        wrds_username=os.getenv("WRDS_USERNAME"),
        wrds_password=os.getenv("WRDS_PASSWORD"),
        wrds_host=os.getenv("WRDS_HOST"),
        wrds_port=int(os.getenv("WRDS_PORT")),
        wrds_db=os.getenv("WRDS_DB"),
        wrds_sslmode=os.getenv("WRDS_SSLMODE")
    )

    # Example query
    user_query = "Describe the annual returns for S&P 500 companies in the tech sector for 2021."
    
    # Format the query using the template
    formatted_query = USER_QUERY_PROMPT_TEMPLATE.format(query_description=user_query)
    
    try:
        # Run the agent with the query
        result = await wrds_agent.run(formatted_query, deps=deps)
        print('Response:', result.data)
    except Exception as e:
        logfire.error(f"Error running WRDS agent: {str(e)}")
        print(f"Error: {str(e)}")

if __name__ == '__main__':
    asyncio.run(main())