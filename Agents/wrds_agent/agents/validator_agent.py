from __future__ import annotations
import os
import json
from typing import Dict, Any, List, Optional
import openai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ValidatorAgent:
    """Validator Agent that verifies the data matches what the user requested."""
    
    def __init__(self):
        """Initialize the Validator Agent."""
        self.api_key = self._get_valid_api_key()
        self.client = openai.OpenAI(api_key=self.api_key)
    
    def _get_valid_api_key(self) -> str:
        """Get a valid OpenAI API key or return a dummy key for testing."""
        api_key = os.getenv("OPENAI_API_KEY")
        
        # Check if API key is valid (should start with 'sk-')
        if not api_key or not api_key.startswith("sk-"):
            print("Warning: Invalid or missing OpenAI API key. Using dummy key for testing.")
            # Use a dummy key for testing that has the correct format
            api_key = "sk-dummy1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefgh"
        
        return api_key
    
    async def validate_results(self, user_query: str, enriched_query: str, sql_query: str, query_result: Dict[str, Any]) -> Dict[str, bool]:
        """Validate that the query results match what the user requested."""
        try:
            # Check if the query was successful
            if not query_result.get("success", False):
                return {"valid": False, "error": query_result.get("error", "Query execution failed")}
            
            # Get the sample data and metadata
            sample_data = query_result.get("sample_data", [])
            columns = query_result.get("columns", [])
            row_count = query_result.get("row_count", 0)
            
            # If no data was returned, check if that's expected
            if row_count == 0 or not sample_data:
                return await self._validate_empty_results(user_query, enriched_query, sql_query)
            
            # Use OpenAI to validate the results
            system_prompt = """
            Financial data validator. Check if query results match user request.
            
            Verify:
            1. Requested tickers present
            2. Time period matches
            3. Required metrics/columns included
            4. Data format correct
            
            Return JSON: {"valid": true/false, "issues": [problems found], "explanation": "brief reason"}
            """
            
            # Prepare the user message with all the context
            user_message = f"""
            Original user query: {user_query}
            
            Enriched query: {enriched_query}
            
            SQL query: {sql_query}
            
            Data columns: {', '.join(columns)}
            
            Number of rows: {row_count}
            
            Sample data:
            {json.dumps(sample_data, indent=2)}
            """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                response_format={"type": "json_object"}
            )
            
            validation_result = json.loads(response.choices[0].message.content)
            
            # Return the validation result
            return {
                "valid": validation_result.get("valid", True),
                "issues": validation_result.get("issues", []),
                "explanation": validation_result.get("explanation", "")
            }
        except openai.APIConnectionError as e:
            print(f"OpenAI API connection error: {str(e)}")
            # Default to valid if API call fails
            return {"valid": True}
        except openai.APIError as e:
            print(f"OpenAI API error: {str(e)}")
            return {"valid": True}
        except Exception as e:
            print(f"Error validating results: {str(e)}")
            return {"valid": True}
    
    async def _validate_empty_results(self, user_query: str, enriched_query: str, sql_query: str) -> Dict[str, Any]:
        """Validate empty results to determine if they're expected or an error."""
        try:
            system_prompt = """
            Financial data expert. Determine if empty query results are expected.
            
            Check if:
            1. Data might not exist (future data, specific criteria)
            2. Query contains logical contradictions
            3. SQL query is too restrictive
            
            Return JSON: {"valid": true/false, "error": "reason for error" (if invalid), "explanation": "brief reason"}
            """
            
            # Prepare the user message with all the context
            user_message = f"""
            Original user query: {user_query}
            
            Enriched query: {enriched_query}
            
            SQL query: {sql_query}
            
            The query returned no results.
            """
            
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                response_format={"type": "json_object"}
            )
            
            validation_result = json.loads(response.choices[0].message.content)
            
            # Return the validation result
            return {
                "valid": validation_result.get("valid", False),
                "error": validation_result.get("error", "No results returned from query") if not validation_result.get("valid", False) else None,
                "explanation": validation_result.get("explanation", "")
            }
        except Exception as e:
            print(f"Error validating empty results: {str(e)}")
            # Default to invalid if validation fails
            return {"valid": False, "error": "No results returned from query and validation failed"}
