"""Utilities for OpenAI API calls.

This module provides utility functions for making OpenAI API calls.
"""

import os
import logging
import time
from typing import Optional, Dict, Any, List

import openai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Maximum number of retries for API calls
MAX_RETRIES = 3
# Delay between retries (in seconds)
RETRY_DELAY = 2

def get_valid_api_key() -> str:
    """Get a valid OpenAI API key or return a dummy key for testing.
    
    Returns:
        A valid API key or a dummy key for testing
    """
    api_key = os.getenv("OPENAI_API_KEY")
    
    # Check if API key is valid (should start with 'sk-')
    if not api_key or not api_key.startswith("sk-"):
        logger.warning("Invalid or missing OpenAI API key. Using dummy key for testing.")
        # Use a dummy key for testing that has the correct format
        api_key = "sk-dummy1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefgh"
    
    return api_key

def get_completion(prompt: str, model: str = "gpt-4o", temperature: float = 0.0) -> str:
    """Get a completion from the OpenAI API.
    
    Args:
        prompt: The prompt to send to the API
        model: The model to use for the completion
        temperature: The temperature to use for the completion
        
    Returns:
        The completion text
    """
    api_key = get_valid_api_key()
    client = openai.OpenAI(api_key=api_key)
    
    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant with expertise in financial databases and SQL."},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature
            )
            
            return response.choices[0].message.content.strip()
        except openai.APIConnectionError as e:
            logger.warning(f"OpenAI API connection error (attempt {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error("Max retries reached. Returning empty string.")
                return ""
        except openai.APIError as e:
            logger.error(f"OpenAI API error: {e}")
            return ""
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return ""
