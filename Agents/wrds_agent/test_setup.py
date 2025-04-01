#!/usr/bin/env python
"""
Test script to verify the WRDS system setup is working correctly.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

def check_environment():
    """Check if the environment is set up correctly."""
    # Load environment variables
    load_dotenv()
    
    # Initialize error flag
    has_error = False
    
    # Check for WRDS credentials
    wrds_username = os.getenv("WRDS_USERNAME")
    wrds_password = os.getenv("WRDS_PASSWORD")
    
    if not wrds_username or wrds_username == "your_username":
        print("❌ WRDS_USERNAME not set in .env file")
        has_error = True
    else:
        print(f"✅ WRDS_USERNAME found: {wrds_username}")
    
    if not wrds_password or wrds_password == "your_password":
        print("❌ WRDS_PASSWORD not set in .env file")
        has_error = True
    else:
        print("✅ WRDS_PASSWORD found")
    
    # Check for OpenAI API key
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key or openai_api_key.startswith("sk-your_"):
        print("❌ OPENAI_API_KEY not set in .env file")
        has_error = True
    else:
        print("✅ OPENAI_API_KEY found")
    
    # Check for data directory
    data_dir = Path(__file__).parent / "data"
    if not data_dir.exists():
        print(f"❌ Data directory not found at {data_dir}")
        print("   Creating data directory...")
        os.makedirs(data_dir, exist_ok=True)
        print("✅ Data directory created")
    else:
        print(f"✅ Data directory found at {data_dir}")
    
    # Check for required modules
    try:
        import pandas
        print("✅ pandas module found")
    except ImportError:
        print("❌ pandas module not found. Run 'pip install -r requirements.txt'")
        has_error = True
    
    try:
        import openai
        print("✅ openai module found")
    except ImportError:
        print("❌ openai module not found. Run 'pip install -r requirements.txt'")
        has_error = True
    
    try:
        import wrds
        print("✅ wrds module found")
    except ImportError:
        print("❌ wrds module not found. Run 'pip install -r requirements.txt'")
        has_error = True
    
    if has_error:
        print("\n❌ Setup incomplete. Please fix the issues above and run this script again.")
    else:
        print("\n✅ Setup complete! You can now run queries with 'python query_wrds.py'")

def main():
    """Main function to run the setup test."""
    print("WRDS System Setup Test")
    print("======================")
    check_environment()

if __name__ == "__main__":
    main()
