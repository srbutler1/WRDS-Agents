# WRDS Multi-Agent Semantic Data System

This package provides a Python multi-agent system for retrieving data from the Wharton Research Data Services (WRDS) using natural language queries. It allows you to describe the financial data you need in plain English, and the system will generate and execute the appropriate SQL queries against the WRDS database.

## Features

- **Multi-Agent Architecture**: Specialized agents work together to process queries
  - **Administrator Agent**: Manages the query process and coordinates between agents
  - **Documentation Agent**: References WRDS documentation and maintains a knowledge base
  - **SQL Agent**: Writes SQL syntax and executes queries against the database
  - **Validator Agent**: Reviews the data to ensure it matches what the user requested
- **Natural Language Interface**: Query financial data using plain English
- **Semantic Understanding**: Maps common financial terms to WRDS database tables and columns
- **Secure Credential Management**: Uses environment variables for secure authentication
- **SQL Query Generation**: Automatically generates SQL queries based on natural language input
- **Automatic CSV Storage**: All query results are automatically saved to CSV files in the `data` directory
- **Robust Error Handling**: Gracefully handles API connection errors and provides fallbacks
- **Interactive Mode**: Provides an interactive console for exploring WRDS data
- Natural language interface for querying the WRDS database
- Multi-agent architecture for coordinated data retrieval
- Automatic SQL query generation based on natural language
- Schema information extracted from WRDS documentation
- Automatic saving of query results to CSV files in the data folder
- Comprehensive error handling and logging

## Project Structure

```
wrds_agent/
├── agents/                 # Agent implementations
│   ├── __init__.py
│   ├── administrator_agent.py
│   ├── base_agent.py
│   ├── documentation_agent.py
│   ├── sql_agent.py
│   └── validator_agent.py
├── data/                   # Directory for saved CSV query results
├── utils/                  # Utility functions
│   ├── __init__.py
│   ├── openai_utils.py
│   └── schema_extractor.py
├── wrds_schema.json        # Extracted schema information
├── autonomous_wrds_system.py  # Main system implementation
├── query_wrds.py          # Command-line interface
├── test_setup.py          # Setup verification script
└── requirements.txt       # Python dependencies
```

## Installation

1. Clone this repository
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the root directory with your WRDS and OpenAI credentials (see `.env.template` for an example):

```
WRDS_USERNAME=your_username
WRDS_PASSWORD=your_password
OPENAI_API_KEY=your_openai_api_key
```

4. Run the setup verification script to ensure everything is configured correctly:

```bash
python test_setup.py
```

## Schema Information

The system uses a schema information file (`wrds_schema.json`) that contains details about the WRDS database tables and their fields. This file is automatically loaded by the DocumentationAgent when the system starts.

If you have WRDS documentation files, you can place them in a directory named `WRDS Documentation` in the parent directory of this repository, and the system will attempt to extract schema information from them. If no documentation files are found, the system will use the existing schema information in the `wrds_schema.json` file.

## Usage

### Running the Multi-Agent System

```bash
python main.py
```

This will run example queries and then enter an interactive mode where you can input your own natural language queries.

### Saving Results

All query results are automatically saved to CSV files in the `data` directory. The filename is generated based on the query and includes a timestamp to ensure uniqueness.

You can also specify a custom output file using the `-o` or `--output` flag:

```bash
python main.py -o apple_returns.csv
```

### Example Queries

The multi-agent system understands a variety of financial data queries, such as:

- "Get daily stock prices for AAPL from 2022-01-01 to 2022-12-31"
- "Show me the fundamentals for MSFT for 2021"
- "What are the analyst estimates for AMZN in the last 6 months?"
- "Get the return on assets for tech companies in 2022"
- "Show me the debt to equity ratio for TSLA over the last 3 years"

## How It Works

The WRDS Multi-Agent System works by:

1. **Administrator Agent**: Receives the user query and coordinates the process
2. **Documentation Agent**: Provides relevant WRDS documentation and schema information
3. **SQL Agent**: Generates and executes SQL queries based on the parsed intent
4. **Validator Agent**: Ensures the returned data matches what the user requested

The system saves all query results to both CSV files and a SQLite database for easy access and persistence.

## System Architecture

```
┌─────────────────┐     ┌─────────────────┐
│  Administrator  │◄────┤     User        │
│     Agent       │     │    Query        │
└───────┬─────────┘     └─────────────────┘
        │
        ▼
┌─────────────────┐     ┌─────────────────┐
│  Documentation  │     │    Validator    │
│     Agent       │     │      Agent      │
└───────┬─────────┘     └───────▲─────────┘
        │                       │
        ▼                       │
┌─────────────────┐             │
│      SQL        │─────────────┘
│     Agent       │
└─────────────────┘
```

## Data Storage

The system stores data in two formats:

1. **CSV Files**: Saved in the `data` directory with filenames based on the data type, ticker, and timestamp
2. **SQLite Database**: Stored in `data/wrds_data.db` with tables for:
   - `queries`: Tracks all executed queries with timestamps
   - `stock_prices`: Daily stock price data
   - `fundamentals`: Annual financial statement data
   - `analyst_estimates`: Analyst forecast data

## API Key Management

The system requires a valid OpenAI API key to function properly. The key must:
- Start with 'sk-' followed by a string of characters
- Be stored in the `.env` file as `OPENAI_API_KEY`

For testing purposes, the system will use a dummy API key if a valid one is not provided, but functionality will be limited.

## Error Handling

The system includes robust error handling for:
- OpenAI API connection errors
- Invalid API keys
- Database connection issues
- Query parsing failures
- Data validation problems

## Security Notes

- Never commit your `.env` file with real credentials to version control
- The `.env` file is listed in `.gitignore` to prevent accidental commits
- For production usage, consider using a secrets management service instead of an `.env` file

## Troubleshooting

If you encounter issues:

1. Verify your WRDS credentials and OpenAI API key are correct in the `.env` file
2. Ensure you have access to the WRDS databases you're querying
3. Check for error messages in the console output
4. Make sure you have network access to both the WRDS server and OpenAI API

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Wharton Research Data Services (WRDS) for providing the financial data
- OpenAI for natural language processing capabilities
- SQLite for database storage
- Pandas for data manipulation
