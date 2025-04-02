# Autonomous WRDS System

This document provides instructions for using the Autonomous WRDS System, which allows you to query the Wharton Research Data Services (WRDS) database using natural language.

## Quick Start

1. Clone this repository
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your credentials (copy from `.env.template`):
   ```
   WRDS_USERNAME=your_username
   WRDS_PASSWORD=your_password
   OPENAI_API_KEY=your_openai_api_key
   ```
4. Verify your setup:
   ```bash
   python test_setup.py
   ```
5. Run a query:
   ```bash
   python query_wrds.py "get the returns of Apple stock for 2022"
   ```

## Features

- **Natural Language Queries**: Ask questions in plain English
- **Automatic SQL Generation**: Converts natural language to SQL
- **Multi-Agent Architecture**: Coordinates between documentation and SQL agents
- **CSV Storage**: Automatically saves results to CSV files in the `data` directory
- **Schema Information**: Uses a pre-extracted schema from WRDS documentation

## Query Examples

```bash
# Get stock returns
python query_wrds.py "Get daily stock returns for Tesla (ticker: TSLA) for the year 2022"

# Get company fundamentals
python query_wrds.py "Get the fundamentals of Apple for the year 2022"

# Get specific financial metrics
python query_wrds.py "What was the return on assets for tech companies in 2022?"
```

## Output

The system will:
1. Process your natural language query
2. Generate and execute an appropriate SQL query
3. Display the results (limited to the first 5 rows)
4. Save the complete results to a CSV file in the `data` directory
5. Show the path to the saved CSV file

Example output:
```
SQL Query:
<SQL query details>

Explanation:
<Explanation of the query>

Results (first 5 rows):
<Table with first 5 rows of results>

Total rows: 251

Results saved to CSV: /path/to/data/query_results_20250401_123456.csv
```

## Data Storage

All query results are automatically saved to CSV files in the `data` directory. The filename is generated based on the query and includes a timestamp to ensure uniqueness.

You can also specify a custom output file using the `-o` or `--output` flag:

```bash
python query_wrds.py "Get daily stock returns for Apple (ticker: AAPL) for the year 2022" -o apple_returns.csv
```

## System Architecture

The Autonomous WRDS System uses a multi-agent architecture:

1. **Documentation Agent**: Identifies relevant tables and provides schema information
2. **SQL Agent**: Generates and executes SQL queries based on the schema information

The system uses OpenAI's language models to understand natural language queries and generate appropriate SQL queries.

## Troubleshooting

### Common Issues

1. **Connection Issues**:
   - Ensure your WRDS credentials are correct in the `.env` file
   - Check your internet connection
   - Verify that the WRDS server is operational

2. **No Results Returned**:
   - Try rephrasing your query to be more specific
   - Ensure you're querying for data that exists in the WRDS database
   - Check the SQL query in the output to see if it looks correct

3. **Schema Information**:
   - If you see "Documentation directory not found" messages, this is normal
   - The system will use the pre-extracted schema in `wrds_schema.json`
   - If you have WRDS documentation files, place them in a directory named `WRDS Documentation` in the parent directory

4. **Module Not Found Errors**:
   - Ensure you've installed all dependencies with `pip install -r requirements.txt`
   - Some modules like `wrds` may require additional system dependencies

### Verifying Your Setup

Run the test_setup.py script to verify your environment is correctly configured:

```bash
python test_setup.py
```

This script checks for:
- Required environment variables
- Data directory existence
- Required Python modules

### Getting Help

If you encounter issues not covered here, please check the WRDS documentation or contact your WRDS administrator for assistance with database-specific questions.

## License

This project is licensed under the MIT License.
