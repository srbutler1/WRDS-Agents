import os
import sys
from pathlib import Path

# Add the parent directory to sys.path to allow imports
base_dir = Path(__file__).parent.parent
sys.path.insert(0, str(base_dir))

# Print current working directory
print(f"Current working directory: {os.getcwd()}")

# Try different paths to find wrds_schema.json
file_paths = [
    # Path in the same directory as the script
    os.path.join(os.path.dirname(__file__), "wrds_schema.json"),
    # Path in the parent directory
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "wrds_schema.json"),
    # Path in the parent's parent directory
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "wrds_schema.json"),
    # Absolute path
    "/Users/appleowner/Downloads/FDA II/Demo Agent/Agents/wrds_schema.json"
]

print("\nChecking for wrds_schema.json:")
for path in file_paths:
    exists = os.path.exists(path)
    print(f"Path: {path}\nExists: {exists}\n")

# Check the path used in DocumentationAgent
def get_documentation_agent_paths():
    # Simulate the path calculation in DocumentationAgent.__init__
    current_file = __file__
    current_dir = os.path.dirname(current_file)
    
    # Path to docs directory
    docs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(current_file))), "WRDS Documentation")
    print(f"Docs directory path: {docs_dir}")
    print(f"Docs directory exists: {os.path.exists(docs_dir)}")
    
    # Path to schema JSON file - first attempt
    agents_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
    schema_json_path1 = os.path.join(agents_dir, "wrds_schema.json")
    print(f"Schema JSON path (first attempt): {schema_json_path1}")
    print(f"Schema JSON file exists (first attempt): {os.path.exists(schema_json_path1)}")
    
    # Path to schema JSON file - fallback
    schema_json_path2 = os.path.join(os.path.dirname(os.path.dirname(current_file)), "wrds_schema.json")
    print(f"Schema JSON path (fallback): {schema_json_path2}")
    print(f"Schema JSON file exists (fallback): {os.path.exists(schema_json_path2)}")

print("\nDocumentationAgent path calculations:")
get_documentation_agent_paths()
