import sys
from td_api import create_knowledge_base, get_database_tables

def main(project_id: str, td_apikey: str, database_name: str):
    try:
        # Get the list of tables from the provided database
        table_list = get_database_tables(td_apikey, database_name)
        # Set a default name for the knowledge base (adjust as needed)
        kb_name = "knowledge_base"
        # Create the knowledge base using the provided project_id, td_apikey and database name
        kb_id = create_knowledge_base(td_apikey, kb_name, project_id, database_name, table_list)
        print(f"Successfully created knowledge base with ID: {kb_id}")
    except Exception as e:
        print(f"Error creating knowledge base: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python main.py <project_id> <td_api_key> <database_name>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3])
