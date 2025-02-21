import sys
from td_api import create_knowledge_base, get_database_tables

def main(project_id: str, td_apikey: str, database_name: str, kb_name: str):
    try:
        # Get the list of tables from the provided database
        table_list = get_database_tables(td_apikey, database_name)
        # Create the knowledge base using the provided parameters
        kb_id = create_knowledge_base(td_apikey, kb_name, project_id, database_name, table_list)
        print(f"Successfully created knowledge base '{kb_name}' with ID: {kb_id}")
    except Exception as e:
        print(f"Error creating knowledge base: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python main.py <project_id> <td_api_key> <database_name> <knowledge_base_name>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
