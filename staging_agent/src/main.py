import sys
from td_api import create_staging_agent

def main(td_apikey: str, database_name: str):
    try:
        create_staging_agent(td_apikey, database_name)
        print(f"Successfully created staging agent for database: {database_name}")
    except Exception as e:
        print(f"Error creating staging agent: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <td_api_key> <database_name>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
