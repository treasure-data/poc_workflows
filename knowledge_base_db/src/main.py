import sys
import logging
import os

# Add common module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'common'))

from td_api_client import create_knowledge_base_from_database, TreasureDataAPIError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main(project_id: str, td_apikey: str, database_name: str, kb_name: str, region: str = "us"):
    """
    Main function to create a knowledge base from a database.
    
    Args:
        project_id: Target project ID
        td_apikey: Treasure Data API key
        database_name: Source database name
        kb_name: Knowledge base name
        region: TD region (us or eu)
    """
    try:
        logger.info(f"Creating knowledge base '{kb_name}' from database: {database_name}")
        kb_id = create_knowledge_base_from_database(td_apikey, project_id, database_name, kb_name, region)
        logger.info(f"Successfully created knowledge base '{kb_name}' with ID: {kb_id}")
    except TreasureDataAPIError as e:
        logger.error(f"API error creating knowledge base: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error creating knowledge base: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 5 or len(sys.argv) > 6:
        print("Usage: python main.py <project_id> <td_api_key> <database_name> <knowledge_base_name> [region]")
        print("  region: 'us' (default) or 'eu'")
        sys.exit(1)
    
    project_id = sys.argv[1]
    api_key = sys.argv[2]
    database_name = sys.argv[3]
    kb_name = sys.argv[4]
    region = sys.argv[5] if len(sys.argv) == 6 else "us"
    
    if region not in ["us", "eu"]:
        print("Error: region must be 'us' or 'eu'")
        sys.exit(1)
    
    main(project_id, api_key, database_name, kb_name, region)
