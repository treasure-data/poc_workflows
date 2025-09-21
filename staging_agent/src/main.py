import sys
import logging
import os

# Add common module to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'common'))

from td_api_client import create_staging_agent, TreasureDataAPIError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main(td_apikey: str, database_name: str, region: str = "us"):
    """
    Main function to create a staging agent.
    
    Args:
        td_apikey: Treasure Data API key
        database_name: Source database name
        region: TD region (us or eu)
    """
    try:
        logger.info(f"Creating staging agent for database: {database_name}")
        create_staging_agent(td_apikey, database_name, region)
        logger.info(f"Successfully created staging agent for database: {database_name}")
    except TreasureDataAPIError as e:
        logger.error(f"API error creating staging agent: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error creating staging agent: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python main.py <td_api_key> <database_name> [region]")
        print("  region: 'us' (default) or 'eu'")
        sys.exit(1)
    
    api_key = sys.argv[1]
    database_name = sys.argv[2]
    region = sys.argv[3] if len(sys.argv) == 4 else "us"
    
    if region not in ["us", "eu"]:
        print("Error: region must be 'us' or 'eu'")
        sys.exit(1)
    
    main(api_key, database_name, region)
