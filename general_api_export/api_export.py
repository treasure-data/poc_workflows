import requests
import json
import pytd
import os
import pandas as pd
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_df():

    # Environment variables for security - with validation
    TD_API_KEY = os.getenv("TD_API_KEY")
    if not TD_API_KEY:
        raise ValueError("TD_API_KEY environment variable is required")
    
    td_region = os.getenv("TD_REGION", "us").lower()
    if td_region == 'us':
        TD_ENDPOINT = "https://api.treasuredata.com"
    elif td_region == 'eu': 
        TD_ENDPOINT = "https://api.eu01.treasuredata.com"
    else:
        logger.warning(f"Unknown TD_REGION '{td_region}', defaulting to US endpoint")
        TD_ENDPOINT = "https://api.treasuredata.com"
    
    API_WRITE_KEY = os.getenv("API_KEY")
    if not API_WRITE_KEY:
        raise ValueError("API_KEY environment variable is required")
    
    td_database = os.getenv("TD_DATABASE")
    if not td_database:
        raise ValueError("TD_DATABASE environment variable is required")
    
    td_table = os.getenv("TD_TABLE")
    if not td_table:
        raise ValueError("TD_TABLE environment variable is required")
    
    columns = os.getenv('TD_COLUMNS')
    if not columns:
        raise ValueError("TD_COLUMNS environment variable is required")
    
    try:
        api_limit = int(os.getenv("api_limit", "1000"))
        lower_limit = int(os.getenv('lower_limit', "0"))
        upper_limit = int(os.getenv('upper_limit', "10000"))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid numeric environment variable: {e}")
    
    logger.info(f"Configuration loaded: database={td_database}, table={td_table}, region={td_region}")
  
    query = f"SELECT {columns} FROM {td_table} WHERE id > {lower_limit} AND id <= {upper_limit}"
    logger.info(f"Executing query: {query}")
    
    try:
        # Step 1: Set up the Treasure Data client
        client = pytd.Client(apikey=TD_API_KEY, endpoint=TD_ENDPOINT, database=td_database)
        
        # Fetch data from Treasure Data
        records = client.query(query, engine='presto')
        
        if not records or 'data' not in records:
            logger.warning("No data returned from query")
            return
        
        # Step 2: Convert the records into DataFrame
        df = pd.DataFrame.from_dict(records['data'])
        if 'columns' in records:
            df.columns = records['columns']
        
        logger.info(f"Retrieved {len(df)} records from Treasure Data")
        
    except Exception as e:
        logger.error(f"Error fetching data from Treasure Data: {e}")
        raise
  

    # Step 3: Process data in chunks
    total_chunks = (len(df) + api_limit - 1) // api_limit
    successful_chunks = 0
    
    for i in range(0, len(df), api_limit):
        chunk_num = (i // api_limit) + 1
        chunk = df.iloc[i:i + api_limit]
        
        logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} records)")
        
        try:
            ### Step 4: API LOGIC GOES HERE
            # Set up the API endpoint and headers
            api_endpoint = 'https://us1.app.pendo.io/api/v1/metadata/account/custom/value'
            headers = {
                'Content-Type': 'application/json',
                'x-pendo-integration-key': API_WRITE_KEY
            }
            
            # TODO: Construct payload from chunk data
            # This is where customer-specific logic should be implemented
            payload = []
            
            # Call API with timeout and retry logic
            response = requests.post(
                api_endpoint, 
                headers=headers, 
                data=json.dumps(payload),
                timeout=30
            )
            
            # Check response
            if response.status_code == 200:
                logger.info(f"Chunk {chunk_num} sent successfully")
                successful_chunks += 1
            else:
                logger.error(f"Chunk {chunk_num} failed: HTTP {response.status_code} - {response.text}")
                
        except requests.exceptions.Timeout:
            logger.error(f"Chunk {chunk_num} failed: Request timeout")
        except requests.exceptions.RequestException as e:
            logger.error(f"Chunk {chunk_num} failed: Network error - {e}")
        except Exception as e:
            logger.error(f"Chunk {chunk_num} failed: Unexpected error - {e}")
    
    logger.info(f"Export completed: {successful_chunks}/{total_chunks} chunks processed successfully")
