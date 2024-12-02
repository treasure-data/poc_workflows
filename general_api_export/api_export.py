import requests
import json
import pytd
import os
import pandas as pd


def load_df():

    # Environment variables for security
    TD_API_KEY = os.getenv("TD_API_KEY")
    if os.getenv("TD_REGION") == 'us':
      TD_ENDPOINT = "https://api.treasuredata.com"
    else if os.getenv("TD_REGION") == 'eu': 
      TD_ENDPOINT = "https://api.eu01.treasuredata.com"
    API_WRITE_KEY = os.getenv("API_KEY")
    td_database = os.getenv("TD_DATABASE")
    td_table= os.getenv("TD_TABLE")
    columns = os.environ.get('TD_COLUMNS')
    api_limit= int(os.getenv("api_limit"))
    lower_limit=int(os.environ.get('lower_limit'))
    upper_limit=int(os.environ.get('upper_limit'))
  
    query = f"Select {columns}  from {td_table} WHERE id > {lower_limit} and id <= {upper_limit}"
    print(query)
    # Step 1: Set up the Treasure Data client
    # Replace 'YOUR_TD_API_KEY' and 'your_database' with your actual API key and database name
    client = pytd.Client(apikey=TD_API_KEY,endpoint=TD_ENDPOINT, database=td_database)
    
    # Fetch data from Treasure Data
    # Replace 'your_table' with the actual table name
    records = client.query(query, engine='presto')
    
    # Step 2: Convert the records into DF
    df = pd.DataFrame.from_dict(records['data'])
    df.columns = records['columns']
  

    # Step 3: Loop through api_number # of records at a time 
    for i in range(0, len(df), api_limit):
      chunk = df.iloc[i:i + api_limit]
        # Construct Payload
      
      ### Step 4:  API LOGIC GOES HERE
      # Set up the API endpoint and headers
      api_endpoint = 'https://us1.app.pendo.io/api/v1/metadata/account/custom/value'
      headers = {
          'Content-Type': 'application/json',
          'x-pendo-integration-key': API_WRITE_KEY
    }
      # Construct Payoad
      payload = []
      
      # Call API
      response = requests.request("POST", api_endpoint, headers=headers, data=json.dumps(payload))

      ### API LOGIC GOES HERE
      
      # Print Response
      if response.status_code != 200:
          print(f"Failed to send records, Response: {response.text}")
      else:
          print(f"Successfully sent records, Response Code {response.status_code}, Response: {response.text}")
