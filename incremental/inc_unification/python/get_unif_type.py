import pandas as pd
import pytd
import os 
import digdag

def main(db, endpoint): 
    
    query = f'select unif_type from unif_type_log where time = (select max(time) from unif_type_log)'
    client = pytd.Client(apikey=os.environ['TD_API_KEY'], endpoint=endpoint, database=db)

    results = client.query(query,engine='presto')

    print(results)

    digdag.env.store({"unif_type": results['data'][0][0]})


