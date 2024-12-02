import pandas as pd
import pytd


class TDConnector:

    def read(query:str, db: str, table: str,td_api_key,td_endpoint:str) -> pd.DataFrame:
        client = pytd.Client(apikey=td_api_key, endpoint=td_endpoint, database=db)

        results = client.query(query,engine='presto')
        #results = client.query(f'select * from {table}',engine='presto')

        return pd.DataFrame(**results)


    def write(df: pd.DataFrame, db: str, table: str, td_api_key,td_endpoint:str):
        client = pytd.Client(apikey=td_api_key, endpoint=td_endpoint, database=db)
        client.load_table_from_dataframe(df, table, writer='bulk_import', if_exists='overwrite')


#------append table in TD
    def insert_df(df: pd.DataFrame, db: str, table: str, td_api_key,td_endpoint:str):
        client = pytd.Client(apikey=td_api_key, endpoint=td_endpoint, database=db)
        client.load_table_from_dataframe(df, table, writer='bulk_import', if_exists='append')