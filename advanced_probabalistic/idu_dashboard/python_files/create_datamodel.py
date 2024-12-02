import os
import sys

##-- Declare ENV Variables from YML file
apikey = os.environ['TD_API_KEY'] 
tdserver = os.environ['TD_API_SERVER']
sink_database = os.environ['SINK_DB']
output_table = os.environ['OUTPUT_TABLE']

import pandas as pd
import numpy as np
import datetime
import os
import pytd
import requests
import json

##-- Declare ENV Variables from YML file
apikey = os.environ['TD_API_KEY'] 
tdserver = os.environ['TD_API_SERVER']
sink_database = os.environ['SINK_DB']
output_table = os.environ['OUTPUT_TABLE']
datamodel_name = os.environ.get('DATAMODEL_NAME', '')
datamodel_shared_users = os.environ.get('SHARED_USERS', '')

### --Function to create datamodel via API below
def create_model():

#######USE Code Below if you prefer to use config file
    with open('config.json', 'r') as f:
      config = json.load(f)

    print(config)

    #check if datamodel name should be taken from DigDag Yaml
    if datamodel_name:
      model_name = datamodel_name
    else:
      model_name =  config['model_name']
    model_tables = config['model_tables']
    change_schema_cols =  config['change_schema_cols']
    join_relations = config['join_relations']

    #check if users list should be taken from DigDag Yaml
    if datamodel_shared_users:
      shared_user_list = datamodel_shared_users
    else:
      shared_user_list = config['shared_user_list']

    print("Model name = {}".format(model_name))
    print("Table list = {}".format(model_tables))
    print("Cols Schema change = {}".format(change_schema_cols))
    print("Join Relations = {}".format(join_relations))
    print("Shared Users List = {}".format(shared_user_list))

    #Establish headers for authentication so you can call the API
    headers= {"Authorization": f"TD1 {apikey}", "content-type": "application/json"}

    #Call datamodel API and create table of all existing models
    datamodel_list = f'https://{tdserver}/reporting/datamodels'
    get_info = requests.get(datamodel_list, headers=headers)
    datamodel_list_json = get_info.json()

    if len(datamodel_list_json) == 0:
      #create empty list with N/A values so rest of code can work
      existing_models = ['N/A']

    else:
      #create empty Dict to store datamodels API response info
      datamodel_dic = dict(name = [], oid = [], created_by = [],
              updated_by = [], last_updated = [], last_built = [],
              status = [], created_at = [])

      for item in datamodel_list_json:
        datamodel_dic['name'].append(item['name'])
        datamodel_dic['oid'].append(item['oid'])
        datamodel_dic['created_by'].append(item['created_by'])
        datamodel_dic['updated_by'].append(item['updated_by'])
        datamodel_dic['last_updated'].append(item['last_updated'])
        datamodel_dic['last_built'].append(item['last_built'])
        datamodel_dic['status'].append(item['status'])
        datamodel_dic['created_at'].append(item['created_at'])
      
      datamodel_hist_df = pd.DataFrame(datamodel_dic)

      #write table to sink databse in TD
      client = pytd.Client(apikey=apikey, endpoint=tdserver, database=sink_database)
      client.load_table_from_dataframe(datamodel_hist_df, output_table, writer='bulk_import', if_exists='overwrite')

      #check if model with that name already exists in JSON response and if it exists skip building steps below
      existing_models = list(datamodel_hist_df.name)

    if model_name in existing_models:
      print(f'Datamodel with name: {model_name} already exists in the account. Workflow will move to next step = updating datamodel')
    else:
      print(f'Datamodel with name: {model_name} will be created for the first time')

      ############ DATAMODEL CREATION STARTS BELOW ##########################################
      
      ##Loop through dictionary of model_tables and create datamodel JSON with schema changes and joins
      db_set = list(set([sink_database for item in model_tables]))
      print(db_set)

      db_table_jsons = {item: {'type': 'presto', 'database': item, 'tables': []} for item in db_set}

      ##Fetch list of tables from each TD database you want to add to model and store under distinct db_name dic object
      for elements in model_tables:
              address = f'https://{tdserver}/v3/table/show/' +sink_database +'/' +elements['name']
              schema_info = requests.get(address,headers=headers).json()
              str0 =   '"' + schema_info['name'] + '"' + ':' + ' { ' + '"' +  "columns" + '"' + ':' 
              # print('STRING IS: {}'.format(str0))
              table_schema = json.loads(schema_info['schema'])
              str1= ''
              for name in table_schema:
                  if name[1] == 'long':
                      name[1] = 'bigint'
                  elif name[1] == 'double':
                      name[1] = 'float'
                  elif name[1] == 'string' or "array" in name[1].lower():
                      name[1] = 'text'
                  if name[0] in change_schema_cols['date']: 
                      name[1] = 'timestamp'  
                  elif name[0] in change_schema_cols['text']: 
                      name[1] = 'text'
                  elif name[0] in change_schema_cols['float']: 
                      name[1] = 'float'
                  elif name[0] in change_schema_cols['bigint']: 
                      name[1] = 'bigint'
                  str1 += '"' + name[0] + '"' + ':' + ' { ' + '"' + "type" + '"' + ': ' + '"' + name[1] + '"' + ' } ' + ','
              str1 = str1[:-1]  
              db_table_jsons[sink_database]['tables'].append(str0 + '{' + str1 + '}' + '}')
                  
      ##Loop through the dic object keys and create final JSOn string for the table parameters           
      for item in db_table_jsons.keys():
          joined_string = ",".join(db_table_jsons[item]['tables'])
          joined_string = json.loads('{'+joined_string+'}')
          db_table_jsons[item]['tables'] = joined_string

      #Code for getting the JOIN relationships between the tables 
      relations = []
      keys = ["dataset","table","column"]

      if len(join_relations['pairs']) == 0:
        relations = []
      else: 
        for join_pair in join_relations['pairs']:
            relations_lst = []
            tab1 = [sink_database, join_pair['tb1'], join_pair['join_key1']]
            tab2 = [sink_database, join_pair['tb2'], join_pair['join_key2']]
            relations_lst.append(dict(zip(keys, tab1)))
            relations_lst.append(dict(zip(keys, tab2)))
              
            relations.append(relations_lst)

      #Store all the previously created JSON elements in our final JSON for datamodel building, joins, and sharing
      myjson = {
          "name": model_name,
          "apikey": apikey,
          "type": "elasticube",
          "description" : "automated build by TD PS",
          "shared_users" : shared_user_list,  
          "datamodel": {
              "datasets" : db_table_jsons,
              "relations" : relations 
                          
          }
      } 

      #Finally code below sends API POST request to build the model
      r = requests.post(url = f'https://{tdserver}/reporting/datamodels', headers = {'AUTHORIZATION':'TD1 ' + apikey ,'Content-Type':'application/json'} , json = myjson)

      #Write model info to historic table
      resp = r.json()
      print(resp)
      model_dic = dict(name = [resp['name']], oid = resp['oid'], created_by = resp['created_by'],
                  updated_by = resp['updated_by'], last_updated = resp['last_updated'], last_built = resp['last_built'],
                  status = resp['status'], created_at = resp['created_at'])

      print(model_dic)

      model_df = pd.DataFrame(model_dic)

      client = pytd.Client(apikey=apikey, endpoint=tdserver, database=sink_database)
      client.load_table_from_dataframe(model_df, output_table, if_exists='append')