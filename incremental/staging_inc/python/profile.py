import yaml
import digdag
import pytd.pandas_td as td
import os

def main():
    with open(r'unification/unify.yml') as file:
        documents = yaml.full_load(file)
        keys = []
        tables = []
        canonical_id = ''
        for item, doc in documents.items():
            if item == 'keys':
                keys = doc
            if item == 'tables':
                tables = doc
            if item == 'canonical_ids':
                canonical_id = doc[0]['name']

        key_list = []
        for item in keys:
            key_list.append(item['name'])
        
        key_list.append('time')
        key_list.append('ingest_time')
        base_obj = {}
        for key in key_list:
            base_obj[key] = 'null'
        
        current_table = os.environ['current_table']
        print(current_table)
        table_list = []
        for t in tables:
            if t['table'] == current_table:
                tbl = {}
                column = {**base_obj}
                name = t['table']
                tbl['name'] = name
                column['time'] = "time"
                column['ingest_time'] = "TD_TIME_PARSE(cast(CURRENT_TIMESTAMP as varchar))"
                column['src'] = f"'{current_table}'"
                print(tbl)
                for cl in t['key_columns']:
                    col = cl['column']
                    key = cl['key']
                    column[key] = col

                tbl['columns'] = column
                table_list.append(tbl)
        
        max_date = os.environ['max_date']
        time_chunk = os.environ['time_chunk']

        subque_arr = []
        for table in table_list:
            q_from = ' from ' + table['name'] 
            q_where = f" WHERE TD_TIME_RANGE(inc_unix, {max_date}, TD_TIME_ADD({max_date}, '{time_chunk}'))"
            q_columns = []
            for key, col in table['columns'].items():
                if key != 'time' and key != 'ingest_time':
                    q = f'CAST({col} as VARCHAR) as {key}'
                else: 
                    q = f'{col} as {key}'
                q_columns.append(q)
            
            query = 'select ' + ", ".join(q_columns) + ', inc_unix ' + q_from + q_where
            subque_arr.append(query)
            print(subque_arr)
        
        sql = subque_arr[0]

        digdag.env.store({'sql' : sql})
        print(sql)