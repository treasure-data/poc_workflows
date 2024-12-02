import os
import digdag
import requests

################### Reading the environment variables ###################
apikey = os.environ['TD_API_KEY']
wf_api_endpoint = os.environ['WF_API_ENDPOINT']
dest_db = os.environ['DEST_DB']
dest_tbl = os.environ['DEST_TBL']
session_id = str(os.environ['SESSION_ID'])
################### Reading the environment variables ###################


################### Intialising the global variables ###################
unif_src_tbl = os.environ['UNIF_SRC_TBL']
canonical_id_name = os.environ['CANONICAL_ID_NAME']
merge_iteration = '9999'

dest_db_tbl_extract_and_merge = f'"{dest_db}"."{dest_tbl}"'
src_db_tbl_extract_and_merge = f'"{dest_db}"."{unif_src_tbl}"'
write_result_of_extract_and_merge = f"{canonical_id_name}_graph_unify_loop_{merge_iteration}"
tbl_source_key_stats = f"{canonical_id_name}_source_key_stats"
tbl_result_key_stats = f"{canonical_id_name}_result_key_stats"

################### Intialising the global variables ###################


def main():
    try:
        print('src_db_tbl_extract_and_merge:', src_db_tbl_extract_and_merge)
        print('dest_db_tbl_extract_and_merge:', dest_db_tbl_extract_and_merge)
        print('write_result_of_extract_and_merge:', write_result_of_extract_and_merge)
        print('tbl_source_key_stats:', tbl_source_key_stats)
        print('tbl_result_key_stats:', tbl_result_key_stats)

        url = wf_api_endpoint + f'/api/sessions/{session_id}/attempts'
        resp = requests.get(url, headers={"AUTHORIZATION": 'TD1 ' + apikey})
        if resp.status_code == 200:
            # print(resp.json())
            for atmpt_dict in resp.json().get('attempts', []):
                # print(atmpt_dict)
                attempt_id = atmpt_dict.get('id')
                if attempt_id:
                    print('Getting the key_stats sql string for attempt_id: ', attempt_id)
                    sql_count = store_sql_in_digdag_variables(attempt_id)
                    if sql_count != 3:
                        print('Retry with different attempt_id if any to get the key_stats sql string...')
                    elif sql_count > 0 and sql_count < 3:
                        print('Please make sure, Unification MUST be completed successfully in single attempt_id.')
                    else:
                        return
                else:
                    print('There is no attempt for this session: ', session_id)
        else:
            print('Invalid Workflow API response. Please check the url:', url)
            raise
    except Exception as e:
        print('Something went wrong in main().', str(e))
        raise


def store_sql_in_digdag_variables(attempt_id):
    try:
        url = wf_api_endpoint + f'/api/attempts/{attempt_id}/tasks'
        print('Making Get Request using url=', url, '\n\n')
        resp = requests.get(url, headers={"AUTHORIZATION": 'TD1 ' + apikey})
        sql_count = 0
        if resp.status_code == 200:
            # print(resp.json())
            for task in resp.json().get('tasks', []):
                # print(task)
                if '+extract_and_merge' in task.get('fullName'):
                    sql_str_extract_and_merge = task.get('config', {}).get('query')
                    # pprint.pprint(task)
                    sql_str_extract_and_merge = sql_str_extract_and_merge.replace(src_db_tbl_extract_and_merge, dest_db_tbl_extract_and_merge).replace(f"{canonical_id_name}_graph_unify_loop_0", write_result_of_extract_and_merge)
                    digdag.env.store({"sql_str_extract_and_merge":sql_str_extract_and_merge})
                    digdag.env.export({'sql_str_extract_and_merge': sql_str_extract_and_merge })
                    sql_count = sql_count + 1
                    print('sql for +extract_and_merge task is extrated successfully... \n')
                elif '+source_key_stats' in task.get('fullName'):
                    sql_str_source_key_stats = task.get('config', {}).get('query')
                    sql_str_source_key_stats = sql_str_source_key_stats.replace(tbl_source_key_stats, tbl_source_key_stats + '_full').replace(f"{canonical_id_name}_graph_unify_loop_0", write_result_of_extract_and_merge)
                    digdag.env.store({"sql_str_source_key_stats":sql_str_source_key_stats})
                    digdag.env.export({'sql_str_source_key_stats': sql_str_source_key_stats })
                    print('sql for +source_key_stats task is extrated successfully... \n')
                    sql_count = sql_count + 1
                elif '+result_key_stats' in task.get('fullName'):
                    sql_str_result_key_stats = task.get('config', {}).get('query')
                    src_tbl_result_key_stats = f'''
                      ---- Below part Needs to be replace within copied query from first unification run of task +result_key_stats
                      (

                        select * from "{canonical_id_name}_graph"
                        UNION ALL -- Reconciling Graph table results from previous run & current Unification run.
                        select a.* from "{canonical_id_name}_graph_prev" a
                        inner join
                        (
                          select follower_id, follower_ns from "{canonical_id_name}_graph_prev"
                          EXCEPT
                          select follower_id, follower_ns from "{canonical_id_name}_graph"
                        ) b
                        on a.follower_id = b.follower_id and a.follower_ns = b.follower_ns
                      )
                      '''
                    sql_str_result_key_stats = sql_str_result_key_stats.replace(tbl_result_key_stats, tbl_result_key_stats + '_full').replace(f'"{canonical_id_name}_graph"', src_tbl_result_key_stats)
                    print('sql for +result_key_stats task is extrated successfully... \n')
                    digdag.env.store({"sql_str_result_key_stats":sql_str_result_key_stats})
                    digdag.env.export({'sql_str_result_key_stats': sql_str_result_key_stats })
                    sql_count = sql_count + 1
            print('sql_count=', sql_count)
        else:
            print('Invalid Workflow API response. Please check the url:', url)
            raise
        return sql_count
    except Exception as e:
        print('Something went wrong in store_sql_in_digdag_variables().', str(e))
        raise

# if __name__ == '__main__':
#     main()
