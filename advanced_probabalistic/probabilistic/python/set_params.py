import pandas as pd
# import pytd
import json
import digdag

def main(stg, sub, prob): 
    print(json.loads(prob))
    digdag.env.store({
                      "source_db": f"{stg}_probalistic_${sub} ",
                      "sink_database": f"cdp_unification_probabalistic_{sub}",
                      "probabalistic": f"{json.loads(prob)}",
                      "dedupe_columns" : f"{json.dumps(json.loads(prob)['dedupe_columns'])}", 
                      "blocking_table" : f"{json.loads(prob)['blocking_table']}", 
                      "max_records_allowed": f"{json.loads(prob)['max_records_allowed']}",
                      "record_limit": f"{json.loads(prob)['record_limit']}",
                      "output_suffix": f"{json.loads(prob)['output_suffix']}",
                      "output_table": f"{json.loads(prob)['output_table']}",
                      "jaccard_similarity_threshold": f"{json.loads(prob)['jaccard_similarity_threshold']}",
                      "convergence_threshold": f"{json.loads(prob)['convergence_threshold']}",
                      "cluster_threshold": f"{json.loads(prob)['cluster_threshold']}",
                      "num_block_splits": f"{json.loads(prob)['num_block_splits']}",
                      "query_engine": f"{json.loads(prob)['query_engine']}"
                      })