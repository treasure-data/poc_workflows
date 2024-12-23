import pandas as pd
# import pytd
import json
import digdag

def main(stg, sub, prob): 
    print(json.loads(prob))
    digdag.env.store({
                      "source_db": f"{stg}_probalistic_${sub} ",
                      "sink_database": f"cdp_unification_probabalistic_{sub}",
                      "probabalistic": f"{json.loads(prob)}"
                      })