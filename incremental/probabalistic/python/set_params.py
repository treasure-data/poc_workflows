import pandas as pd
import pytd
import os 
import digdag

def main(canonical_id, sub): 
    print(canonical_id, sub)
    digdag.env.store({"id_col": canonical_id,
                      "source_db": f"cdp_unification_{sub}",
                      "sink_database": f"cdp_unification_probabalistic_{sub}"
                      })

