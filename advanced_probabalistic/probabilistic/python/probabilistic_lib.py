import json
import os
import sys
import pandas as pd
import numpy as np
import datetime
#from python.data_loader import TDConnector


# os.system(f"{sys.executable} -m pip install td-ml-probabilistic-unification")


#from pandiet import Reducer

#####----Defining Variables-------#####
TD_SINK_DATABASE=os.environ.get('TD_SINK_DATABASE')
TD_API_KEY=os.environ.get('TD_API_KEY')
TD_API_SERVER=os.environ.get('TD_API_SERVER')

id_col=os.environ.get('id_col')
cluster_col_name=os.environ.get('cluster_col_name')
convergence_threshold=float(os.environ.get('convergence_threshold'))
cluster_threshold=float(os.environ.get('cluster_threshold'))
string_type=os.environ.get('string_type')
fill_missing=os.environ.get('fill_missing')
feature_dict=json.loads(os.environ.get('feature_dict'))
blocking_table=os.environ.get('blocking_table')
output_table=os.environ.get('output_table')

record_limit=int(os.environ.get('record_limit'))
lower_limit=int(os.environ.get('lower_limit'))
upper_limit=int(os.environ.get('upper_limit'))
range_index=os.environ.get('range_index')
paralelism = os.environ.get('paralelism')

input_table=blocking_table

from python.td_ml_probabilistic_unification import *
