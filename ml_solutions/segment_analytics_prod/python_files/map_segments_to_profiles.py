import os
import sys

##-- Declare ENV Variables from YML file
apikey = os.environ['TD_API_KEY'] 
tdserver = os.environ['TD_API_SERVER']
sink_database = os.environ['SINK_DB']
output_table = os.environ['OUTPUT_TABLE']

#pip-install scan ps library
os.system(f"{sys.executable} -m pip install td-ml-map-segment-profiles")

#import all functions and variables from library
from  td_ml_map_segment_profiles import *