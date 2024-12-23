# Similarity Calculation Module

import pandas as pd
import numpy as np
import re
from similarity.jarowinkler import JaroWinkler
from similarity.cosine import Cosine
cosine = Cosine(2)
from fuzzywuzzy import fuzz
from similarity.normalized_levenshtein import NormalizedLevenshtein

normalized_levenshtein = NormalizedLevenshtein()
jarowinkler = JaroWinkler()


# Function to Calculate Similarity for Email
def get_email_similarity(email_1, email_2):
    """
    Calculates normalized Levenshtein distance for email similarity.
    """
    if email_1 is None or email_2 is None or email_1 == '' or email_2 == '':
        return np.nan
    email_1 = str(email_1).split('@')[0].lower().strip()
    email_2 = str(email_2).split('@')[0].lower().strip()
    return 1 - np.round(normalized_levenshtein.distance(email_1, email_2), 2)

# Function to Calculate Similarity for Phone
def get_phone_similarity(phone_1, phone_2):
    """
    Calculates normalized Levenshtein distance for phone number similarity.
    """
    if phone_1 is None or phone_2 is None or phone_1 == '' or phone_2 == '':
        return np.nan
    phone_1 = re.sub("\D", "", str(phone_1).strip())
    phone_2 = re.sub("\D", "", str(phone_2).strip())
    return 1 - np.round(normalized_levenshtein.distance(phone_1, phone_2), 2)

# Function to Calculate Cosine Similarity
def get_cosine_similarity(text_1, text_2):
    """
    Calculates cosine similarity for string similarity.
    """
    if text_1 is None or text_2 is None or text_1 == '' or text_2 == '':
        return np.nan
    text_1 = str(text_1).lower().strip()
    text_2 = str(text_2).lower().strip()
    profile_1 = cosine.get_profile(text_1)
    profile_2 = cosine.get_profile(text_2)
    return np.round(cosine.similarity_profiles(profile_1, profile_2), 2)

# Function to Calculate Jaro-Winkler Similarity
def get_jarowinkler_similarity(text_1, text_2):
    """
    Calculates Jaro-Winkler similarity for string similarity.
    """
    if text_1 is None or text_2 is None or text_1 == '' or text_2 == '':
        return np.nan
    text_1 = re.sub('[^A-Za-z0-9]+', ' ', str(text_1)).lower().strip()
    text_2 = re.sub('[^A-Za-z0-9]+', ' ', str(text_2)).lower().strip()
    return jarowinkler.similarity(text_1, text_2)

# Function to Calculate Fuzzy Token Sort Ratio Similarity
def get_fuzzy_similarity(text_1, text_2):
    """
    Calculates fuzzy token sort ratio similarity for string similarity.
    """
    if text_1 is None or text_2 is None or text_1 == '' or text_2 == '':
        return np.nan
    text_1 = re.sub('[^A-Za-z0-9]+', ' ', str(text_1)).lower().strip()
    text_2 = re.sub('[^A-Za-z0-9]+', ' ', str(text_2)).lower().strip()
    return np.round(fuzz.token_sort_ratio(text_1, text_2) / 100, 2)

# Main Function to Calculate Similarities
def get_similarities(sim_data, feature_dict, string_type='jarowinkler'):
    """
    Calculates similarities based on the specified feature dictionary and string similarity type.
    """
    sim_feat_list = []
    col_names = []
    weights = []
    try:
      for feature in feature_dict:
          name = feature['name']
          col_names.append(name)
          type = feature['type']
          weights.append(float(feature['weight']))
          sim_feat_list.append(name + '_sim')

          if type == 'email':
              sim_data[name + '_sim'] = sim_data[[name + '_2', name + '_1']].apply(lambda x: get_email_similarity(*x), axis=1)
          elif type == 'phone':
              sim_data[name + '_sim'] = sim_data[[name + '_2', name + '_1']].apply(lambda x: get_phone_similarity(*x), axis=1)
          elif type == 'string':
              if string_type == 'jarowinkler':
                  sim_data[name + '_sim'] = sim_data[[name + '_2', name + '_1']].apply(lambda x: get_jarowinkler_similarity(*x), axis=1)
              elif string_type == 'cosine':
                  sim_data[name + '_sim'] = sim_data[[name + '_2', name + '_1']].apply(lambda x: get_cosine_similarity(*x), axis=1)
              else:
                  sim_data[name + '_sim'] = sim_data[[name + '_2', name + '_1']].apply(lambda x: get_fuzzy_similarity(*x), axis=1)

      return sim_data, sim_feat_list, col_names, weights
    except Exception as e:
      print("Exception :", str(e))
      print("feature :", feature)
      print("sim_data :", sim_data)
      print('sim_data values', sim_data[[name + '_2', name + '_1']])
      raise e
