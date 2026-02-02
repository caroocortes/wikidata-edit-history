import csv
import glob
import re
from Levenshtein import distance as levenshtein_distance
import pandas as pd
import os
import json
import numpy as np
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta
from scripts.utils import get_time_feature
from sentence_transformers import SentenceTransformer, util
from sklearn.metrics.pairwise import cosine_similarity
import torch

from scripts.transitive_closure_cache import TransitiveClosureCache
from scripts.const import *
from scripts.utils import get_time_unit

class FeatureCreation():

    def __init__(self, conn):
        self.conn = conn

    def create_embedding_features(self, df, old_col, new_col):
        """
            Calculates cosine similarity between old and new value embeddings
        """
        
        old_texts = []
        new_texts = []

        old_description = []
        new_description = []

        old_label = []
        new_label = []

        for _, row in df.iterrows():
            entity = str(row['entity_label'])
            prop = str(row['property_label'])
                
            latest_description = str(row['entity_description'])
            entity_instance_of = str(row['entity_types_31']) if not pd.isna(row['entity_types_31']) else ''
            entity_subclass_of = str(row['entity_types_279']) if not pd.isna(row['entity_types_279']) else ''

            old_val = str(row[old_col]).replace('"', '') # these are the entity labels
            new_val = str(row[new_col]).replace('"', '')
            if 'label' in old_col:

                old_value_description = str(row['old_value_description']) if not pd.isna(row['old_value_description']) else ''
                new_value_description = str(row['new_value_description']) if not pd.isna(row['new_value_description']) else ''
                # entity changes -> add the entity label as context
                old_texts.append(f"Entity: {entity}, { 'Entity is instance of:'  + entity_instance_of if entity_instance_of else ''}, { 'Entity is subclass of:' + entity_subclass_of if entity_subclass_of else ''}, Entity Description: {latest_description}, Property: {prop}, Old Value: {old_val}, Old Value Description: {old_value_description}" )
                new_texts.append(f"Entity: {entity}, { 'Entity is instance of:'  + entity_instance_of if entity_instance_of else ''}, { 'Entity is subclass of:' + entity_subclass_of if entity_subclass_of else ''}, Entity Description: {latest_description}, Property: {prop}, New Value: {new_val}, New Value Description: {new_value_description}")

                # only calculate these features fro entity changes
                old_label.append(old_val) # labels
                new_label.append(new_val)

                old_description.append(old_value_description) # descriptions
                new_description.append(new_value_description)
            
            else:
                # add the property label + entity label + latest description to provide context
                old_texts.append(f"Entity: {entity}, { 'Entity is instance of:'  + entity_instance_of if entity_instance_of else ''}, { 'Entity is subclass of:' + entity_subclass_of if entity_subclass_of else ''}, Entity Description: {latest_description}, Property: {prop}, Old Value: {old_val}")
                new_texts.append(f"Entity: {entity}, { 'Entity is instance of:'  + entity_instance_of if entity_instance_of else ''}, { 'Entity is subclass of:' + entity_subclass_of if entity_subclass_of else ''}, Entity Description: {latest_description}, Property: {prop}, New Value: {new_val}")

        # load model
        model = SentenceTransformer('all-MiniLM-L6-v2')

        device = "cuda" if torch.cuda.is_available() else "cpu"

        old_text_embeddings = model.encode(
            old_texts,
            device=device,
            show_progress_bar=True,
            batch_size=512
        )
        new_text_embeddings = model.encode(
            new_texts,
            device=device,
            show_progress_bar=True,
            batch_size=512
        )
        # calculate cosine similarity
        similarities = np.array([
            cosine_similarity([old_emb], [new_emb])[0][0]
            for old_emb, new_emb in zip(old_text_embeddings, new_text_embeddings)
        ])
        df['full_cosine_similarity'] = similarities

        if 'label' in old_col:
            old_label_embeddings = model.encode(
                old_label,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            new_label_embeddings = model.encode(
                new_label,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            # calculate cosine similarity
            similarities = np.array([
                cosine_similarity([old_emb], [new_emb])[0][0]
                for old_emb, new_emb in zip(old_label_embeddings, new_label_embeddings)
            ])
            df['label_cosine_similarity'] = similarities

            old_description_embeddings = model.encode(
                old_description,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            new_description_embeddings = model.encode(
                new_description,
                device=device,
                show_progress_bar=True,
                batch_size=512
            )
            # calculate cosine similarity
            similarities = np.array([
                cosine_similarity([old_emb], [new_emb])[0][0]
                for old_emb, new_emb in zip(old_description_embeddings, new_description_embeddings)
            ])
            df['description_cosine_similarity'] = similarities 


        return df


    @staticmethod
    def has_adjacent_swap(old, new):
        """
            Check if two strings differ by an adjacent character swap
            e.g. "tent" vs "tetn" -> return 1
        """
        if len(old) != len(new):
            # different length -> there's a char addition or deletion
            return 0
        
        diffs = []
        for i in range(len(old)):
            # get charactes that differ in order
            if old[i] != new[i]:
                diffs.append(i)
            # old: caro old[2]=r old[3]=o
            # new: caor new[2]=o new[3]=r
            # diffs = [2,3]

        if len(diffs) == 2:
            i, j = diffs
            # check the difference is adjacent (j = i+1) and swapped
            if j == i + 1 and old[i] == new[j] and old[j] == new[i]:
                return 1
        return 0

    @staticmethod
    def avg_word_levenshtein(old, new):
        """
            Calculate average Levenshtein distance between words in old and new strings
        """

        old_words = old.split()
        new_words = new.split()
        total_distance = 0
        count = 0
        for o_word in old_words:
            for n_word in new_words:
                dist = levenshtein_distance(o_word, n_word)
                total_distance += dist
                count += 1
        if count == 0:
            return 0
        return total_distance / count

    ####################
    # Text features
    ####################
    @staticmethod
    def create_text_features(datatype, old_value, new_value):
        """Extract features for string datatype changes"""
        
        features = dict()

        new_value = str(new_value).strip().replace('"', '')
        old_value = str(old_value).strip().replace('"', '')

        features['length_diff_abs'] = abs(len(new_value) - len(old_value))
        features['token_count_old'] = len(old_value.split())
        features['token_count_new'] = len(new_value.split())
        
        def calc_overlap(old_value, new_value):
            old_tokens = set(old_value.split())
            new_tokens = set(new_value.split())
            if len(old_tokens | new_tokens) == 0:
                return 0
            # | is union
            # & is intersection
            return len(old_tokens & new_tokens) / len(old_tokens | new_tokens)
        
        # percentage (ratio) of token overlap
        features['token_overlap'] = calc_overlap(old_value, new_value)
        
        features['old_in_new'] = int(old_value in new_value)
        features['new_in_old'] = int(new_value in old_value)
            
        features['levenshtein_distance'] = levenshtein_distance(old_value.lower().strip(), new_value.lower().strip())

        old_len = len(old_value)
        new_len = len(new_value)
        max_len = max(old_len, new_len) if max(old_len, new_len) > 0 else 1 # replace 0's with 1 to avoid division by zero

        # percentage of how much changed
        features['edit_distance_ratio'] = features['levenshtein_distance'] / max_len
        
        if (features['token_overlap'] == 0) and (features['old_in_new'] == 0) and (features['new_in_old'] == 0):
            features['complete_replacement'] = 1
        else:
            features['complete_replacement'] = 0
        
        # for property_value_update or rewording (?), the structure similarity should be low.
        # for textual change the structure similarity should be high.
        if max(features['token_count_old'], features['token_count_new']) == 0:
            print('New Value: {}, Old Value: {}'.format(new_value, old_value))
        features['structure_similarity'] = 1 - abs(features['token_count_old'] - features['token_count_new']) / \
                                        max(features['token_count_old'], features['token_count_new'])

        # for rewording 
        # if features['token_overlap'] > 0.8 and old_value != new_value:
        #     features['high_token_overlap'] = 1
        # else:
        #     features['high_token_overlap'] = 0

        result = (
            features['length_diff_abs'],
            features['token_count_old'],
            features['token_count_new'],         
            features['token_overlap'],
            features['old_in_new'],
            features['new_in_old'], 
            features['levenshtein_distance'],
            features['edit_distance_ratio'],
            features['complete_replacement'],
            features['structure_similarity'],
        )

        if datatype == 'text': # remove for entity
        
            features['char_insertions'] =  sum(1 for c in new_value if c not in old_value)
            features['char_deletions'] = sum(1 for c in old_value if c not in new_value)

            features['adjacent_char_swap'] = FeatureCreation.has_adjacent_swap(old_value, new_value)

            features['avg_word_similarity'] = FeatureCreation.avg_word_levenshtein(old_value, new_value)

            # what os.path.commonprefix returns: paths: ['/home/User/Photos', /home/User/Videos']    commonprefix: /home/User/
            # Added that length of suffix/prefix is at least 3 to avoid short suffix/prefix (e.g. just the first letter...)
            features['has_significant_prefix'] = int(len(os.path.commonprefix([old_value, new_value])) >= 3)

            features['has_significant_suffix'] = int(len(os.path.commonprefix([old_value[::-1], new_value[::-1]])) >= 3)

            result = result + (
                features['char_insertions'],
                features['char_deletions'],
                features['adjacent_char_swap'],
                features['avg_word_similarity'],
                features['has_significant_prefix'],
                features['has_significant_suffix'],
            )
        
        return result

    ####################
    # Time features
    ####################
    @staticmethod
    def create_time_features(old_value, new_value):
        """
        Extract time change features
        """
        old_value = str(old_value).strip().replace('"', '')
        new_value = str(new_value).strip().replace('"', '')

        features = dict()

        def get_date_parts(datatime_str, option='date'):

            try:
                if option == 'date':
                    time_str_cleaned = (re.sub(r'[^0-9TZ:\-]', '', str(datatime_str))).replace('Z', '')
                    date_part = time_str_cleaned.split('T')[0]
                    
                    # Handle negative years (BC dates)
                    is_negative = date_part.startswith('-')
                    if is_negative:
                        date_part = date_part[1:]  # Remove leading '-'
                    
                    parts = date_part.split('-')
                    
                    if len(parts) < 3:
                        raise ValueError(f"Invalid date format: {datatime_str}")
                    
                    year = int(parts[0])
                    if is_negative:
                        year = -year  # Make it negative again
                    
                    month = int(parts[1])
                    day = int(parts[2])
                    return year, month, day
                elif option == 'time':
                    time_str_cleaned = (re.sub(r'[^0-9TZ:\-]', '', str(datatime_str))).replace('Z', '')
                    parts = time_str_cleaned.split('T')[1].split(':')
                    hour = int(parts[0])
                    minute = int(parts[1])
                    second = int(parts[2])
                    return hour, minute, second
            except Exception as e:
                print(f"Error parsing datetime string: {datatime_str} with option {option}: {e}")
                raise e
        
        def calc_date_diff(dt1, dt2):
            """Calculate date difference in days"""
            try:
                
                if dt1 is None or dt2 is None:
                    return 1000
                
                if 'T' not in dt1 or 'T' not in dt2:
                    # if there's somevalue or novalue
                    return 1000
                
                dt1_year, dt1_month, dt1_day = get_date_parts(dt1, option='date')
                dt2_year, dt2_month, dt2_day = get_date_parts(dt2, option='date')

                if None in (dt1_year, dt1_month, dt1_day, dt2_year, dt2_month, dt2_day):
                    return 1000
                # do it manually since WD allows 00 for month and day and Pyhton libraries don't
                diff_year = int(abs(dt2_year - dt1_year) * 365.25) # use .25 for leap years
                diff_month = int(abs(dt2_month - dt1_month) * 30.44) # average days in month
                diff_day = int(abs(dt2_day - dt1_day))

                return diff_year + diff_month + diff_day
            
            except:
                return 10000
        
        def calc_time_diff(dt1, dt2):
            """Calculate time difference in minutes"""
            try:

                if dt1 is None or dt2 is None:
                    return 1000
                
                if 'T' not in dt1 or 'T' not in dt2:
                    # if there's somevalue or novalue
                    return 1000
                
                hour1, minute1, second1 = get_date_parts(dt1, option='time')
                hour2, minute2, second2 = get_date_parts(dt2, option='time')

                if None in (hour1, minute1, second1, hour2, minute2, second2):
                    return 1000

                minute_diff = int(abs(minute2 - minute1))
                hour_diff = int(abs(hour2 - hour1) * 60) # convert to minutes
                second_diff = int(abs(second2 - second1) / 60)  # convert to minutes
                
                return hour_diff + minute_diff + second_diff
            
            except:
                return 1000

        def calc_sign_change(dt1, dt2):

            if dt1[1:] == dt2[1:]:
                return 1
            else:
                return 0
        
        def change_one_to_value(dt1, dt2):
            try:
                if dt1 is None or dt2 is None:
                    return 0
                if 'T' not in dt1 or 'T' not in dt2:
                    # for somevalue or novalue
                    return 0

                year1, month1, day1 = get_date_parts(dt1, option='date')
                year2, month2, day2 = get_date_parts(dt2, option='date')

                if ((month1 == 1 and month2 > 0) or (day1 == 1 and day2 > 0)) and year1 == year2:
                    return 1
                else:
                    return 0
            except:
                return 0

        def calc_change_zero_one(dt1, dt2, option='zero_to_one'):
            try:
                
                if dt1 is None or dt2 is None:
                    return 0
                if 'T' not in dt1 or 'T' not in dt2:
                    # for somevalue or novalue
                    return 0

                year1, month1, day1 = get_date_parts(dt1, option='date')
                year2, month2, day2 = get_date_parts(dt2, option='date')

                if option == 'zero_to_one':
                    # YYYY-00-00 -> YYYY-01-01
                    # YYYY-00-00 -> YYYY-01-00
                    # YYYY-00-00 -> YYYY-00-01
                    if ((month1 == 0 and month2 == 1 and (day2 == 0 or day2 == 1)) or (day1 == 0 and day2 == 1 and (month2 == 0 or month2 == 1))) and year1 == year2:
                        return 1
                    else:
                        return 0
                    
                elif option == 'one_to_zero':
                    # YYYY-01-01 -> YYYY-00-00
                    # YYYY-01-00 -> YYYY-00-00
                    # YYYY-00-01 -> YYYY-00-00
                    if ((month1 == 1 and month2 == 0 and (day1 == 0 or day1 == 1)) or (day1 == 1 and day2 == 0 and (month1 == 0 or month1 == 1))) and year1 == year2:
                        return 1
                    else:
                        return 0

                elif option == 'one_to_value':
                    # YYYY-01-01 -> YYYY-MM-DD (MM>1 or DD>1)
                    if ((month1 == 1 and month2 > 1) and (day1 == 1 and day2 > 1)) and year1 == year2:
                        return 1
                    else:
                        return 0
            except:
                return 0


        def added_removed_part(dt1, dt2, part='year', option='date', change_type='added'):
            try:
                if dt1 is None or dt2 is None:
                    return 0
                if 'T' not in dt1 or 'T' not in dt2:
                    # for somevalue or novalue
                    return 0

                if option == 'date':
                    year1, month1, day1 = get_date_parts(dt1, option='date')
                    year2, month2, day2 = get_date_parts(dt2, option='date')
                    if change_type == 'added':
                        if part == 'year' and year1 == 0 and year2 != 0:
                            return 1
                        if part == 'month' and ((month1 == 0 and month2 > 0) or (month1 == 1 and month2 > 1)):
                            return 1
                        if part == 'day' and ((day1 == 0 and day2 > 0) or (day1 == 1 and day2 > 1)):
                            return 1
                        return 0
                    elif change_type == 'removed':
                        if part == 'year' and year1 > 0 and year2 == 0:
                            return 1
                        if part == 'month' and month1 > 0 and month2 == 0:
                            return 1
                        if part == 'day' and day1 > 0 and day2 == 0:
                            return 1
                        return 0
            
                elif option == 'time':
                    hour1, minute1, second1 = get_date_parts(dt1, option='time')
                    hour2, minute2, second2 = get_date_parts(dt2, option='time')
                    if change_type == 'added':
                        if part == 'hour' and hour1 == 0 and hour2 != 0:
                            return 1
                        if part == 'minute' and minute1 == 0 and minute2 != 0:
                            return 1
                        if part == 'second' and second1 == 0 and second2 != 0:
                            return 1
                        return 0
                    elif change_type == 'removed':
                        if part == 'hour' and hour1 != 0 and hour2 == 0:
                            return 1
                        if part == 'minute' and minute1 != 0 and minute2 == 0:
                            return 1
                        if part == 'second' and second1 != 0 and second2 == 0:
                            return 1
                        return 0
            except:
                return 0

        features['date_diff_days'] = calc_date_diff(old_value, new_value)
        features['time_diff_minutes'] = calc_time_diff(old_value, new_value)
        features['sign_change'] = calc_sign_change(old_value, new_value)
        features['change_one_to_zero'] = calc_change_zero_one(old_value, new_value, option='one_to_zero')
        features['change_one_to_value'] = calc_change_zero_one(old_value, new_value, option='one_to_value')
        features['change_zero_to_one'] = calc_change_zero_one(old_value, new_value, option='zero_to_one')
        features['day_added'] = added_removed_part(old_value, new_value, part='day', option='date', change_type='added')
        features['day_removed'] = added_removed_part(old_value, new_value, part='day', option='date', change_type='removed')
        features['month_added'] = added_removed_part(old_value, new_value, part='month', option='date', change_type='added')
        features['month_removed'] = added_removed_part(old_value, new_value, part='month', option='date', change_type='removed')

        year1, month1, day1 = get_date_parts(old_value, option='date')
        year2, month2, day2 = get_date_parts(new_value, option='date')

        features['different_year'] = 1 if year1 != year2 else 0
        features['different_day'] = 1 if day1 != day2 else 0
        features['different_month'] = 1 if month1 != month2 else 0

        # removed for now due to shapley value analysis
        # features['day_added'] = added_removed_part(old_value, new_value, part='day', option='date', change_type='added')
        # features['day_removed'] = added_removed_part(old_value, new_value, part='day', option='date', change_type='removed')
        # features['hour_added'] = added_removed_part(old_value, new_value, part='hour', option='time', change_type='added')
        # features['hour_removed'] = added_removed_part(old_value, new_value, part='hour', option='time', change_type='removed')
        # features['minute_added'] = added_removed_part(old_value, new_value, part='minute', option='time', change_type='added')
        # features['minute_removed'] = added_removed_part(old_value, new_value, part='minute', option='time', change_type='removed')
        # features['second_added'] = added_removed_part(old_value, new_value, part='second', option='time', change_type='added')
        # features['second_removed'] = added_removed_part(old_value, new_value, part='second', option='time', change_type='removed')
        # features['change_one_to_value'] = change_one_to_value(old_value, new_value)

        result = (
            features['date_diff_days'],
            features['time_diff_minutes'],
            features['sign_change'],
            features['change_one_to_zero'],
            features['change_one_to_value'],
            features['change_zero_to_one'],
            features['day_added'],
            features['day_removed'],
            features['month_added'],
            features['month_removed'],
            features['different_year'],
            features['different_day'],
            features['different_month'],
        )

        return result
    
    ####################
    # Quantity features
    ####################
    @staticmethod
    def calc_precision_change(old_value, new_value, datatype='quantity', part=None):
        # returns 1 if only precision (decimal places) changed, 0 otherwise
        # ndp == non decimal part
        # dp == decimal part
        if datatype == 'globecoordinate':
            if '{' in old_value and '{' in new_value:
                old = json.loads(old_value).get(part, None)
                new = json.loads(new_value).get(part, None)
                
                old_ndp = str(old).split('.')[0] if '.' in str(old) else str(old)
                old_dp = str(old).split('.')[1] if '.' in str(old) else 0

                new_ndp = str(new).split('.')[0] if '.' in str(new) else str(new)
                new_dp = str(new).split('.')[1] if '.' in str(new) else 0
            else:
                return 0
        else:

            # quantity
            old_ndp = str(old_value).split('.')[0] if '.' in str(old_value) else str(old_value)
            old_dp = str(old_value).split('.')[1] if '.' in str(old_value) else 0

            new_ndp = str(new_value).split('.')[0] if '.' in str(new_value) else str(new_value)
            new_dp = str(new_value).split('.')[1] if '.' in str(new_value) else 0

        if old_ndp == new_ndp and old_dp != new_dp:
            return 1
        else:
            return 0

    @staticmethod
    def calc_precision_added_removed(old_value, new_value, option='added', datatype='quantity', part=None):
        """
            Returns 1 if precision was added or removed, 0 otherwise
            NOTE: we only check that the precision was added/removed, not that it increased/decreased
        """
        if datatype == 'quantity':
            
            new = str(new_value)
            old = str(old_value)

        else: # globecoordinate
            if '{' in old_value and '{' in new_value:
                old = str(json.loads(old_value).get(part, '')) # part is longitude or latitude
                new = str(json.loads(new_value).get(part, ''))
            else:
                return 0
            
        new_first_part = new.split('.')[0]
        old_first_part = old.split('.')[0]
        if new_first_part != old_first_part:
            return 0  # different values, not just precision change

        if option == 'added':
            return 1 if ('.' in new) and ('.' not in old) else 0
        else: # removed
            return 1 if ('.' not in new) and ('.' in old) else 0

    @staticmethod
    def calc_length_increase_decrease(old_value, new_value, datatype='quantity', option='increase', part=None):

        if datatype == 'quantity':
            new_length = len(str(new_value).replace('-', '').replace('+', ''))
            old_length = len(str(old_value).replace('-', '').replace('+', ''))
        else: # globecoordinate
            if '{' in old_value and '{' in new_value: # for somevalue or novalue
                old = str(json.loads(old_value).get(part, '')) # part is longitude or latitude
                new = str(json.loads(new_value).get(part, ''))
                new_length = len(new.replace('-', '').replace('+', ''))
                old_length = len(old.replace('-', '').replace('+', ''))
            else:
                return 0
        
        if option == 'increase':
            return 1 if new_length > old_length else 0
        else: # decrease
            return 1 if new_length < old_length else 0
    
    @staticmethod
    def calc_sign_change(old_value, new_value, datatype='quantity', part=None):

        if datatype == 'quantity':
            new_float = float(new_value)
            old_float = float(old_value)
        else: # globecoordinate
            if '{' in old_value and '{' in new_value: # for somevalue or novalue
                old = str(json.loads(old_value).get(part, '')) # part is longitude or latitude
                new = str(json.loads(new_value).get(part, ''))
                new_float = float(new)
                old_float = float(old)
            else:
                return 0
        return 1 if (old_float * new_float < 0) else 0

    @staticmethod
    def calc_relative_value_diff_abs(old_value, new_value, datatype='quantity', part=None):

        if datatype == 'quantity':
            new_float = float(new_value)
            old_float = float(old_value)
        else: # globecoordinate
            if '{' in old_value and '{' in new_value: # for somevalue or novalue
                old = str(json.loads(old_value).get(part, '')) # part is longitude or latitude
                new = str(json.loads(new_value).get(part, ''))
                new_float = float(new)
                old_float = float(old)
            else:
                return 0
        return abs((new_float - old_float) / (old_float if old_float != 0 else 1))

    @staticmethod
    def shares_precision_prefix(old_value, new_value, datatype='quantity', part=None):
        if datatype == 'globecoordinate':
            if '{' in old_value and '{' in new_value:
                old = json.loads(old_value).get(part, None)
                new = json.loads(new_value).get(part, None)
                
                old_dp = str(old).split('.')[1] if '.' in str(old) else 0
                new_dp = str(new).split('.')[1] if '.' in str(new) else 0
            else:
                return 0, 0
        else:
            
            # quantity
            old_dp = str(old_value).split('.')[1] if '.' in str(old_value) else 0
            new_dp = str(new_value).split('.')[1] if '.' in str(new_value) else 0
        
        if old_dp == 0 or new_dp == 0: # therew's no decimal part
            return 0, 0
        
        old_decimal_parts = list(str(old_dp))
        new_decimal_parts = list(str(new_dp))

        shared_numbers = 0
        for old_part, new_part in zip(old_decimal_parts, new_decimal_parts):
            if old_part == new_part:
                shared_numbers += 1
            else:
                break

        if shared_numbers == 0:
            return 0, shared_numbers
        else:
            return 1, shared_numbers

    @staticmethod
    def create_quantity_features(old_value, new_value):
        features = dict()

        new_value = str(new_value).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()
        old_value = str(old_value).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()
        
        # remove + sign
        old_str = str(old_value).replace('"', '').replace('+', '').strip()
        new_str = str(new_value).replace('"', '').replace('+', '').strip()

        # relative value diff
        # relative value difernece -> proportional change relative to the old_value
        features['relative_value_diff_abs'] = FeatureCreation.calc_relative_value_diff_abs(old_str, new_str, datatype='quantity')
        
        # sign change 
        features['sign_change'] = FeatureCreation.calc_sign_change(old_str, new_str, datatype='quantity')
        
        # only precision change 
        features['precision_change'] = FeatureCreation.calc_precision_change(old_str, new_str, datatype='quantity')
        
        # precision added/removed
        features['precision_added'] = FeatureCreation.calc_precision_added_removed(old_str, new_str, 'added', 'quantity')
        features['precision_removed'] = FeatureCreation.calc_precision_added_removed(old_str, new_str, 'removed', 'quantity')

        features['length_increase'] = FeatureCreation.calc_length_increase_decrease(old_str, new_str, datatype='quantity', option='increase')
        features['length_decrease'] = FeatureCreation.calc_length_increase_decrease(old_str, new_str, datatype='quantity', option='decrease')

        features['whole_number_change'] = int(np.floor(abs(float(old_str))) != np.floor(abs(float(new_str))))

        features['shared_prefix'], features['shared_prefix_length'] = FeatureCreation.shares_precision_prefix(old_str, new_str, datatype='quantity')

        result = (
            features['sign_change'],
            features['precision_change'],
            features['precision_added'],
            features['precision_removed'],
            features['length_increase'],
            features['length_decrease'],
            features['whole_number_change'],
            features['shared_prefix'],
            features['shared_prefix_length'],
        )

        return result


    ####################
    # Globecoordinate features
    ####################
    @staticmethod
    def create_globe_coordinate_features(old_value, new_value):

        features = dict()
        old_val = json.loads(old_value)
        new_val = json.loads(new_value)

        new_val['latitude'] = float(str(new_val['latitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip())
        new_val['longitude'] = float(str(new_val['longitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip())

        features['relative_value_diff_latitude'] = abs(
            (new_val['latitude'] - old_val['latitude']) / (old_val['latitude'] if old_val['latitude'] != 0 else 1)
        )

        features['relative_value_diff_longitude'] = abs(
            (new_val['longitude'] - old_val['longitude']) / (old_val['longitude'] if old_val['longitude'] != 0 else 1)
        )
        
        # features['latitude_diff_abs'] = abs(new_val['latitude'] - old_val['latitude'])
        # features['longitude_diff_abs'] = abs(new_val['longitude'] - old_val['longitude'])
        
        features['latitude_sign_change'] = int((float(new_val['latitude']) * float(old_val['latitude']) < 0))
        features['longitude_sign_change'] = int((float(new_val['longitude']) * float(old_val['longitude']) < 0))

        # add abs because if there's a negative value then they will be different even though the whole number is the same
        features['latitude_whole_number_change'] = (1 if math.floor(abs(new_val['latitude'])) != math.floor(abs(old_val['latitude'])) else 0)
        features['longitude_whole_number_change'] = (1 if math.floor(abs(new_val['longitude'])) != math.floor(abs(old_val['longitude'])) else 0)

        # distance with haversine formula
        from math import radians, sin, cos, sqrt, atan2
        
        lat1, lon1 = radians(old_val['latitude']), radians(old_val['longitude'])
        lat2, lon2 = radians(new_val['latitude']), radians(new_val['longitude'])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        a = max(0.0, min(1.0, a))
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        distance_km = 6371 * c  # Earth radius in km
        
        features['coordinate_distance_km'] = distance_km

        features['latitude_precision_change'] = FeatureCreation.calc_precision_change(old_value, new_value, datatype='globecoordinate', part='latitude')
        features['longitude_precision_change'] = FeatureCreation.calc_precision_change(old_value, new_value, datatype='globecoordinate', part='longitude')
        
        # features['latitude_precision_added'] = FeatureCreation.calc_precision_added_removed(new_value, old_value, 'added', 'globecoordinate', 'latitude')
        # features['latitude_precision_removed'] = FeatureCreation.calc_precision_added_removed(new_value, old_value, 'removed', 'globecoordinate', 'latitude')
        
        # features['longitude_precision_added'] = FeatureCreation.calc_precision_added_removed(new_value, old_value, 'added', 'globecoordinate', 'longitude')
        # features['longitude_precision_removed'] = FeatureCreation.calc_precision_added_removed(new_value, old_value, 'removed', 'globecoordinate', 'longitude')

        features['latitude_length_increase'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='increase', part='latitude')
        features['latitude_length_decrease'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='decrease', part='latitude')
        
        features['longitude_length_increase'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='increase', part='longitude')
        features['longitude_length_decrease'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='decrease', part='longitude')
        
        features['longitude_shared_prefix'], features['longitude_shared_prefix_length'] = FeatureCreation.shares_precision_prefix(old_value, new_value, datatype='globecoordinate', part='longitude')
        features['latitude_shared_prefix'], features['latitude_shared_prefix_length'] = FeatureCreation.shares_precision_prefix(old_value, new_value, datatype='globecoordinate', part='latitude')

        result = (
            features['relative_value_diff_latitude'],
            features['relative_value_diff_longitude'],
            features['latitude_sign_change'], 
            features['longitude_sign_change'],
            features['latitude_whole_number_change'], 
            features['longitude_whole_number_change'], 
            features['coordinate_distance_km'],
            features['latitude_precision_change'], 
            features['longitude_precision_change'], 
            features['latitude_length_increase'], 
            features['latitude_length_decrease'], 
            features['longitude_length_increase'], 
            features['longitude_length_decrease'], 
            features['longitude_shared_prefix'], 
            features['latitude_shared_prefix'],
            features['longitude_shared_prefix_length'],
            features['latitude_shared_prefix_length'],
        )

        return result

    ####################
    # Entity features
    ####################
    def transitive_closure_check(self, value1, value2, table_name):
        """
            Check if value2 is in the transitive closure of value1 in the given table
            e.g. check if new_value is subclass of old_value
        """
        start_time = time.time()
        query = """
            SELECT 1
            FROM {table}
            WHERE entity_id = {value1} AND transitive_closure_qids LIKE {value2}
            LIMIT 1
        """.format(table=table_name, value1=value1, value2=f'%{value2}%')

        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            elapsed = time.time() - start_time
            if result:
                print(f'Transitive closure check for {value1}, {value2}, {table_name} in {elapsed:.2f} seconds')
                return 1
            else:
                print(f'Transitive closure check for {value1}, {value2}, {table_name} in {elapsed:.2f} seconds')
                return 0
            
            
    def extract_entity_p31(self, entity_id):
        """Extract entity p31 values from the database given the entity ID"""
        
        query = """
            SELECT type_labels_list
            FROM p31_entity_types
            WHERE entity_numeric_id = %s
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            start_time = time.time()
            cur.execute(query, (entity_id,))
            result = cur.fetchone()
            elapsed = time.time() - start_time
            if result:
                return result[0]
            else:
                return ''
            

    def extract_entity_p279(self, entity_id):
        """Extract entity p279 values from the database given the entity ID"""
        
        query = """
            SELECT type_labels_list
            FROM p279_entity_types
            WHERE entity_numeric_id = %s
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            start_time = time.time()
            cur.execute(query, (entity_id,))
            result = cur.fetchone()
            elapsed = time.time() - start_time
            if result:
                return result[0]
            else:
                return ''


    def get_entity_label_alias_description(self, entity_id):
        """Get entity metadata with Redis caching"""
        
        if not self.redis_client:
            # query directly
            return self._query_entity_label_alias_description(entity_id)
        
        cache_key = f'entity:{entity_id}'
        
        # Try Redis cache
        try:
            cached = self.redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            print(f"Redis error: {e}, falling back to DB")
        
        metadata = self._query_entity_label_alias_description(entity_id)
        
        # Store in Redis (with TTL to prevent stale data)
        try:
            self.redis_client.setex(
                cache_key,
                432000,  # 5 day TTL
                json.dumps(metadata)
            )
        except Exception as e:
            print(f"Redis set error: {e}")
        
        return metadata
    
    def _query_entity_label_alias_description(self, entity_id):
        """Query database for entity metadata"""
        query = """
            SELECT label, alias, description
            FROM entity_labels_alias_description
            WHERE qid = %s
            LIMIT 1
        """
        with self.connection.cursor() as cur:
            cur.execute(query, (entity_id,))
            result = cur.fetchone()
            
            if result:
                return {
                    'label': result[0] or '', 'alias': result[1] or '', 'description': result[2] or ''
                }
        
        return {'label': '', 'alias': '', 'description': ''}
    
    @staticmethod
    def create_entity_features_text_transitive(row, transitive_cache):
        """Extract features for entity datatypes using labels"""

        old_value_label = row['old_value_label']
        new_value_label = row['new_value_label']

        features_tuple = FeatureCreation.create_text_features('entity', old_value_label, new_value_label)

        text_features_dict = {
            'length_diff_abs': features_tuple[0],
            'token_count_old': features_tuple[1],
            'token_count_new': features_tuple[2],         
            'token_overlap': features_tuple[3],
            'old_in_new': features_tuple[4],
            'new_in_old': features_tuple[5], 
            'levenshtein_distance': features_tuple[6],
            'edit_distance_ratio': features_tuple[7],
            'complete_replacement': features_tuple[8],
            'structure_similarity': features_tuple[9],
        }

        features = dict()

        new_value = row['new_value']
        old_value = row['old_value']

        features['old_value_subclass_new_value'] = transitive_cache.check(old_value, new_value, 'subclass_transitive')
        features['new_value_subclass_old_value'] = transitive_cache.check(new_value, old_value, 'subclass_transitive')

        features['old_value_located_in_new_value'] = transitive_cache.check(old_value, new_value, 'located_in_transitive')
        features['new_value_located_in_old_value'] = transitive_cache.check(new_value, old_value, 'located_in_transitive')
        
        features['old_value_has_parts_new_value'] = transitive_cache.check(old_value, new_value, 'has_part_transitive')
        features['new_value_has_parts_old_value'] = transitive_cache.check(new_value, old_value, 'has_part_transitive')
        
        features['old_value_part_of_new_value'] = transitive_cache.check(old_value, new_value, 'part_of_transitive')
        features['new_value_part_of_old_value'] = transitive_cache.check(new_value, old_value, 'part_of_transitive')

        result = {**text_features_dict, **features, 'label_cosine_similarity': 0.0, 'description_cosine_similarity': 0.0, 'full_cosine_similarity': 0.0}

        return pd.Series(result, index=ENTITY_ONLY_FEATURES_COLS)
    
    def create_entity_features(self, old_value, new_value):

        old_value = str(old_value).strip().replace('"', '')
        new_value = str(new_value).strip().replace('"', '')

        old_value_label = ''
        new_value_label = ''
        old_value_description = ''
        new_value_description = ''

        features_tuple = (
            None,
            None,
            None,         
            None,
            None,
            None, 
            None,
            None,
            None,
            None,
        )

        features = dict()

        features['new_value_part_of_old_value'] = 0
        features['old_value_part_of_new_value'] = 0

        features['new_value_subclass_old_value'] = 0
        features['old_value_subclass_new_value'] = 0

        features['new_value_has_parts_old_value'] = 0
        features['old_value_has_parts_new_value'] = 0

        features['new_value_located_in_old_value'] = 0
        features['old_value_located_in_new_value'] = 0


        result = features_tuple + (
            features['old_value_subclass_new_value'], 
            features['new_value_subclass_old_value'],

            features['old_value_located_in_new_value'],
            features['new_value_located_in_old_value'],
            features['old_value_has_parts_new_value'],
            features['new_value_has_parts_old_value'],

            features['old_value_part_of_new_value'],
            features['new_value_part_of_old_value'],

            old_value_label,
            new_value_label, 
            old_value_description, 
            new_value_description,
        )
        
        return result

    ####################
    # Reverted edit features
    ####################
    @staticmethod
    def has_revert_keyword(next_changes):
        """Check if any of next changes have revert keyword in comment"""
        for change in next_changes:
            comment = str(change.get('comment', '')).lower()
            if any(keyword in comment for keyword in RV_KEYWORDS):
                return 1
        return 0

    @staticmethod
    def check_hash_revert_old(current_change, next_changes):
        """Check for hash reversion in next 10 changes"""
        curr_old_hash = current_change.get('old_hash', '')
        curr_new_hash = current_change.get('new_hash', '')
        
        for next_change in next_changes:
            next_old_hash = next_change.get('old_hash', '')
            next_new_hash = next_change.get('new_hash', '')

            # delete + update
            if (curr_old_hash == next_new_hash and curr_old_hash != '' and next_new_hash != '') or (curr_old_hash == '' and next_new_hash == '' and curr_new_hash == next_old_hash): # create
                return 1

            # else: # hay algún cambio que tenga una keyword y que no cumpla lo de los hashes??
            #     comment = str(next_change.get('comment', '')).lower()
            #     if any(keyword in comment for keyword in RV_KEYWORDS):
            #         return 1
        
        return 0

    @staticmethod
    def check_hash_revert(current_change, next_change, property_id):
        """Check for hash reversion in next 10 changes"""

        if property_id == -1 or property_id == -2: # label or description changes -> we don't store a hash, so use value comparison

            curr_old_hash = str(current_change.get('old_value', '')).strip() if current_change.get('old_value', '') != '{}' else ''
            curr_new_hash = str(current_change.get('new_value', '')).strip() if current_change.get('new_value', '') != '{}' else ''

            next_old_hash = str(next_change.get('old_value', '')).strip() if next_change.get('old_value', '') != '{}' else ''
            next_new_hash = str(next_change.get('new_value', '')).strip() if next_change.get('new_value', '') != '{}' else ''
        else:

            curr_old_hash = current_change.get('old_hash', '')
            curr_new_hash = current_change.get('new_hash', '')
            
            next_old_hash = next_change.get('old_hash', '')
            next_new_hash = next_change.get('new_hash', '')

        next_comment = str(next_change.get('comment', '')).lower()
        next_timestamp = datetime.strptime(next_change['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
        current_timestamp = datetime.strptime(current_change['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")

        # delete + update
        # create
        # has rv comment or within a week
        if ((curr_old_hash == next_new_hash and curr_old_hash != '' and next_new_hash != '') or \
            (curr_old_hash == '' and next_new_hash == '' and curr_new_hash == next_old_hash)) and \
            (any(keyword in next_comment for keyword in RV_KEYWORDS) or (next_timestamp - current_timestamp).total_seconds() <= 604800): 

            return 1
        
        return 0
    
    @staticmethod
    def finalize_changes_with_temporal_features(changes_pv_dict):
        """
        Add temporal features to changes after all changes are collected
        """

        reverted_edit_features = []
        
        # process changes_by_epv and update self.changes
        for (property_id, value_id), changes in changes_pv_dict.items():
            changes.sort(key=lambda x: x['timestamp'])
            entity_user_history = defaultdict(list)
            
            for i, change_info in enumerate(changes):
                current_user = change_info['user_id']
                current_ts = datetime.strptime(change_info['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
 
                # -------------------------
                # FEATURE : time to prev / next
                # -------------------------
                if i > 0:
                    prev_timestamp = datetime.strptime(changes[i-1]['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
                    time_to_prev = (current_ts - prev_timestamp).total_seconds()
                else:
                    time_to_prev = -1 # be careful with this in model training
                
                if i < len(changes) - 1:
                    next_timestamp = datetime.strptime(changes[i+1]['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
                    time_to_next = (next_timestamp - current_ts).total_seconds()
                else:
                    time_to_next = -1 # be careful with this in model training
                
                # Check next 10 for reverts
                if len(changes) < 10 :
                    next_10 = changes[i+1:]
                else:
                    next_10 = changes[i+1:i+11]

                # -------------------------
                # FEATURE : rv keyword in comment in any of the next 10 changes
                # -------------------------
                rv_keyword_next_10 = FeatureCreation.has_revert_keyword(next_10)

                # -------------------------
                # FEATURE : is_reverted_within_a_day
                # -------------------------
                is_reverted_within_day = 0
                next_changes = changes[i+1:]

                for future_change in next_changes:
                    future_timestamp = datetime.strptime(future_change['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
                    if (future_timestamp - current_ts).total_seconds() > 86400: # more than a day
                        break

                    if (
                        FeatureCreation.check_hash_revert_old(change_info, [future_change], property_id=property_id)
                    ):
                        is_reverted_within_day = 1
                        break
                
                # -------------------------
                # FEATURE: num_changes_by_same_user_on_entity_last_24h
                # -------------------------
                window_start = current_ts - timedelta(hours=24)

                # keep only last 24h
                entity_user_history[current_user] = [
                    ts for ts in entity_user_history[current_user]
                    if ts >= window_start # dont have to parse because im saving current_ts and its already a datetime object
                ]

                num_changes_same_user_last_24h = len(
                    entity_user_history[current_user]
                )

                # record current change
                entity_user_history[current_user].append(current_ts)

                # -------------------------
                # FEATURE : hash reversion in any of the next 10 changes
                # -------------------------
                hash_reverted_next_10 = FeatureCreation.check_hash_revert_old(change_info, next_10, property_id=property_id)
                
                # BASIC FEATURES
                is_weekend =  1 if current_ts.weekday() > 4 else 0
                day_of_week = current_ts.strftime("%A")
                hour_of_day = current_ts.hour   
                
                action_encoded = ACTION_ENCODING.get(change_info.get('action', ''), 0)
                user_type_encoded = USER_TYPE_ENCODING.get(change_info.get('user_type', ''), 0)
                day_of_week_encoded = DAY_OF_WEEK_ENCODING.get(day_of_week, 0)

                features = (
                    change_info['revision_id'],
                    property_id,
                    value_id,
                    change_info['change_target'],

                    change_info['new_datatype'],
                    change_info['old_datatype'],
                    change_info['action'],

                    user_type_encoded,
                    day_of_week_encoded,
                    hour_of_day,
                    is_weekend,
                    action_encoded, 
                    is_reverted_within_day, 
                    num_changes_same_user_last_24h,
                    rv_keyword_next_10, 
                    hash_reverted_next_10, 
                    time_to_prev, 
                    time_to_next,

                    '', #label
                )

                reverted_edit_features.append(features)

        return reverted_edit_features
    
    @staticmethod
    def tag_reverted_edits(changes_pv_dict, value_changes, entity_stats):
        """
        Tag reverted edits
        """

        def get_key_from_change(change_info, property_id=None, value_id=None):

            if isinstance(change_info, tuple):
                # from value_change I get tuples
                revision_id = change_info[0]
                property_id = change_info[1]
                value_id = change_info[3]
                change_target = change_info[8]
            elif isinstance(change_info, dict):
                # from changes_pv_dict I get dicts
                revision_id = change_info['revision_id']
                property_id = property_id
                value_id = value_id
                change_target = change_info['change_target']

            key = (revision_id, property_id, value_id, change_target)

            return key

        def update_revert_stats(change):
            """
            Helper function to update revert statistics for a change
            """

            # Update reverted edits count
            action = change['action']
            
            # Return counts for entity-level stats
            counts = {
                'total': 1, # reverted edits
                'create': 1 if action == 'CREATE' else 0,
                'delete': 1 if action == 'DELETE' else 0,
                'update': 1 if action == 'UPDATE' else 0
            }
            return counts

        # create dict of changes: key -> original tuple for quick update
        dict_lookup = dict()
        
        for change in value_changes:
            key = get_key_from_change(change)
            dict_lookup[key] = change
        
        # track reverts
        revert_flags = {} 
        num_reverted_edits = 0
        num_reversions = 0
        num_reverted_edits_create = 0
        num_reverted_edits_delete = 0
        num_reverted_edits_update = 0
        
        # process changes_by_epv and determine revert status
        for (property_id, value_id), changes in changes_pv_dict.items():
            changes.sort(key=lambda x: x['timestamp'])
            
            for i, change_info in enumerate(changes):
                key = get_key_from_change(change_info, property_id, value_id)
                
                if key not in revert_flags:
                    revert_flags[key] = (0, 0)  # (is_reverted, reversion)
                
                next_changes = changes[i+1:]

                for j, future_change in enumerate(next_changes):
                    reverted = FeatureCreation.check_hash_revert(change_info, future_change, property_id=property_id)
                    
                    if reverted == 1:
                        # mark current edit as reverted
                        revert_flags[key] = (1, 0)
                        
                        # mark future change as reversion
                        future_key = get_key_from_change(future_change, property_id, value_id)
                        revert_flags[future_key] = (0, 1)

                        # Update stats for the original reverted change
                        counts = update_revert_stats(change_info)
                        
                        num_reverted_edits += counts['total']
                        num_reverted_edits_create += counts['create']
                        num_reverted_edits_delete += counts['delete']
                        num_reverted_edits_update += counts['update']
                        
                        # Update stats for the reversion (future_change counted as reversion only)
                        num_reversions += 1

                        # mark changes in between as reverted (these are also reverted edits!)
                        for inter_change in next_changes[:j]:
                            inter_key = get_key_from_change(inter_change, property_id, value_id)
                            revert_flags[inter_key] = (1, 0)
                            
                            # Update stats for intermediate changes
                            counts = update_revert_stats(inter_change)
                            
                            num_reverted_edits += counts['total']
                            num_reverted_edits_create += counts['create']
                            num_reverted_edits_delete += counts['delete']
                            num_reverted_edits_update += counts['update']
                        
                        break  # Found revert, move to next change
        
        final_value_changes = []
        
        for key, original_tuple in dict_lookup.items():

            if key[3] == 'rank':
                # need to get corresponding value change for rank
                value_key = (key[0], key[1], key[2], '')
                is_reverted, reversion = revert_flags.get(value_key, (0, 0))
            else:
                is_reverted, reversion = revert_flags.get(key, (0, 0))

            updated_tuple = original_tuple + (is_reverted, reversion)
            
            final_value_changes.append(updated_tuple)

        entity_stats['num_reverted_edits'] = num_reverted_edits
        entity_stats['num_reversions'] = num_reversions
        entity_stats['num_reverted_edits_create'] = num_reverted_edits_create
        entity_stats['num_reverted_edits_delete'] = num_reverted_edits_delete
        entity_stats['num_reverted_edits_update'] = num_reverted_edits_update

        return final_value_changes, entity_stats
    

    ####################
    # Property replacement features
    ####################
    @staticmethod
    def create_property_replacement_features(c1, c2):
        """Create features for property replacements"""

        # Save pair of create/delete where the value is the same and the property is different
        # for each pair, save: user_id, property_label (need to keep track of this for text similarity), timestamp, action
        
        if c1['action'] == 'DELETE':
            delete_change = c1
            create_change = c2
        else:
            delete_change = c2
            create_change = c1

        if c1['user_id'] == c2['user_id']:
            same_user = 1
        else:
            same_user = 0

        create_change_dt = datetime.strptime(create_change['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
        delete_change_dt = datetime.strptime(delete_change['timestamp'].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
        time_diff = (delete_change_dt - create_change_dt).total_seconds() 

        same_day = int(create_change_dt.date() == delete_change_dt.date())
        same_hour = int(create_change_dt.hour == delete_change_dt.hour)
        same_revision = int(delete_change['revision_id'] == create_change['revision_id'])
        delete_before_create = int(delete_change_dt < create_change_dt)

        features = (
            
            delete_change['revision_id'],
            delete_change['property_id'],
            delete_change['value_id'],
            delete_change['change_target'],

            create_change['revision_id'],
            create_change['property_id'],
            create_change['value_id'],
            create_change['change_target'],
            
            time_diff,
            same_day,
            same_hour,
            same_revision,
            delete_before_create,
            same_user,
            0.0, # property label similarity

            delete_change['timestamp'],
            create_change['timestamp'],

            delete_change['property_label'],
            create_change['property_label'],
            delete_change['user_id'],
            create_change['user_id'],
            
            '',
        )

        return features

    
    ####################################################################################################
    # Update of features that weren't calculated during parsinf of the file
    ####################################################################################################
    def create_and_update_embedding_features(self, datatype, key_cols, select_cols, embedding_cols, table_prefix, batch_size=100000, max_batches=None):
        """
        Creates and updates embedding features for the given datatype. Processes in batches.
        
        :param datatype: datatype to calculate features for
        :param key_cols: key columns of the feature tables
        :param embedding_cols: columns of embedding features
        :param table_prefix: can be _sa, _ao, _less or ''
        :param batch_size: number of rows to process in each batch
        :param max_batches: maximum number of batches to process (optional)
        """
        print('Creating embedding features for datatype:', datatype, flush=True)
        main_start_time = time.time()

        if not self.conn:
            print('No DB connection available', flush=True)
            return

        base_query = """
            SELECT {key_cols_str}, {select_cols_str}, {embedding_cols_str}
                FROM features_{datatype}{table_prefix}
                WHERE 
                    (label IS NULL or label = '') AND 
                    ({embedding_cols_str_filter}) AND 
                    processed = FALSE
                LIMIT {batch_size} OFFSET {offset}
        """
        
        offset = 0
        num_batches = 0

        os.makedirs(f'{DATA_PATH}/{datatype}{table_prefix}', exist_ok=True)
        
        embedding_cols_str = ', '.join(embedding_cols)
        key_cols_str = ', '.join(key_cols)
        select_cols_str = ', '.join(select_cols)
        
        if datatype in ('time', 'quantity', 'text', 'globecoordinate'):
            key_cols_temp = ', '.join([f'{col} {col_type}' for col, col_type in BASE_KEY_TYPES.items()])
        else:
            key_cols_temp = ', '.join([f'{col} {col_type}' for col, col_type in PROP_REP_KEY_TYPES.items()])

        cursor = self.conn.cursor()
        cursor.execute(f"CREATE TEMP TABLE temp_results_{datatype}{table_prefix} ({key_cols_temp}, {', '.join([f'{col} FLOAT' for col in embedding_cols])})")

        while True:

            if max_batches and num_batches >= max_batches:
                print(f'Reached max_batches limit ({max_batches}), stopping', flush=True)
                break
            
            if not os.path.exists(f'{DATA_PATH}/{datatype}{table_prefix}/chunk_{offset//batch_size}.csv'):

                query = base_query.format(
                    key_cols_str = key_cols_str,
                    select_cols_str=select_cols_str,
                    embedding_cols_str=embedding_cols_str,
                    embedding_cols_str_filter = f' OR '.join([f'({col} IS NULL OR {col} = 0.0)' for col in embedding_cols]),
                    datatype=datatype,
                    table_prefix=table_prefix,
                    batch_size=batch_size,
                    offset=offset
                )
            
                df = pd.read_sql(query, self.conn)
                
                if len(df) == 0:
                    break

                df.to_csv(f'{DATA_PATH}/{datatype}{table_prefix}/chunk_{offset//batch_size}.csv', index=False, sep=';', quoting=csv.QUOTE_NONE, escapechar='\\')
            
                result = self.create_embedding_features(df, old_col='old_value', new_col='new_value')
                result = result[[*key_cols, *embedding_cols]]

                # Save to csv for loading with copy
                os.makedirs(f'{DATA_PATH}/{datatype}{table_prefix}', exist_ok=True)
                batch_file = f'{DATA_PATH}/{datatype}{table_prefix}/chunk_{offset//batch_size}.csv'
                result.to_csv(batch_file, index=False, header=False, sep=';', quoting=csv.QUOTE_NONE, escapechar='\\')
            else:
                batch_file = f'{DATA_PATH}/{datatype}{table_prefix}/chunk_{offset//batch_size}.csv'

            with open(batch_file, 'r') as f:
                cursor.copy_expert(f"COPY temp_results_{datatype}{table_prefix} FROM STDIN (FORMAT CSV, DELIMITER ';', QUOTE '\"', ESCAPE '\\')", f)

            os.remove(batch_file)

            print('Updating change table', flush=True)

            # Update embedding features
            query = f"""
                UPDATE features_{datatype}{table_prefix} sf
                SET {', '.join([f'{col} = tp.{col}' for col in embedding_cols])}, processed = TRUE
                FROM temp_results_{datatype}{table_prefix} tp 
                WHERE 
                    (sf.label = '' OR sf.label IS NULL) AND
                    {' AND '.join([f'sf.{col} = tp.{col}' if col != 'change_target' else f"COALESCE(sf.{col}, '') = COALESCE(tp.{col}, '')" for col in key_cols])}
            """
            cursor.execute(query)

            cursor.execute(f"TRUNCATE TABLE temp_results_{datatype}{table_prefix}")

            self.conn.commit()

            offset += batch_size
            num_batches += 1

        print(f'Created {num_batches} batches for embedding features update', flush=True)

        cursor.execute(f"DROP TABLE temp_results_{datatype}{table_prefix}")
        self.conn.commit()

        final_end_time = time.time() - main_start_time
        final_time, unit = get_time_unit(final_end_time)
        print(f'Finished creating and updating embedding features for {datatype} in {final_time} {unit}', flush=True)

    
    def update_label_description_entity_features(self, table_prefix):
        """
        Update new_value_label, new_value_description, old_value_label, old_value_description for entity changes
        so we can calculate the features using these values
        """
        if not self.conn:
            print('No DB connection available', flush=True)
            return

        cursor = self.conn.cursor()

        old_new = ['old', 'new']

        for suffix in old_new:
            print(f'Updating {suffix}_value_label, {suffix}_value_description in the features_entity{table_prefix}', flush=True)
            start_time = time.time()

            cursor.execute(f"""
                UPDATE features_entity{table_prefix} fe
                SET 
                    {suffix}_value_label =
                        CASE 
                            WHEN elad.label IS NOT NULL AND elad.label <> '' THEN elad.label
                            ELSE elad.alias 
                        END
                    ,
                    {suffix}_value_description = elad.description
                FROM entity_labels_alias_description elad
                WHERE elad.qid::TEXT = fe.{suffix}_value->>0 AND ({suffix}_value_label IS NULL OR {suffix}_value_label = '')
            """)

            elapsed_time = time.time() - start_time
            final_time, unit = get_time_unit(elapsed_time)

            print(f'Finished updating {suffix}_value_label and {suffix}_value_description in {final_time} {unit}', flush=True)


    def create_all_features_entity(self, table_prefix):

        #TODO: Only needs to run once, then this can be commented to be skipped
        # update old_value_label and new_value_label & old_value_description and new_value_description
        self.update_label_description_entity_features(table_prefix)
        
        # transitive closure 
        self.transitive_cache = TransitiveClosureCache(CSV_PATHS)

        select_cols_str = ', '.join([
            'old_value_label', 'new_value_label',
            'old_value_description', 'new_value_description', 
            'entity_label', 'entity_description', 
            'entity_types_31', 'entity_types_279'
        ])

        ENTITY_ONLY_FEATURES_COLS = ENTITY_ONLY_FEATURES_COLS_TYPES.keys()
        feature_cols_str = ', '.join(ENTITY_ONLY_FEATURES_COLS)

        key_cols = ['revision_id', 'property_id', 'value_id', 'change_target']
        key_cols_str = ', '.join(key_cols)

        batch_size = 100000
        offset = 0
        num_batches = 0

        cursor = self.conn.cursor()

        key_cols_temp = ', '.join([f'{col} {col_type}' for col, col_type in BASE_KEY_TYPES.items()])
        cursor.execute(f"CREATE TEMP TABLE temp_results_{datatype}{table_prefix} ({key_cols_temp}, {', '.join([f'{col} {col_type}' for col, col_type in ENTITY_ONLY_FEATURES_COLS_TYPES.items()])})")

        start_time = time.time()
        
        while True:

            query = """
                SELECT {key_cols_str}, {select_cols_str}, {feature_cols_str}
                    FROM features_entity{table_prefix}
                    WHERE 
                        (label IS NULL or label = '') and processed = FALSE
                    LIMIT {batch_size} OFFSET {offset}
            """.format(
                key_cols_str=key_cols_str,
                select_cols_str=select_cols_str,
                feature_cols_str=feature_cols_str,
                table_prefix=table_prefix,
                batch_size=batch_size,
                offset=offset
            )
            df = pd.read_sql(query, self.conn)
            
            if len(df) == 0:
                break
            
            # --------------- create text features + transitive closure features ---------------
            df[ENTITY_ONLY_FEATURES_COLS] = df.apply(
                lambda row: self.create_entity_features_text_transitive(row, self.transitive_cache),
                axis=1
            )
            
            # --------------- create embedding features ---------------
            result = self.create_embedding_features(df, old_col='old_value_label', new_col='new_value_label')

            # --------------- Save all results in csv for later load with copy ---------------
            datatype = 'entity'
            # Save to csv for loading with copy
            os.makedirs(f'{DATA_PATH}/{datatype}', exist_ok=True)
            batch_file = f'{DATA_PATH}/{datatype}/{datatype}_chunk_{offset//batch_size}.csv'
            result.to_csv(batch_file, index=False, header=False, sep=';', quoting=csv.QUOTE_NONE, escapechar='\\')
            
            offset += batch_size
            num_batches += 1

            # --------------- Load file data into temp table ---------------
            with open(batch_file, 'r') as f:
                cursor.copy_expert(f"COPY temp_results_{datatype}{table_prefix} FROM STDIN (FORMAT csv, DELIMITER ';', QUOTE '\"', ESCAPE '\\')", f)
            os.remove(batch_file)
        
        # --------------- Updating feature table ---------------
        print('Updating feature table', flush=True)

        cursor.execute(f"""
            UPDATE features_{datatype}{table_prefix} f
            SET {', '.join([f'{col} = tp.{col}' for col in ENTITY_ONLY_FEATURES_COLS])}, processed = TRUE
            FROM temp_results_{datatype}{table_prefix} tp
            WHERE 
                (f.label = '' OR f.label IS NULL) AND
                {' AND '.join([f'f.{col} = tp.{col}' if col != 'change_target' else f"COALESCE(f.{col}, '') = COALESCE(tp.{col}, '')" for col in key_cols])}
        """)
        
        elapsed_time = time.time() - start_time
        final_time, unit = get_time_unit(elapsed_time)
        print(f'Finished entity feature creation {final_time} {unit}', flush=True)

        cursor.execute(f"DROP TABLE temp_results_{datatype}{table_prefix}")

        self.conn.commit()


    def create_missing_features(self, datatype, table_prefix, max_batches=None):
        """
        Creates missing features for the given datatype and table.
        
        :param datatype: can be one of 'entity', 'property_replacement', 'quantity', 'time', 'text', 'globecoordinate'
        :param table_prefix: can be one of '_sa', '_ao', '_less'
        :param max_batches: maximum number of batches to process (optional)
        """
        print('Creating missing features for datatype:', datatype, flush=True)

        if datatype not in ['entity', 'property_replacement', 'quantity', 'time', 'text', 'globecoordinate']:
            print('Unsupported datatype for embedding features. Has to be one of: entity, property_replacement, quantity, time, text, globecoordinate. Input datatype:', datatype, flush=True)
            return

        if table_prefix not in ['_sa', '_ao', '_less', '']:
            print('Unsupported table prefix for embedding features. Has to be one of _sa, _ao, _less. Input table prefix:', table_prefix, flush=True)
            return

        if datatype == 'entity':
            self.create_all_features_entity(table_prefix)

        else:
            if datatype == 'property_replacement':
                embedding_cols = ['property_label_cosine_similarity']
                select_cols = ['deleted_property_label', 'created_property_label']
                key_cols = ['pair_id']
            else: # quantity, time, text, globecoordinate
                key_cols = ['revision_id', 'property_id', 'value_id', 'change_target']
                select_cols = ['old_value', 'new_value', 'entity_label', 'entity_description', 'property_label', 'entity_types_31', 'entity_types_279']
                embedding_cols = ['full_cosine_similarity']

            self.create_and_update_embedding_features(datatype, key_cols, select_cols, embedding_cols, table_prefix, max_batches=max_batches)
    
