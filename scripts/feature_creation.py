import csv
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
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import torch
import re
import io

from scripts.transitive_closure_cache import TransitiveClosureCache
from scripts.utils import query_to_df
from scripts.const import *
from scripts.utils import get_time_unit

class FeatureCreation():

    def __init__(self, set_up=None, conn=None):
        self.conn = conn
        self.set_up = set_up

    def create_embedding_features(self, model, df, old_col, new_col):
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

            old_val = str(row[old_col]).replace('"', '') # these are the entity labels
            new_val = str(row[new_col]).replace('"', '')
            if 'label' in old_col:

                old_value_description = str(row['old_value_description']) if not pd.isna(row['old_value_description']) else ''
                new_value_description = str(row['new_value_description']) if not pd.isna(row['new_value_description']) else ''

                # only calculate these features fro entity changes
                old_label.append(old_val) # labels
                new_label.append(new_val)

                old_description.append(old_value_description) # descriptions
                new_description.append(new_value_description)
            else:
                old_texts.append(old_val)
                new_texts.append(new_val)

        device = "cuda" if torch.cuda.is_available() else "cpu"

        if 'label' not in old_col: # remove for entity
            old_text_embeddings = model.encode(
                old_texts,
                device=device,
                show_progress_bar=True
            )
            new_text_embeddings = model.encode(
                new_texts,
                device=device,
                show_progress_bar=True
            )
            # calculate cosine similarity
            similarities = np.array([
                cosine_similarity([old_emb], [new_emb])[0][0]
                for old_emb, new_emb in zip(old_text_embeddings, new_text_embeddings)
            ])
            df['value_cosine_similarity'] = similarities

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

    ####################################################################################################
    # Text features
    ################################################################################
    @staticmethod
    def create_text_features(datatype, old_value, new_value):
        """Extract features for string datatype changes"""
        
        features = dict()

        new_value = str(new_value).strip().replace('"', '')
        old_value = str(old_value).strip().replace('"', '')
        
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

        old_len = len(old_value)
        new_len = len(new_value)
        max_len = max(old_len, new_len) if max(old_len, new_len) > 0 else 1 # replace 0's with 1 to avoid division by zero

        lev_dist = levenshtein_distance(old_value.lower().strip(), new_value.lower().strip())
       
        # percentage of how much changed
        features['edit_distance_ratio'] = lev_dist / max_len
        
        if (features['token_overlap'] == 0) and (features['old_in_new'] == 0) and (features['new_in_old'] == 0):
            features['complete_replacement'] = 1
        else:
            features['complete_replacement'] = 0

        result = (
            features['token_overlap'],
            features['old_in_new'],
            features['new_in_old'], 
            features['edit_distance_ratio'],
            features['complete_replacement'],
        )

        if datatype == 'text': # remove for entity

            features['length_diff_abs'] = int(abs(len(new_value) - len(old_value)))
            features['token_count_old'] = int(len(old_value.split()))
            features['token_count_new'] = int(len(new_value.split()))

            special_char_regex = r'[^a-zA-Z0-9]'
            old_value_no_special_char = re.sub(special_char_regex,"",old_value)
            new_value_no_special_char = re.sub(special_char_regex,"",new_value)
            features['same_value_without_special_char'] = int(old_value_no_special_char == new_value_no_special_char)

            special_char_count_old = len(re.findall(special_char_regex, old_value))
            special_char_count_new = len(re.findall(special_char_regex, new_value))

            features['special_char_count_diff'] =  special_char_count_old - special_char_count_new
            
            # features['special_chars_added'] = int(features['special_char_count_diff'] > 0)
            # features['special_chars_removed'] = int(features['special_char_count_diff'] < 0)
            # # the values are the same but there's a change of a special char
            # features['only_special_char_change'] = int((features['same_value_without_special_char'] == 1) & (features['special_char_count_diff'] != 0))

            # for property_value_update or rewording (?), the structure similarity should be low.
            # for textual change the structure similarity should be high.
            # if max(features['token_count_old'], features['token_count_new']) == 0:
            #     features['structure_similarity'] = 0
            # else:
            #     features['structure_similarity'] = 1 - abs(features['token_count_old'] - features['token_count_new']) / \
            #                                 max(features['token_count_old'], features['token_count_new'])
                
            def get_edit_operations(old_value, new_value):
                
                m, n = len(old_value), len(new_value)
                dp = [[0] * (n + 1) for _ in range(m + 1)]
                
                for i in range(m + 1):
                    dp[i][0] = i
                for j in range(n + 1):
                    dp[0][j] = j
                
                for i in range(1, m + 1):
                    for j in range(1, n + 1):
                        if old_value[i-1] == new_value[j-1]:
                            dp[i][j] = dp[i-1][j-1]
                        else:
                            dp[i][j] = 1 + min(
                                dp[i-1][j],    # deletion
                                dp[i][j-1],    # insertion
                                dp[i-1][j-1]   # substitution
                            )

                i, j = m, n
                insertions = deletions = substitutions = 0
                
                while i > 0 or j > 0:
                    if i > 0 and j > 0 and old_value[i-1] == new_value[j-1]:
                        i -= 1
                        j -= 1
                    elif i > 0 and j > 0 and dp[i][j] == dp[i-1][j-1] + 1:
                        substitutions += 1
                        i -= 1
                        j -= 1
                    elif j > 0 and dp[i][j] == dp[i][j-1] + 1:
                        insertions += 1
                        j -= 1
                    else:
                        deletions += 1
                        i -= 1
                
                return insertions, deletions, substitutions

            features['char_insertions'], features['char_deletions'], features['char_substitutions'] = get_edit_operations(old_value, new_value)

            features['adjacent_char_swap'] = FeatureCreation.has_adjacent_swap(old_value, new_value)

            features['avg_word_similarity'] = FeatureCreation.avg_word_levenshtein(old_value, new_value)

            # what os.path.commonprefix returns: paths: ['/home/User/Photos', /home/User/Videos']    commonprefix: /home/User/
            # Added that length of suffix/prefix is at least 3 to avoid short suffix/prefix (e.g. just the first letter...)
            features['has_significant_prefix'] = int(len(os.path.commonprefix([old_value, new_value])) >= 3)

            features['has_significant_suffix'] = int(len(os.path.commonprefix([old_value[::-1], new_value[::-1]])) >= 3)

            result = result + (
                features['length_diff_abs'],
                features['token_count_old'],
                features['token_count_new'],  
                lev_dist,
                # features['structure_similarity'],
                features['same_value_without_special_char'],
                features['special_char_count_diff'],
                # features['special_chars_added'],
                # features['special_chars_removed'],
                # features['only_special_char_change'],
                features['char_insertions'],
                features['char_deletions'],
                features['char_substitutions'],
                features['adjacent_char_swap'],
                features['has_significant_prefix'],
                features['has_significant_suffix'],
            )
        
        return result

    ########################################################################################################################
    # Time features
    ########################################################################################################################
    @staticmethod
    def create_time_features(old_value, new_value):
        """
        Extract time change features
        """
        old_value = str(old_value).strip().replace('"', '')
        new_value = str(new_value).strip().replace('"', '')

        if old_value in ['some_value', 'no_value'] or new_value in ['some_value', 'no_value']:
            return (
                1000, # big difference
                0,
                0,
                0,
                0,
                0,
                0,
                1, # month, year, day changed
                1,
                1
            )
        else:

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

            old_date = get_date_parts(old_value, 'date') 
            new_date = get_date_parts(new_value, 'date')

        features = dict()

        def calc_date_diff(old_date, new_date):
            """Calculate date difference in days"""
            try:
                
                dt1_year, dt1_month, dt1_day = old_date
                dt2_year, dt2_month, dt2_day = new_date

                # do it manually since WD allows 00 for month and day and Pyhton libraries don't
                diff_year = int(abs(dt2_year - dt1_year) * 365.25) # use .25 for leap years
                diff_month = int(abs(dt2_month - dt1_month) * 30.44) # average days in month
                diff_day = int(abs(dt2_day - dt1_day))

                return diff_year + diff_month + diff_day
            
            except:
                return 10000
        
        def calc_sign_change(dt1, dt2):

            if dt1[1:] == dt2[1:]:
                return 1
            else:
                return 0
        
        def added_removed_part(old_date, new_date, part='year', option='date', change_type='added'):
            try:

                if option == 'date':
                    year1, month1, day1 = old_date
                    year2, month2, day2 = new_date
                    
                    if year1 != year2:
                        return 0

                    if change_type == 'added':
                        if part == 'year' and year1 == 0 and year2 != 0:
                            return 1
                        # YYYY-01-01 -> YYYY-05-00:
                        # YYYY-01-01 -> YYYY-05-10:
                        if part == 'month' and ((month1 == 0 and month2 > 0 and day1 == 0) or (month1 == 1 and month2 > 1 and day1 == 1 and (day2 > 1 or day2 == 0))):
                            return 1
                        if part == 'day' and ((day1 == 0 and day2 > 0) or (day1 == 1 and day2 > 1 and month1 == 1 and month2 > 1)):
                            return 1
                        return 0
                    elif change_type == 'removed':

                        if part == 'year' and year1 > 0 and year2 == 0:
                                return 1

                        # cases like: YYYY-MM-DD -> YYYY-01-01 where MM and DD !=01
                        if (part == 'month' or part == 'day') and month1 > 1 and day1 > 1 and month2 == 1 and day2 == 1:
                            return 1
                        # cases like YYYY-MM-00 -> YYYY-00-00 
                        if part == 'month' and month1 > 0 and month2 == 0:
                            if not (day1 == 1 and day2 == 0) and not (day1 == 0 and day2 == 0): # if it's not a reformatting change
                                return 1
                        # cases like YYYY-MM-DD -> YYYY-MM-00
                        if part == 'day' and day1 > 0 and day2 == 0:
                            if not (day1 == 1 and day2 == 0): # if it's not a reformatting change
                                return 1
                        return 0
            
            except:
                return 0

        def is_placeholder_to_zero(old_date, new_date):

            year1, month1, day1 = old_date
            year2, month2, day2 = new_date
            
            if year1 != year2:
                return 0  # year changed, not a reformatting
            
            # YYYY-01-01 -> YYYY-00-00 
            if month1 == 1 and day1 == 1 and month2 == 0 and day2 == 0:
                return 1
            
            # YYYY-MM-01 -> YYYY-MM-00 
            if month1 == month2 and month1 > 1 and day1 == 1 and day2 == 0:
                return 1
            
            # YYYY-01-00 -> YYYY-00-00 
            if month1 == 1 and month2 == 0 and day1 == 0 and day2 == 0:
                return 1

            return 0
        
        def date_part_changed(old_date, new_date, option='year'):
            
            """
            Returns 1 if there was an actual change in the month/year/day
            Not re_formatting (e.g. 01-01 -> XX-XX, 01-01 -> 00-00, 01-00 -> XX-00) or unrefinement/refinement (e.g. from X to 0)
            """
            year1, month1, day1 = old_date
            year2, month2, day2 = new_date
            if option == 'year':
                if year1 != year2:
                    return 1
            elif option == 'month':
                is_reformatting = ((month1 == 1 and day1 == 1 and day2 == 0 and month2 == 0) or  \
                                (month1 > 0 and month2 > 0 and month1 == month2 and day1 == 1 and day2 == 0) or \
                                (month1 == 1 and month2 == 0 and day1 == 0 and day2 == 0)) and year1 == year2
                
                # 1. just month added
                # 2. goes from 01-01 to MM-00 or MM-DD
                # 3. goes from 00-00 to MM-DD
                is_refinement = ((month1 == 0 and month2 > 0 and day2 == 0) or \
                                (month1 == 1 and day1 == 1 and month2 > 1 and (day2 > 1 or day2 == 0)) or \
                                (month1 == 0 and month2 > 0 and day1 == 0 and day2 > 0)) and year1 == year2
                # goes from MM-00 (month1) to 00-00 (month2)
                is_unrefinement = (month2 == 0 and month1 > 0) and year1 == year2
                if month1 != month2 and not is_reformatting and not is_refinement and not is_unrefinement:
                    return 1
            elif option == 'day':
                # 1. goes from 01-01 to 00-00
                # 2. goes from XX-01 to XX-00
                # 3. goes from 01-00 to 00-00
                is_reformatting = ((month1 == 1 and day1 == 1 and day2 == 0 and month2 == 0) or  \
                                (month1 > 0 and month2 > 0 and month1 == month2 and day1 == 1 and day2 == 0) or \
                                (month1 == 1 and month2 == 0 and day1 == 0 and day2 == 0)) and year1 == year2
                # 1. XX-00 to XX-DD with XX that can be 01/00
                # 2. goes from 01-01 to MM-DD
                is_refinement = ((day1 == 0 and day2 > 0) or \
                                (day1 == 1 and day2 > 1 and month1 == 1 and month2 > 1)) and year1 == year2
                # goes from XX-00 to XX-DD 
                is_unrefinement = (day2 == 0 and day1 > 0) and year1 == year2
                if day1 != day2 and not is_reformatting and not is_refinement and not is_unrefinement:
                    return 1
            return 0

        features['date_diff_days'] = calc_date_diff(old_date, new_date)
        features['sign_change'] = calc_sign_change(old_value, new_value)
        features['change_one_to_zero'] = is_placeholder_to_zero(old_date, new_date)
        features['day_added'] = added_removed_part(old_date, new_date, part='day', option='date', change_type='added')
        features['day_removed'] = added_removed_part(old_date, new_date, part='day', option='date', change_type='removed')
        features['month_added'] = added_removed_part(old_date, new_date, part='month', option='date', change_type='added')
        features['month_removed'] = added_removed_part(old_date, new_date, part='month', option='date', change_type='removed')

        features['different_year'] = date_part_changed(old_date, new_date, option='year')
        features['different_day'] = date_part_changed(old_date, new_date, option='day')
        features['different_month'] = date_part_changed(old_date, new_date, option='month')

        result = (
            features['date_diff_days'],
            features['sign_change'],
            features['change_one_to_zero'],
            features['day_added'],
            features['day_removed'],
            features['month_added'],
            features['month_removed'],
            features['different_year'],
            features['different_day'],
            features['different_month']
        )

        return result
    
    ########################################################################################################################
    # Quantity features
    ########################################################################################################################
    @staticmethod
    def calc_precision_change(old_value, new_value, datatype='quantity', part=None):
        # returns 1 if only precision (decimal places) changed, 0 otherwise
        if datatype == 'globecoordinate':
            if '{' in old_value and '{' in new_value:
                old = json.loads(old_value).get(part, None)
                new = json.loads(new_value).get(part, None)
            
            elif type(old_value) == dict and type(new_value) == dict:
                old = old_value.get(part, None)
                new = new_value.get(part, None)
            
            else:
                return 0
            
            old_ndp = str(old).split('.')[0] if '.' in str(old) else str(old)
            try:
                old_dp = str(old).split('.')[1] if '.' in str(old) and int(str(old).split('.')[1]) > 0 else '0'
            except ValueError:
                old_dp = '0'

            new_ndp = str(new).split('.')[0] if '.' in str(new) else str(new)
            try:
                new_dp = str(new).split('.')[1] if '.' in str(new) and int(str(new).split('.')[1]) > 0 else '0'
            except ValueError:
                new_dp = '0'
        else:

            # quantity
            old_ndp = str(old_value).split('.')[0] if '.' in str(old_value) else str(old_value)
            try:
                old_dp = str(old_value).split('.')[1] if '.' in str(old_value) and int(str(old_value).split('.')[1]) > 0 else '0'
            except ValueError:
                old_dp = '0'

            new_ndp = str(new_value).split('.')[0] if '.' in str(new_value) else str(new_value)
            try:
                new_dp = str(new_value).split('.')[1] if '.' in str(new_value) and int(str(new_value).split('.')[1]) > 0 else '0'
            except ValueError:
                new_dp = '0'

        # if both decimal parts are 0 -> there's no precision change
        # e.g. 12 -> 12.0 is not a precision change, or 12.0 -> 12.00
        if old_ndp == new_ndp and old_dp != new_dp and (old_dp != '0' or new_dp != '0'):
            return 1
        else:
            return 0

    @staticmethod
    def calc_length_increase_decrease(old_value, new_value, datatype='quantity', option='increase', part=None):

        if datatype == 'quantity':
            new_length = len(str(new_value).replace('-', '').replace('+', '').replace('.', ''))
            old_length = len(str(old_value).replace('-', '').replace('+', '').replace('.', ''))
        else: # globecoordinate
            if '{' in old_value and '{' in new_value: # for somevalue or novalue
                old = str(json.loads(old_value).get(part, '')) # part is longitude or latitude
                new = str(json.loads(new_value).get(part, ''))
                new_length = len(new.replace('-', '').replace('+', '').replace('.', ''))
                old_length = len(old.replace('-', '').replace('+', '').replace('.', ''))
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
        return 1 if (old_float * new_float < 0) and (math.floor(abs(old_float)) == math.floor(abs(new_float))) else 0

    @staticmethod
    def check_containment(old_value, new_value, datatype='quantity', part=None, option='old_in_new'):

        if datatype == 'globecoordinate':
            if '{' in old_value and '{' in new_value:
                old_value = json.loads(old_value).get(part, None)
                new_value = json.loads(new_value).get(part, None)
            else:
                return 0

        if option == 'old_in_new':
            return 1 if str(new_value).startswith(str(old_value)) else 0
        elif option == 'new_in_old':
            return 1 if str(old_value).startswith(str(new_value)) else 0
        else:
            return 0
    
    @staticmethod
    def same_decimal_length(old_value, new_value, datatype='quantity', part=None):
        if datatype == 'globecoordinate':
            if '{' in old_value and '{' in new_value:
                old_value = json.loads(old_value).get(part, None)
                new_value = json.loads(new_value).get(part, None)
            else:
                return 0
            
        old_dec = str(old_value).split('.')[1] if '.' in str(old_value) else ''
        new_dec = str(new_value).split('.')[1] if '.' in str(new_value) else ''

        return 1 if len(old_dec) == len(new_dec) else 0
    
    @staticmethod
    def same_float_value(old_value, new_value, datatype='quantity', part=None):
        if datatype == 'globecoordinate':
            if '{' in old_value and '{' in new_value:
                old_value = json.loads(old_value).get(part, None)
                new_value = json.loads(new_value).get(part, None)
            else:
                return 0
        try:
            return 1 if float(old_value) == float(new_value) else 0
        except:
            return 0

    @staticmethod
    def create_quantity_features(old_value, new_value):
        features = dict()

        new_value = str(new_value).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()
        old_value = str(old_value).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()
        
        # remove + sign
        old_str = str(old_value).replace('"', '').replace('+', '').strip()
        new_str = str(new_value).replace('"', '').replace('+', '').strip()

        # sign change 
        features['sign_change'] = FeatureCreation.calc_sign_change(old_str, new_str, datatype='quantity')
        
        # only precision change 
        features['precision_change'] = FeatureCreation.calc_precision_change(old_str, new_str, datatype='quantity')
        
        features['whole_number_change'] = int(np.floor(abs(float(old_str))) != np.floor(abs(float(new_str))))

        features['old_is_prefix_of_new'] = FeatureCreation.check_containment(old_str, new_str, datatype='quantity', option='old_in_new')
        features['new_is_prefix_of_old'] = FeatureCreation.check_containment(old_str, new_str, datatype='quantity', option='new_in_old')

        if features['old_is_prefix_of_new'] == 1:
            features['length_increase'] = FeatureCreation.calc_length_increase_decrease(old_str, new_str, datatype='quantity', option='increase')
        else: 
            features['length_increase'] = 0

        if features['new_is_prefix_of_old'] == 1:
            features['length_decrease'] = FeatureCreation.calc_length_increase_decrease(old_str, new_str, datatype='quantity', option='decrease')
        else:
            features['length_decrease'] = 0

        features['same_float_value'] = FeatureCreation.same_float_value(old_str, new_str, datatype='quantity')

        result = (
            features['sign_change'],
            features['precision_change'],
            features['length_increase'],
            features['length_decrease'],
            features['whole_number_change'],
            features['old_is_prefix_of_new'],
            features['new_is_prefix_of_old'],
            features['same_float_value'],
        )

        return result


    ########################################################################################################################
    # Globecoordinate features
    ########################################################################################################################
    @staticmethod
    def create_globe_coordinate_features(old_value, new_value):

        features = dict()
        old_val = json.loads(old_value)
        new_val = json.loads(new_value)

        new_val['latitude'] = float(str(new_val['latitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip())
        new_val['longitude'] = float(str(new_val['longitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip())
        
        # add abs because if there's a negative value then they will be different even though the whole number is the same
        features['latitude_whole_number_change'] = (1 if math.floor(abs(new_val['latitude'])) != math.floor(abs(old_val['latitude'])) else 0)
        features['longitude_whole_number_change'] = (1 if math.floor(abs(new_val['longitude'])) != math.floor(abs(old_val['longitude'])) else 0)

        features['latitude_sign_change'] = int((float(new_val['latitude']) * float(old_val['latitude']) < 0) and (features['latitude_whole_number_change'] == 0))
        features['longitude_sign_change'] = int((float(new_val['longitude']) * float(old_val['longitude']) < 0) and (features['longitude_whole_number_change'] == 0))

        features['latitude_precision_change'] = FeatureCreation.calc_precision_change(old_value, new_value, datatype='globecoordinate', part='latitude')
        features['longitude_precision_change'] = FeatureCreation.calc_precision_change(old_value, new_value, datatype='globecoordinate', part='longitude')

        features['longitude_old_is_prefix_of_new'] = FeatureCreation.check_containment(old_value, new_value, datatype='globecoordinate', part='longitude', option='old_in_new')
        features['longitude_new_is_prefix_of_old'] = FeatureCreation.check_containment(old_value, new_value, datatype='globecoordinate', part='longitude', option='new_in_old')

        features['latitude_old_is_prefix_of_new'] = FeatureCreation.check_containment(old_value, new_value, datatype='globecoordinate', part='latitude', option='old_in_new')
        features['latitude_new_is_prefix_of_old'] = FeatureCreation.check_containment(old_value, new_value, datatype='globecoordinate', part='latitude', option='new_in_old')

        if features['latitude_old_is_prefix_of_new'] == 1:
            features['latitude_length_increase'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='increase', part='latitude')
        else:
            features['latitude_length_increase'] = 0
        
        if features['latitude_new_is_prefix_of_old'] == 1:
            features['latitude_length_decrease'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='decrease', part='latitude')
        else:
            features['latitude_length_decrease'] = 0

        if features['longitude_old_is_prefix_of_new'] == 1:
            features['longitude_length_increase'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='increase', part='longitude')
        else:
            features['longitude_length_increase'] = 0
        
        if features['longitude_new_is_prefix_of_old'] == 1:
            features['longitude_length_decrease'] = FeatureCreation.calc_length_increase_decrease(old_value, new_value, 'globecoordinate', option='decrease', part='longitude')
        else:
            features['longitude_length_decrease'] = 0

        features['longitude_same_float_value'] = FeatureCreation.same_float_value(old_value, new_value, datatype='globecoordinate', part='longitude')
        features['latitude_same_float_value'] = FeatureCreation.same_float_value(old_value, new_value, datatype='globecoordinate', part='latitude')

        result = (
            features['latitude_sign_change'], 
            features['longitude_sign_change'],

            features['latitude_whole_number_change'], 
            features['longitude_whole_number_change'], 

            features['latitude_precision_change'], 
            features['longitude_precision_change'], 

            features['latitude_length_increase'], 
            features['latitude_length_decrease'], 

            features['longitude_length_increase'], 
            features['longitude_length_decrease'], 

            features['latitude_old_is_prefix_of_new'],
            features['latitude_new_is_prefix_of_old'],
            features['latitude_same_float_value'],

            features['longitude_old_is_prefix_of_new'],
            features['longitude_new_is_prefix_of_old'],
            features['longitude_same_float_value'],
        )

        return result

    ########################################################################################################################
    # Entity features
    ########################################################################################################################
                
    @staticmethod
    def create_entity_features_text_transitive(row, transitive_cache):
        """Extract features for entity datatypes using labels"""

        old_value_label = row['old_value_label']
        new_value_label = row['new_value_label']

        features_tuple = FeatureCreation.create_text_features('entity', old_value_label, new_value_label)
        # ['token_overlap', 'old_in_new', 'new_in_old', 'edit_distance_ratio', 'complete_replacement', 'label_cosine_similarity', 'description_cosine_similarity', 'is_link_change', 
        # 'old_value_subclass_new_value', 'new_value_subclass_old_value', 'old_value_located_in_new_value', 'new_value_located_in_old_value', 'old_value_has_parts_new_value', 'new_value_has_parts_old_value', 'old_value_part_of_new_value', 'new_value_part_of_old_value']

        text_features_dict = {
            'token_overlap': features_tuple[0],
            'old_in_new': features_tuple[1],
            'new_in_old': features_tuple[2], 
            'edit_distance_ratio': features_tuple[3],
            'complete_replacement': features_tuple[4],
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

        is_link_change = int((old_value_label == new_value_label) & (row['old_value'] != row['new_value']))

        result = {**text_features_dict, **features, 'label_cosine_similarity': 0.0, 'description_cosine_similarity': 0.0, 'is_link_change': is_link_change}
        
        ENTITY_ONLY_FEATURES_COLS = ENTITY_ONLY_FEATURES_COLS_TYPES.keys()

        return pd.Series(result, index=ENTITY_ONLY_FEATURES_COLS)
    
    def create_entity_features(self):
        # ['token_overlap', 'old_in_new', 'new_in_old', 'edit_distance_ratio', 'complete_replacement', 'label_cosine_similarity', 'description_cosine_similarity', 'is_link_change', 
        # 'old_value_subclass_new_value', 'new_value_subclass_old_value', 'old_value_located_in_new_value', 'new_value_located_in_old_value', 'old_value_has_parts_new_value', 'new_value_has_parts_old_value', 'old_value_part_of_new_value', 'new_value_part_of_old_value']

        return (
            None, # token_overlap
            None, # old_in_new
            None, # new_in_old
            None, # edit_distance_ratio
            None, # complete_replacement
            None, # is_link_change
            0, # new_value_part_of_old_value
            0, # old_value_part_of_new_value
            0, # new_value_subclass_old_value
            0, # old_value_subclass_new_value
            0, # new_value_has_parts_old_value
            0, # old_value_has_parts_new_value
            0, # new_value_located_in_old_value
            0, # old_value_located_in_new_value
            '', # old_value_label
            '', # new_value_label
            '', # old_value_description
            '', # new_value_description
        )
        

    ####################
    # Reverted edit features
    ####################
    def check_revert(self, current_change, next_change):
        """Check for hash reversion + (comment with reverted edit keyword or reverted within 4 weeks)"""

        curr_old_hash = str(current_change.get('old_value', '')).strip() if current_change.get('old_value', '') != '{}' else ''
        curr_new_hash = str(current_change.get('new_value', '')).strip() if current_change.get('new_value', '') != '{}' else ''

        next_old_hash = str(next_change.get('old_value', '')).strip() if next_change.get('old_value', '') != '{}' else ''
        next_new_hash = str(next_change.get('new_value', '')).strip() if next_change.get('new_value', '') != '{}' else ''

        next_comment = str(next_change.get('comment', '')).lower()

        def parse_timestamp(ts):
            if isinstance(ts, datetime):
                return ts
            ts_str = str(ts).replace("T", " ").replace("Z", "")
            ts_str = re.sub(r'[+-]\d{2}:?\d{0,2}$', '', ts_str).strip()
            return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

        next_timestamp = parse_timestamp(next_change['timestamp'])
        current_timestamp = parse_timestamp(current_change['timestamp'])

        diff_timestamps = (next_timestamp - current_timestamp).total_seconds()
        # seconds_per_day = 24 * 60 * 60 
        # seconds_in_four_weeks = 28 * seconds_per_day
        time_threshold = self.set_up.get('time_threshold_seconds', 28 * 24 * 60 * 60)

        # DELETE + UPDATE case
        # direct reversion: A→B then B→A (no intermediates)
        direct = (
            curr_old_hash == next_new_hash and 
            curr_new_hash == next_old_hash and
            curr_old_hash != '' and next_new_hash != '' and
            diff_timestamps <= time_threshold
        )

        # trailing reversion: A→B ... →A (with intermediates, requires rv comment)
        trailing = (
            curr_old_hash == next_new_hash and
            curr_old_hash != '' and
            next_new_hash != '' and
            curr_new_hash != next_old_hash and  # explicitly intermediates exist
            # still restrict on time in case there's a restore that undos a similar change that happened 3 years ago, the values will still 
            # match
            (('restore' in next_comment or 'rollback' in next_comment) and diff_timestamps <= time_threshold)  # trailing reverts are done by restores/rollbacks
        )

        # CREATE case
        create_case = (
            curr_old_hash == '' and 
            next_new_hash == '' and 
            curr_new_hash == next_old_hash and
            diff_timestamps <= time_threshold
        )

        if (direct or trailing or create_case):
            return 1

        return 0
    
    def tag_reverted_edits(self, changes_pv_dict, value_changes, entity_stats):
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
        # stores tuples for a change (value or rank change): (is_reverted, reversion, reversion_timestamp, revision_id_reversion)
        revert_flags = {} 
        
        num_reverted_edits = 0
        num_reversions = 0
        num_reverted_edits_create = 0
        num_reverted_edits_delete = 0
        num_reverted_edits_update = 0
        
        # process changes_by_epvc and determine revert status
        for (property_id, value_id, change_target), pv_changes in changes_pv_dict.items():
            pv_changes.sort(key=lambda x: x['timestamp'])
            reversion_keys = set()
            reverted_keys = set()

            for i, current_change in enumerate(pv_changes):
                curr_key = get_key_from_change(current_change, property_id, value_id)

                if curr_key in reverted_keys:
                        continue

                next_changes = pv_changes[i+1:]

                for j, future_change in enumerate(next_changes):

                    future_key = (future_change['revision_id'], property_id, value_id, future_change['change_target'])
                    if future_key in reversion_keys or \
                        change_target != future_change['change_target'] or \
                        (current_change['change_target'] == 'rank' and current_change['action'] in ['DELETE', 'CREATE']):
                        # it has already been marked or the change target is different (e.g. value vs rank), so skip
                        # only skip the create/delete of rank, those get tagged if the corresponding value gets tagged
                        continue

                    curr_action = current_change['action']
                    next_action = future_change['action']

                    valid_action_pair = (
                        (curr_action == 'UPDATE' and next_action == 'UPDATE') or
                        (curr_action == 'CREATE' and next_action == 'DELETE') or
                        (curr_action == 'DELETE' and next_action == 'CREATE') or
                        # for restore cases like:
                        (curr_action == 'UPDATE' and next_action == 'CREATE' and (('restore' in future_change['comment']) or ('rollback' in future_change['comment'])))
                    )

                    reverted = 0
                    if valid_action_pair:
                        reverted = self.check_revert(current_change, future_change)
                    
                    if reverted == 1:
                        # mark current edit as reverted
                        rank_key = (current_change['revision_id'], property_id, value_id, 'rank')
                        if curr_key not in revert_flags:
                            # flags: 1, 0
                            revert_flags[curr_key] = (1, 0, future_change['timestamp'], future_change['revision_id'])

                            if current_change['change_target'] == '' and (current_change['action'] in ['DELETE', 'CREATE']):
                                revert_flags[rank_key] = (1, 0, future_change['timestamp'], future_change['revision_id'])

                        elif revert_flags[curr_key][0] == 0 and revert_flags[curr_key][1] == 1:  # is_reverted == 0 adn reversion == 1
                            revert_flags[curr_key] = (1, 1, future_change['timestamp'], future_change['revision_id'])

                            if change_target == '' and current_change['action'] in ['DELETE', 'CREATE']: # tag the rank changes
                                revert_flags[rank_key] = (1, 1, future_change['timestamp'], future_change['revision_id'])

                        reverted_keys.add(curr_key)

                        future_key = (future_change['revision_id'], property_id, value_id, future_change['change_target'])
                        rank_key = (future_change['revision_id'], property_id, value_id, 'rank')
                        if future_key not in revert_flags:
                            revert_flags[future_key] = (0, 1, None, None)

                            if future_change['change_target'] == '' and (future_change['action'] in ['DELETE', 'CREATE']):
                                revert_flags[rank_key] = (0, 1, None, None)

                        elif revert_flags[future_key][1] == 0 and revert_flags[future_key][0] == 1: # reversion = 0 and is_Reverted = 1
                            
                            revert_flags[future_key][1] = (1, 1, revert_flags[future_key][2], revert_flags[future_key][3])
                            
                            if future_change['change_target'] == '' and future_change['action'] in ['DELETE', 'CREATE']:
                                
                                revert_flags[rank_key] = (1, 1, revert_flags[rank_key][2], revert_flags[rank_key][3])

                        reversion_keys.add(future_key)

                        # restore changes where the value restored (CREATE)
                        # comes from a sequence of updates
                        # v1 -> v2 #update
                        # v2 -> v3
                        # v3 -> {} # deleted
                        # {} -> v1 # create
                        if ('restore' in future_change['comment'] or 'rollback' in future_change['comment']) and \
                            current_change['action'] == 'UPDATE' and \
                            future_change['action'] == 'CREATE':

                                for inter_change in next_changes[:j]:
                                    
                                    inter_key = (inter_change['revision_id'], property_id, value_id, inter_change['change_target'])
                                    reverted_keys.add(inter_key)
                                    if inter_key not in revert_flags:
                                        revert_flags[inter_key] = (1, 0, future_change['timestamp'], future_change['revision_id'])
                                        
                                        if inter_change['change_target'] == '' and (inter_change['action'] in ['DELETE', 'CREATE']):
                                            rank_key = (inter_change['revision_id'], property_id, value_id, 'rank')
                                            revert_flags[rank_key] = (1, 0, future_change['timestamp'], future_change['revision_id'])
                                    
                                        # Update stats for intermediate changes
                                        counts = update_revert_stats(inter_change)
                                        
                                        num_reverted_edits += counts['total']
                                        num_reverted_edits_create += counts['create']
                                        num_reverted_edits_delete += counts['delete']
                                        num_reverted_edits_update += counts['update']

                        # Update stats for the original reverted change
                        counts = update_revert_stats(current_change)
                        
                        num_reverted_edits += counts['total']
                        num_reverted_edits_create += counts['create']
                        num_reverted_edits_delete += counts['delete']
                        num_reverted_edits_update += counts['update']
                        
                        # Update stats for the reversion (future_change counted as reversion only)
                        num_reversions += 1
                            
                        break  # Found revert, move to next change
        
        final_value_changes = []
        
        for key, original_tuple in dict_lookup.items():

            if key[3] == 'rank':
                # need to get corresponding value change for rank
                value_key = (key[0], key[1], key[2], '')
                is_reverted, reversion, reversion_timestamp, revision_id_reversion  = revert_flags.get(value_key, (0, 0, None, None))
            else:
                is_reverted, reversion, reversion_timestamp, revision_id_reversion = revert_flags.get(key, (0, 0, None, None))

            updated_tuple = original_tuple + (is_reverted, reversion, reversion_timestamp, revision_id_reversion)
            
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
    # @staticmethod
    # def create_property_replacement_features(c1, c2):
    #     """Create features for property replacements"""

    #     # Save pair of create/delete where the value is the same and the property is different
    #     # for each pair, save: user_id, property_label (need to keep track of this for text similarity), timestamp, action
        
    #     if c1[9] == 'DELETE':
    #         delete_change = c1
    #         create_change = c2
    #     else:
    #         delete_change = c2
    #         create_change = c1

    #     if c1[19] == c2[19]:
    #         same_user = 1
    #     else:
    #         same_user = 0

    #     create_change_dt = datetime.strptime(create_change[13].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
    #     delete_change_dt = datetime.strptime(delete_change[13].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
        
    #     if delete_change_dt > create_change_dt: # so it's positive time
    #         time_diff = (delete_change_dt - create_change_dt).total_seconds() 
    #     else:
    #         time_diff = (create_change_dt - delete_change_dt).total_seconds()

    #     same_day = int(create_change_dt.date() == delete_change_dt.date())
    #     same_hour = int(create_change_dt.hour == delete_change_dt.hour)
    #     same_revision = int(delete_change[0] == create_change[0])
    #     delete_before_create = int(delete_change_dt < create_change_dt)

    #     features = (
            
    #         delete_change[0],
    #         delete_change[1],
    #         delete_change[3],
    #         delete_change[8],

    #         create_change[0],
    #         create_change[1],
    #         create_change[3],
    #         create_change[8],
            
    #         time_diff,
    #         same_day,
    #         same_hour,
    #         same_revision,
    #         delete_before_create,
    #         same_user,
    #         0.0, # property label similarity

    #         delete_change[13],
    #         create_change[13],

    #         delete_change[2],
    #         create_change[2],
    #         delete_change[19],
    #         create_change[19],
            
    #         '',
    #     )

    #     return features

    
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
                LIMIT {batch_size}
        """
        
        num_batches = 0

        embedding_cols_str = ', '.join(embedding_cols)
        key_cols_str = ', '.join(key_cols)
        select_cols_str = ', '.join(select_cols)
        
        key_cols_temp = ', '.join([f'{col} {col_type}' for col, col_type in BASE_KEY_TYPES.items()])

        cursor = self.conn.cursor()
        cursor.execute(f"CREATE TEMP TABLE temp_results_{datatype}{table_prefix} ({key_cols_temp}, {', '.join([f'{col} FLOAT' for col in embedding_cols])})")
        self.conn.commit()

        # load model
        model = SentenceTransformer('all-MiniLM-L6-v2')

        while True:

            if max_batches:
                print(f'Processing batch {num_batches}/{max_batches} for embedding features update', flush=True)

            if max_batches and num_batches >= max_batches:
                print(f'Reached max_batches limit ({max_batches}), stopping', flush=True)
                break
            
            if not os.path.exists(f'{DATA_PATH}/{datatype}{table_prefix}/chunk_{num_batches}.csv'):

                query = base_query.format(
                    key_cols_str = key_cols_str,
                    select_cols_str=select_cols_str,
                    embedding_cols_str=embedding_cols_str,
                    embedding_cols_str_filter = f' OR '.join([f'({col} IS NULL OR {col} = 0.0)' for col in embedding_cols]),
                    datatype=datatype,
                    table_prefix=table_prefix,
                    batch_size=batch_size
                )
            
                df = query_to_df(self.conn, query)
                
                if len(df) == 0:
                    break

                result = self.create_embedding_features(model, df, old_col='old_value', new_col='new_value')
                result = result[[*key_cols, *embedding_cols]]

                buffer = io.StringIO()
                result.to_csv(buffer, index=False, header=False, sep=';', quoting=csv.QUOTE_ALL, escapechar='\\')
                buffer.seek(0)
                cursor.copy_expert(f"COPY temp_results_{datatype}{table_prefix} FROM STDIN (FORMAT CSV, DELIMITER ';', QUOTE '\"', ESCAPE '\\')", buffer)

            else:
                batch_file = f'{DATA_PATH}/{datatype}{table_prefix}/chunk_{num_batches}.csv'
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

            num_batches += 1

        print(f'Created {num_batches} batches for embedding features update', flush=True)

        cursor.execute(f"DROP TABLE temp_results_{datatype}{table_prefix}")
        self.conn.commit()

        final_end_time = time.time() - main_start_time
        final_time, unit = get_time_unit(final_end_time)
        print(f'Finished creating and updating embedding features for {datatype} in {final_time} {unit}', flush=True)

    
    def update_label_description_entity_features(self, table_suffix):
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
            print(f'Updating {suffix}_value_label, {suffix}_value_description in the features_entity{table_suffix}', flush=True)
            start_time = time.time()

            cursor.execute(f"""
                UPDATE features_entity{table_suffix} fe
                SET 
                    {suffix}_value_label =
                        CASE 
                            WHEN elad.label IS NOT NULL AND elad.label <> '' THEN elad.label
                            ELSE elad.alias 
                        END
                    ,
                    {suffix}_value_description = elad.description
                FROM entity_labels_alias_description elad
                WHERE elad.qid::TEXT = fe.{suffix}_value->>0 AND (fe.{suffix}_value_label IS NULL OR fe.{suffix}_value_label = '')
            """)
            
            self.conn.commit()

            elapsed_time = time.time() - start_time
            final_time, unit = get_time_unit(elapsed_time)

            print(f'Finished updating {suffix}_value_label and {suffix}_value_description in {final_time} {unit}', flush=True)


    def create_all_features_entity(self, table_suffix, max_batches=None):
        
        # transitive closure 
        self.transitive_cache = TransitiveClosureCache()

        datatype = 'entity'

        select_cols_str = ', '.join([
            'old_value', 'new_value',
            'old_value_label', 'new_value_label',
            'old_value_description', 'new_value_description'
        ])

        ENTITY_ONLY_FEATURES_COLS = list(ENTITY_ONLY_FEATURES_COLS_TYPES.keys())
        feature_cols_str = ', '.join(ENTITY_ONLY_FEATURES_COLS)

        key_cols = ['revision_id', 'property_id', 'value_id', 'change_target']
        key_cols_str = ', '.join(key_cols)

        batch_size = 100000
        num_batches = 0

        cursor = self.conn.cursor()

        key_cols_temp = ', '.join([f'{col} {col_type}' for col, col_type in BASE_KEY_TYPES.items()])
        cursor.execute(f"CREATE TEMP TABLE temp_results_{datatype}{table_suffix} ({key_cols_temp}, {', '.join([f'{col} {col_type}' for col, col_type in ENTITY_ONLY_FEATURES_COLS_TYPES.items()])})")
        self.conn.commit()

        # load model for embedding features
        model = SentenceTransformer('all-MiniLM-L6-v2')

        start_time = time.time()
        
        while True:

            if max_batches and num_batches >= max_batches:
                print(f'Reached max_batches limit ({max_batches}), stopping', flush=True)
                break

            query = """
                SELECT {key_cols_str}, {select_cols_str}, {feature_cols_str}
                    FROM features_entity{table_suffix}
                    WHERE 
                        (label IS NULL or label = '') and processed = FALSE
                    LIMIT {batch_size}
            """.format(
                key_cols_str=key_cols_str,
                select_cols_str=select_cols_str,
                feature_cols_str=feature_cols_str,
                table_suffix=table_suffix,
                batch_size=batch_size
            )
            df = query_to_df(self.conn, query)
            
            if len(df) == 0:
                break
            
            # --------------- create text features + transitive closure features ---------------
            df[ENTITY_ONLY_FEATURES_COLS] = df.apply(
                lambda row: self.create_entity_features_text_transitive(row, self.transitive_cache),
                axis=1
            )

            integer_cols = []
            for col, type_ in ENTITY_ONLY_FEATURES_COLS_TYPES.items():
                if type_ == 'INT':
                    integer_cols.append(col)

            for col in integer_cols:
                if col in df.columns:
                    df[col] = df[col].fillna(0).astype(int)
            
            # --------------- create embedding features ---------------
            result = self.create_embedding_features(model, df, old_col='old_value_label', new_col='new_value_label')

            result = df[[*key_cols, *ENTITY_ONLY_FEATURES_COLS]]

            buffer = io.StringIO()
            result.to_csv(buffer, index=False, header=False, sep=';', quoting=csv.QUOTE_ALL, escapechar='\\')
            buffer.seek(0)
            cursor.copy_expert(f"COPY temp_results_{datatype}{table_suffix} FROM STDIN (FORMAT CSV, DELIMITER ';', QUOTE '\"', ESCAPE '\\')", buffer)

            del result
            del df

            # --------------- Updating feature table ---------------
            print('Updating feature table', flush=True)

            cursor.execute(f"""
                UPDATE features_{datatype}{table_suffix} f
                SET {', '.join([f'{col} = tp.{col}' for col in ENTITY_ONLY_FEATURES_COLS])}, processed = TRUE
                FROM temp_results_{datatype}{table_suffix} tp
                WHERE 
                    (f.label = '' OR f.label IS NULL) AND
                    {' AND '.join([f'f.{col} = tp.{col}' if col != 'change_target' else f"COALESCE(f.{col}, '') = COALESCE(tp.{col}, '')" for col in key_cols])}
            """)

            cursor.execute(f"TRUNCATE TABLE temp_results_{datatype}{table_suffix}")

            self.conn.commit()
            
        elapsed_time = time.time() - start_time
        final_time, unit = get_time_unit(elapsed_time)
        print(f'Finished entity feature creation {final_time} {unit}', flush=True)    

        cursor.execute(f"DROP TABLE temp_results_{datatype}{table_suffix}")

        self.conn.commit()


    def create_remaining_features(self, datatype, table_suffix, max_batches=None):
        """
        Creates missing features for the given datatype and table.
        
        :param datatype: can be one of 'entity', 'property_replacement', 'quantity', 'time', 'text', 'globecoordinate'
        :param table_suffix: can be one of '_sa', '_ao', '_less'
        :param max_batches: maximum number of batches to process (optional)
        """
        print('Creating missing features for datatype:', datatype, flush=True)

        if datatype not in ['entity', 'quantity', 'time', 'text', 'globecoordinate']:
            print('Unsupported datatype for embedding features. Has to be one of: entity, quantity, time, text, globecoordinate. Input datatype:', datatype, flush=True)
            return

        if table_suffix not in ['_sa', '_ao', '_less', '']:
            print('Unsupported table suffix for embedding features. Has to be one of _sa, _ao, _less. Input table suffix:', table_suffix, flush=True)
            return

        if datatype == 'entity':
            self.create_all_features_entity(table_suffix, max_batches=max_batches)
        elif datatype == 'text':
            key_cols = ['revision_id', 'property_id', 'value_id', 'change_target']
            select_cols = ['old_value', 'new_value']
            embedding_cols = ['value_cosine_similarity']

            self.create_and_update_embedding_features(datatype, key_cols, select_cols, embedding_cols, table_suffix, max_batches=max_batches)
    
