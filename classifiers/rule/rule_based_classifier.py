import re
import decimal
from decimal import Decimal, ROUND_HALF_UP
import json
from datetime import datetime
import time
import io
import csv
import pandas as pd
import numpy as np

from parser_scripts.const import STOP_WORDS, BASE_KEY_TYPES
from parser_scripts.utils import strip_accents, query_to_df
from classifiers.rule.transitive_closure_cache import TransitiveClosureCache


class RuleBasedClassifier():

    def __init__(self, set_up=None, conn=None):
        self.set_up = set_up
        self.conn = conn
        self.rule_base_time = 0

    ####################
    # Text classification
    ####################
    @staticmethod
    def classify_text_changes(old_value, new_value):
        def formatting_residual_check(old_value, new_value):
            """Checks whether the entire difference is explainable by case,
            accent, special-char, and/or whitespace changes alone (any
            combination). Returns the set of active labels if fully explained,
            None if real content differs."""
            active = set()

            # counts of accented characters in old and new values
            accented_old = sum(1 for c in old_value if strip_accents(c) != c)
            accented_new = sum(1 for c in new_value if strip_accents(c) != c)
            if accented_old != accented_new:  # if they differ, there was a change in accents
                active.add('textual_change')
            old_no_accent = strip_accents(old_value)
            new_no_accent = strip_accents(new_value)

            # Unicode-aware special-char count: a character only counts as
            # "special" if it's neither alphanumeric nor whitespace.
            special_old = sum(1 for c in old_no_accent if not (c.isalnum() or c.isspace()))
            special_new = sum(1 for c in new_no_accent if not (c.isalnum() or c.isspace()))
            if special_old != special_new:
                active.add('textual_change')

            # remove special characters (non alphanumeric and non whitespace) for whitespace comparison
            old_stripped = ''.join(c for c in old_no_accent if c.isalnum() or c.isspace())
            new_stripped = ''.join(c for c in new_no_accent if c.isalnum() or c.isspace())

            # counts whitespace
            if len(re.findall(r'\s', old_stripped)) != len(re.findall(r'\s', new_stripped)):
                active.add('textual_change')

            # remove whitespace
            old_nospace = re.sub(r'\s', '', old_stripped)
            new_nospace = re.sub(r'\s', '', new_stripped)
            if old_nospace.lower() == new_nospace.lower() and old_nospace != new_nospace:
                active.add('textual_change')
            if old_nospace.lower() == new_nospace.lower():
                return active if active else None
            
            return None

        old_value = str(old_value).strip().replace('"', '')
        new_value = str(new_value).strip().replace('"', '')
        
        resolved = formatting_residual_check(old_value, new_value)
        if resolved is not None:
            return ','.join(sorted(resolved))
        
        def _is_stopword_only(diff_text):
            words = diff_text.strip().split()
            return len(words) > 0 and all(w.lower() in STOP_WORDS for w in words)

        #  REFINEMENT
        if new_value.startswith(old_value + " ") and old_value != new_value:
            added = new_value[len(old_value):].strip()
            if not _is_stopword_only(added):
                return 'refinement'
            else:
                return 'textual_change'
        elif new_value.endswith(" " + old_value) and old_value != new_value:
            added = new_value[:len(new_value) - len(old_value)].strip()
            if not _is_stopword_only(added):
                return 'refinement'
            else:
                return 'textual_change'

        # UNREFINEMENT
        if old_value.startswith(new_value + " ") and new_value != old_value:
            removed = old_value[len(new_value):].strip()
            if not _is_stopword_only(removed):
                return 'unrefinement'
            else:
                return 'textual_change'
        elif old_value.endswith(" " + new_value) and new_value != old_value:
            removed = old_value[:len(old_value) - len(new_value)].strip()
            if not _is_stopword_only(removed):
                return 'unrefinement'
            else:
                return 'textual_change'

        return ''
    
    ####################
    # Time classification
    ####################
    @staticmethod
    def classify_time_changes(old_value, new_value):

        # for JSONB storage
        old_value = str(old_value).strip().replace('"', '')
        new_value = str(new_value).strip().replace('"', '')
        
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

        def combine(matches):
            """Collapse a list of (label, branch_id) matches into a single
            (label, branch) pair. If every match agrees on the label, join
            their branch_ids under that one label. If matches disagree (a
            genuinely mixed edit - e.g. year added [refinement] and day
            removed [unrefinement] in the same revision), it's prop value update, branches remain."""
            if not matches:
                return '', ''
            distinct_labels = sorted(set(m[0] for m in matches))
            if len(distinct_labels) == 1:
                return distinct_labels[0], ','.join(m[1] for m in matches)
            
            if 'refinement' in distinct_labels and 'unrefinement' in distinct_labels:
                # "+0-05-04T00:00:00Z"	-> "+2000-00-00T00:00:00Z" -> month and day get added, year gets removed
                # "+0-01-01T00:00:00Z"	"+1-00-00T00:00:00Z" -> year gets added, month and day get removed
                return 'property_value_update', ','.join(f"{m[1]}" for m in matches)

            return ','.join(distinct_labels), ','.join(f"{m[1]}" for m in matches)

        def reformatting_check(value):
            stripped = re.sub(r'[^\d+\-T:Z]', '', value)
            _MALFORMED_TIMESTAMP_RE = re.compile(r'^([+-])(\d+)-(\d{1,2})-(\d{1,2})T(.*)$')
            match = _MALFORMED_TIMESTAMP_RE.match(stripped)
            if not match:
                return None
            sign, year, month, day, time_of_day = match.groups()
            return sign, int(year), int(month), int(day), time_of_day

        stripped_old = reformatting_check(old_value)
        stripped_new = reformatting_check(new_value)
        if stripped_new is not None and stripped_old is not None and stripped_new == stripped_old and old_value != new_value:
            return 're_formatting', 're_formatting'
        
        # sign change -> property_value_update and precedes any other change
        if old_value[0] in ['+', '-'] and new_value[0] in ['+', '-']:
            old_sign, new_sign = old_value[0], new_value[0]
            if old_sign != new_sign:
                return 'property_value_update', 'sign_change'
        elif old_value[0] in ['+', '-'] and new_value[0] not in ['+', '-']:
            old_sign, new_sign = old_value[0], '+'
            if old_sign != new_sign:
                return 'property_value_update', 'sign_change'
        elif old_value[0] not in ['+', '-'] and new_value[0] in ['+', '-']:
            old_sign, new_sign = '+', new_value[0]
            if old_sign != new_sign:
                return 'property_value_update', 'sign_change'

        old_date = get_date_parts(old_value, 'date')
        new_date = get_date_parts(new_value, 'date')

        old_year, old_month, old_day = old_date
        new_year, new_month, new_day = new_date

        # ---------------------------------------
        # VALUE UPDATE: any of year/month/day changes
        # ---------------------------------------
        # check they are not 0 because that's refinement/unrefinement
        if old_year != new_year and old_year != 0 and new_year != 0:
            return 'property_value_update', 'different_year'

        if old_month != new_month and old_month > 0 and new_month > 0:
            return 'property_value_update', 'different_month'

        if old_day != new_day and old_day > 0 and new_day > 0:
            return 'property_value_update', 'different_day'

        date_matches = []  # list of (label, branch_id) - every date-level rule that fired
        time_matches = []  # list of (label, branch_id) - every time-level rule that fired

        # ---------------------------------------
        # REFINEMENT
        # ---------------------------------------
        # if the year is added
        if (old_year == 0 and new_year != 0):
            date_matches.append(('refinement', 'added_year'))

        # if the month is added
        if (old_month == 0 and new_month > 0):
            date_matches.append(('refinement', 'added_month'))

        # if the day is added
        if (old_day == 0 and new_day > 0):
            date_matches.append(('refinement', 'added_day'))

        # ---------------------------------------
        # UNREFINEMENT
        # ---------------------------------------
        # if the year is removed
        if (old_year != 0 and new_year == 0):
            date_matches.append(('unrefinement', 'removed_year'))

        # if the month is removed
        if (old_month > 0 and new_month == 0):
            date_matches.append(('unrefinement', 'removed_month'))

        # if the day is removed
        if (old_day > 0 and new_day == 0):
            date_matches.append(('unrefinement', 'removed_day'))

        old_time = get_date_parts(old_value, 'time')
        new_time = get_date_parts(new_value, 'time')

        old_hour, old_minute, old_second = old_time
        new_hour, new_minute, new_second = new_time

        # ---------------------------------------
        # VALUE UPDATE: any of hour/minute/second changes
        # ---------------------------------------
        # check they are not 0 because that's refinement/unrefinement
        if old_hour != new_hour and old_hour > 0 and new_hour > 0:
            time_matches.append(('property_value_update', 'different_hour'))

        if old_minute != new_minute and old_minute > 0 and new_minute > 0:
            time_matches.append(('property_value_update', 'different_minute'))

        if old_second != new_second and old_second > 0 and new_second > 0:
            time_matches.append(('property_value_update', 'different_second'))

        # ---------------------------------------
        # REFINEMENT
        # ---------------------------------------
        # if the hour is added
        if (old_hour == 0 and new_hour > 0):
            time_matches.append(('refinement', 'added_hour'))

        # if the minute is added
        if (old_minute == 0 and new_minute > 0):
            time_matches.append(('refinement', 'added_minute'))

        # if the second is added
        if (old_second == 0 and new_second > 0):
            time_matches.append(('refinement', 'added_second'))

        # ---------------------------------------
        # UNREFINEMENT
        # ---------------------------------------
        # if the hour is removed
        if (old_hour > 0 and new_hour == 0):
            time_matches.append(('unrefinement', 'removed_hour'))

        # if the minute is removed
        if (old_minute > 0 and new_minute == 0):
            time_matches.append(('unrefinement', 'removed_minute'))

        # if the second is removed
        if (old_second > 0 and new_second == 0):
            time_matches.append(('unrefinement', 'removed_second'))

        date_label, date_branch = combine(date_matches)
        time_label, time_branch = combine(time_matches)

        final_label = ''
        final_branch = ''
        
        if date_label != '':
            final_label += date_label
            final_branch += date_branch

        if time_label != '':
            if final_label != '':
                final_label += ','
                final_branch += ','
            final_label += time_label
            final_branch += time_branch

        return final_label, final_branch
    
    ####################
    # Quantity classification
    ####################
    @staticmethod
    def classify_numeric_changes(old_value, new_value):

        if ('e' in old_value.lower() or old_value.lower() in ('inf', '-inf', 'nan')) or \
            ('e' in new_value.lower() or new_value.lower() in ('inf', '-inf', 'nan')):
            print(f"Scientific notation or special value detected: old_value={old_value}, new_value={new_value}")
            return 'property_value_update', 'scientific_notation_or_special_value'

        def _round_half_up(value_str, places):
            quantum = Decimal(1).scaleb(-places)
            with decimal.localcontext() as ctx:
                ctx.prec = max(len(value_str), places, 28) + 10
                return Decimal(value_str).quantize(quantum, rounding=ROUND_HALF_UP)

        # VALUE UPDATE: sign change, whole value change, old_value and new_value aren't prefixes of each other nor rounded versions of each other
        if old_value[0] in ['+', '-'] and new_value[0] in ['+', '-']:
            old_sign = old_value[0]
            new_sign = new_value[0]

            if old_sign != new_sign:
                return 'property_value_update', 'sign_change'
            elif old_sign == new_sign and old_value[1:] == new_value[1:]:
                return 're_formatting', 're_format_sign_change'

        elif old_value[0] in ['+', '-'] and new_value[0] not in ['+', '-']:
            old_sign = old_value[0]
            new_sign = '+'

            if old_sign != new_sign:
                return 'property_value_update', 'sign_change'
            elif old_sign == new_sign and old_value[1:] == new_value:
                    return 're_formatting', 're_format_sign_change'
        elif old_value[0] not in ['+', '-'] and new_value[0] in ['+', '-']:
            old_sign = '+'
            new_sign = new_value[0]

            if old_sign != new_sign:
                return 'property_value_update', 'sign_change'
            elif old_sign == new_sign and old_value == new_value[1:]:
                return 're_formatting', 're_format_sign_change'

        old_whole_value = int(old_value.split('.')[0]) if '.' in old_value else int(old_value)
        new_whole_value = int(new_value.split('.')[0]) if '.' in new_value else int(new_value)

        old_decimals = old_value.split('.')[1] if '.' in old_value else ''
        new_decimals = new_value.split('.')[1] if '.' in new_value else ''

        if old_whole_value != new_whole_value and old_decimals == '' and new_decimals == '':
            return 'property_value_update', 'different_whole_value'

        # NOTE: if the whole value is the same but the decimal part is different, we can have a reformatting, refinement or unrefinement

        # RE-FORMATTING
        # simple .0 addition/removal
        # 9 -> 9.0
        if (old_whole_value == new_whole_value and old_decimals == '' and new_decimals != '' and int(new_decimals) == 0):
            return 're_formatting', 'added_zero_decimal'
        if (old_whole_value == new_whole_value and old_decimals != '' and int(old_decimals) == 0 and new_decimals == ''):  # 9.0 -> 9
            return 're_formatting', 'removed_zero_decimal'

        if (old_whole_value == new_whole_value and old_decimals == '' and new_decimals != '' and int(new_decimals) > 0):  # old_value gets an added decimal part
            return 'refinement', 'added_decimal_part'
        if (old_whole_value == new_whole_value and old_decimals != '' and int(old_decimals) > 0 and new_decimals == ''):  # new_value gets a deleted part
            return 'unrefinement', 'deleted_decimal_part'

        # addition of 0 at the end of the existing decimal part
        # e.g., +9.12 <-> +9.120; +9.120 <-> +9.1200; etc.
        if old_whole_value == new_whole_value and old_decimals != '' and new_decimals != '' and \
            old_decimals != new_decimals and \
            (len(old_decimals) > len(new_decimals) and old_decimals.startswith(new_decimals) and int(old_decimals[len(new_decimals):]) == 0):
                return 're_formatting', 'removed_trailing_zeroes'

        if old_whole_value == new_whole_value and old_decimals != '' and new_decimals != '' and \
            old_decimals != new_decimals and \
            (len(new_decimals) > len(old_decimals) and new_decimals.startswith(old_decimals) and int(new_decimals[len(old_decimals):]) == 0):
            return 're_formatting', 'added_trailing_zeroes'

        # REFINEMENT: old value is prefix of new one, old_value is rounded version of new_value
        # UNREFINEMENT: new value is prefix of old one, new_value is rounded version of old_value
        if (old_decimals != '' or new_decimals != ''):
            # new value is prefix of old one and it's not rounded
            if old_whole_value == new_whole_value and (len(old_decimals) > len(new_decimals) and old_decimals.startswith(new_decimals)):
                return 'unrefinement', 'prefix_truncation'
            # old value is prefix of new one and it's not rounded
            elif old_whole_value == new_whole_value and (len(new_decimals) > len(old_decimals) and new_decimals.startswith(old_decimals)):
                return 'refinement', 'prefix_extension'

            # Remove zero decimals in case of 9.5 <-> 10.0 
            # since in this case the rounding needs to be to 0 places to get 10
            new_effective_places = len(new_decimals.rstrip('0'))
            old_effective_places = len(old_decimals.rstrip('0'))

            if new_effective_places < old_effective_places and _round_half_up(old_value, new_effective_places) == Decimal(new_value):
                return 'unrefinement', 'rounding'
            if old_effective_places < new_effective_places and _round_half_up(new_value, old_effective_places) == Decimal(old_value):
                return 'refinement', 'rounding'

            # if the whole value is the same but the decimal part is different (no truncation or rounding up)
            if old_whole_value == new_whole_value:
                return 'property_value_update', 'different_decimal_part'

            # case for different whole value when you still have decimal part 
            return 'property_value_update', 'different_whole_value'
        
        return '', ''

    @staticmethod
    def classify_quantity_changes(old_value, new_value):

        old_value = str(old_value).strip().replace('"', '').replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()
        new_value = str(new_value).strip().replace('"', '').replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()

        return RuleBasedClassifier.classify_numeric_changes(old_value, new_value)

    ####################
    # Globecoordinate classification
    ####################
    @staticmethod
    def classify_globecoordinate_changes(old_value, new_value):

        old_val = json.loads(old_value, parse_float=str, parse_int=str)
        new_val = json.loads(new_value, parse_float=str, parse_int=str)

        new_val['latitude'] = str(new_val['latitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()
        new_val['longitude'] = str(new_val['longitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()

        old_val['latitude'] = str(old_val['latitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()
        old_val['longitude'] = str(old_val['longitude']).replace('\\n', '').replace('\r', '').replace('\n', '').replace('\t', '').strip()

        # NOTE: It can happen that only one of the 2 changes
        latitude_label = ''
        latitude_branch = ''
        if old_val['latitude'] != new_val['latitude']:
            latitude_label, latitude_branch = RuleBasedClassifier.classify_numeric_changes(old_val['latitude'], new_val['latitude'])

        longitude_label = ''
        longitude_branch = ''
        if old_val['longitude'] != new_val['longitude']:
            longitude_label, longitude_branch = RuleBasedClassifier.classify_numeric_changes(old_val['longitude'], new_val['longitude'])

        return latitude_label, latitude_branch,  longitude_label, longitude_branch
        
    ####################
    # Reverted edit classification
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
                value_id = change_info[2]
                change_target = change_info[7]

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
                            
                            revert_flags[future_key] = (1, 1, revert_flags[future_key][2], revert_flags[future_key][3])
                            
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
                            ((current_change['action'] == 'UPDATE' and future_change['action'] == 'CREATE') or \
                                (current_change['action'] == 'UPDATE' and future_change['action'] == 'UPDATE')):

                                for inter_change in next_changes[:j]: # go up to future_change, but not including it
                                    
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
        final_rank_changes = []
        
        for key, original_tuple in dict_lookup.items():

            if key[3] == 'rank':
                # need to get corresponding value change for rank
                value_key = (key[0], key[1], key[2], '')
                is_reverted, reversion, reversion_timestamp, revision_id_reversion  = revert_flags.get(value_key, (0, 0, None, None))

                # remove change_target since I split value changes and rank changes into different tables
                # also remove branch, new_datatype and old_datatype
                original_tuple_rank = original_tuple[:5] + original_tuple[8:11] + original_tuple[12:]
                updated_tuple = original_tuple_rank + (is_reverted, reversion, reversion_timestamp, revision_id_reversion)
                final_rank_changes.append(updated_tuple)
                
            else:
                is_reverted, reversion, reversion_timestamp, revision_id_reversion = revert_flags.get(key, (0, 0, None, None))
            
                # remove change_target since I split value changes and rank changes into different tables
                original_tuple_no_change_target = original_tuple[:7] + original_tuple[8:]
                updated_tuple = original_tuple_no_change_target + (is_reverted, reversion, reversion_timestamp, revision_id_reversion)
                final_value_changes.append(updated_tuple)

        entity_stats['num_reverted_edits'] = num_reverted_edits
        entity_stats['num_reversions'] = num_reversions
        entity_stats['num_reverted_edits_create'] = num_reverted_edits_create
        entity_stats['num_reverted_edits_delete'] = num_reverted_edits_delete
        entity_stats['num_reverted_edits_update'] = num_reverted_edits_update

        return final_value_changes, final_rank_changes, entity_stats

    ####################
    # Entity classification
    ####################
    @staticmethod
    def classify_entity_changes(row):

        old_value = str(row['old_value']).strip().replace('"', '')
        new_value = str(row['new_value']).strip().replace('"', '')

        old_value_label = str(row['old_value_label']).strip().replace('"', '')
        new_value_label = str(row['new_value_label']).strip().replace('"', '')

        if old_value != new_value and old_value_label == new_value_label:
            return 'property_value_update'

        #  NOTE: "has part(s)" is the inverse property of "part of" (P361)

        if row['old_value_subclass_new_value'] == 1 or \
            row['old_value_located_in_new_value'] == 1 or \
            row['old_value_part_of_new_value'] == 1 or \
            row['new_value_has_parts_old_value'] == 1 :
            return 'unrefinement'
        
        if row['new_value_subclass_old_value'] == 1 or \
            row['new_value_located_in_old_value'] == 1 or \
            row['new_value_part_of_old_value'] == 1 or \
            row['old_value_has_parts_new_value'] == 1 :
            return 'refinement'

        return ''
    
    ########################################################################################################################
    # Entity features
    ########################################################################################################################
         
    def classify_entity_change_rb(self, row, transitive_cache):
        """
            Classify entity changes via rule based and if not extract features for entity using labels + description
            To classify rule-based I need labels and transitive closure features
        """
        new_value = row['new_value'].replace('"', '').strip()
        old_value = row['old_value'].replace('"', '').strip()

        old_value_label = row['old_value_label']
        new_value_label = row['new_value_label']

        features_for_rb = {
            'old_value': row['old_value'],
            'new_value': row['new_value'],
            'old_value_label': old_value_label,
            'new_value_label': new_value_label,
            'old_value_subclass_new_value': transitive_cache.check(old_value, new_value, 'subclass_transitive'),
            'new_value_subclass_old_value': transitive_cache.check(new_value, old_value, 'subclass_transitive'),
            'old_value_located_in_new_value': transitive_cache.check(old_value, new_value, 'located_in_transitive'),
            'new_value_located_in_old_value': transitive_cache.check(new_value, old_value, 'located_in_transitive'),
            'old_value_has_parts_new_value': transitive_cache.check(old_value, new_value, 'has_part_transitive'),
            'new_value_has_parts_old_value': transitive_cache.check(new_value, old_value, 'has_part_transitive'),
            'old_value_part_of_new_value': transitive_cache.check(old_value, new_value, 'part_of_transitive'),
            'new_value_part_of_old_value': transitive_cache.check(new_value, old_value, 'part_of_transitive')
        }
        start_time = time.perf_counter()
        rb_label = self.classify_entity_changes(features_for_rb)
        endtime = time.perf_counter()
        elapsed_time = endtime - start_time
        self.rule_base_time += elapsed_time

        return rb_label
    

    def _load_gs_lookup(self, datatype):
        """
            Loads the gold standard labels once and caches them in memory,
            keyed by (old_value, new_value) -> label.
            Rows matching this lookup already have a known-correct label,
            so they can skip rule-based classification and embedding/NLI
            feature computation entirely.
        """
        cache_attr = f'_gs_lookup_{datatype}'
        cached = getattr(self, cache_attr, None)
        if cached is not None:
            return cached

        gs_path = f'classifiers/ml/training_dataset/gs_{datatype}.csv'
        gs_df = pd.read_csv(gs_path)

        gs_df['old_value'] = gs_df['old_value'].astype(str)
        gs_df['new_value'] = gs_df['new_value'].astype(str)
        gs_df['old_value'] = gs_df['old_value'].astype(str).str.strip().str.strip('"')
        gs_df['new_value'] = gs_df['new_value'].astype(str).str.strip().str.strip('"')

        lookup = gs_df.set_index(['old_value', 'new_value'])['label'].to_dict()
        setattr(self, cache_attr, lookup)
        return lookup
    
    def _split_by_gs(self, df, gs_lookup):
        """
            Splits a batch DataFrame into rows already covered by the gold
            standard (df_gs) and rows that still need full feature
            computation + inference (df_remaining).
        """
        keys = list(zip(
            df['old_value'].astype(str),
            df['new_value'].astype(str)
        ))
        gs_labels = pd.Series([gs_lookup.get(k) for k in keys], index=df.index)

        is_gs = gs_labels.notna()
        df_gs = df[is_gs].copy()
        df_gs['_gs_label'] = gs_labels[is_gs]

        df_remaining = df[~is_gs].copy()

        return df_gs, df_remaining
    
    
    def update_label_description_entity_features(self, table_suffix):
        """
        Update new_value_label, new_value_description, old_value_label, old_value_description for entity changes
        so we can calculate the features using these values
        """
        if not self.conn:
            print('No DB connection available', flush=True)
            return

        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entity_stats_all AS
                SELECT qid, entity_label, entity_description FROM entity_stats
                UNION ALL
                SELECT qid, entity_label, entity_description FROM entity_stats_sa
                UNION ALL
                SELECT qid, entity_label, entity_description FROM entity_stats_ao
                UNION ALL
                SELECT qid, entity_label, entity_description FROM entity_stats_less;

            CREATE INDEX IF NOT EXISTS idx_entity_stats_all_qid ON entity_stats_all (qid);
            ANALYZE entity_stats_all;
        """)
        self.conn.commit()

        old_new = ['old', 'new']

        for suffix in old_new:
            print(f'Updating {suffix}_value_label, {suffix}_value_description in the updates_entity{table_suffix}_full', flush=True)
            start_time = time.perf_counter()

            cursor.execute(f"""
                UPDATE updates_entity{table_suffix}_full fe
                SET 
                    {suffix}_value_label = es.entity_label,
                    {suffix}_value_description = es.entity_description
                FROM 
                    entity_stats_all es
                WHERE es.qid = fe.{suffix}_value->>0 AND (fe.{suffix}_value_label = '' OR fe.{suffix}_value_label IS NULL);
            """)
            
            self.conn.commit()

            elapsed_time = time.perf_counter() - start_time

            print(f'Finished updating {suffix}_value_label and {suffix}_value_description in {elapsed_time} seconds', flush=True)


    def entity_rb_classification(self, table_suffix):

        if table_suffix not in ['_sa', '_ao', '_less', '']:
            print('Unsupported table suffix for embedding features. Has to be one of _sa, _ao, _less. Input table suffix:', table_suffix, flush=True)
            return
        
        datatype = 'entity'
        
        # transitive closure 
        self.transitive_cache = TransitiveClosureCache()

        gs_lookup = self._load_gs_lookup(datatype)

        select_cols_str = ', '.join([
            'old_value', 'new_value',
            'old_value_label', 'new_value_label'
        ])

        key_cols = ['revision_id', 'property_id', 'value_id']
        key_cols_str = ', '.join(key_cols)

        batch_size = 1000000

        cursor = self.conn.cursor()

        key_cols_temp = ', '.join([f'{col} {col_type}' for col, col_type in BASE_KEY_TYPES.items()])
        try:
            cursor.execute(f"ALTER TABLE updates_{datatype}{table_suffix} DROP COLUMN IF EXISTS label, ADD COLUMN IF NOT EXISTS label TEXT DEFAULT NULL;")
            cursor.execute(f"ALTER TABLE updates_{datatype}{table_suffix} DROP COLUMN IF EXISTS rb, ADD COLUMN IF NOT EXISTS rb BOOLEAN DEFAULT FALSE;")
            cursor.execute(f"ALTER TABLE updates_{datatype}{table_suffix} DROP COLUMN IF EXISTS gs, ADD COLUMN IF NOT EXISTS gs BOOLEAN DEFAULT FALSE;")
            cursor.execute(f"ALTER TABLE updates_{datatype}{table_suffix} DROP COLUMN IF EXISTS processed, ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT FALSE;")
            self.conn.commit()
        except Exception as e:
            print(f"Error altering table updates_{datatype}{table_suffix}: {e}", flush=True)
            self.conn.rollback()
            return
        cursor.execute(f"CREATE TEMP TABLE temp_results_{datatype}{table_suffix} ({key_cols_temp}, label TEXT, gs BOOLEAN DEFAULT FALSE, rb BOOLEAN DEFAULT FALSE);")
        self.conn.commit()

        start_time = time.perf_counter()

        while True:

            query = """
                SELECT {key_cols_str}, {select_cols_str}
                    FROM updates_entity{table_suffix}
                    WHERE 
                        processed = FALSE
                    LIMIT {batch_size}
            """.format(
                key_cols_str=key_cols_str,
                select_cols_str=select_cols_str,
                table_suffix=table_suffix,
                batch_size=batch_size
            )
            df = query_to_df(self.conn, query)

            df['rb'] = False
            df['gs'] = False
            
            if len(df) == 0:
                print('No more unprocessed rows found, exiting loop', flush=True)
                break

            df_gs, df_remaining = self._split_by_gs(df, gs_lookup)

            if len(df_gs) > 0:
                print(f'Found {len(df_gs)} rows in gold standard, label already set', flush=True)
                df_gs['label'] = df_gs['_gs_label']
                df_gs['gs'] = True

            if len(df_remaining) > 0:
                print(f'Found {len(df_remaining)} rows not in gold standard, doing Rule-Based Classification', flush=True)
                # --------------- DO RB CLASSIFICATION ---------------
                df_remaining['label'] = df_remaining.apply(
                    lambda row: self.classify_entity_change_rb(row, self.transitive_cache),
                    axis=1,
                    result_type='expand'
                )

                df_remaining['rb'] = np.where(df_remaining['label'] != '', True, False)
            
            result_gs = df_gs[[*key_cols, 'label', 'gs', 'rb']] if len(df_gs) > 0 else pd.DataFrame(columns=[*key_cols, 'label'])
            result_remaining = df_remaining[[*key_cols, 'label', 'gs', 'rb']] if len(df_remaining) > 0 else pd.DataFrame(columns=[*key_cols, 'label'])

            result = pd.concat([result_gs, result_remaining], ignore_index=True)

            buffer = io.StringIO()
            result.to_csv(buffer, index=False, header=False, sep=';', quoting=csv.QUOTE_ALL, escapechar='\\')
            buffer.seek(0)
            cursor.copy_expert(f"COPY temp_results_{datatype}{table_suffix} FROM STDIN (FORMAT CSV, DELIMITER ';', QUOTE '\"', ESCAPE '\\')", buffer)

            del result_gs, result_remaining, df_gs, df_remaining

            # --------------- Updating feature table ---------------
            print('Updating feature table', flush=True)

            cursor.execute(f"""
                UPDATE updates_{datatype}{table_suffix} f
                SET label = tp.label, processed = TRUE, gs = tp.gs, rb = tp.rb
                FROM temp_results_{datatype}{table_suffix} tp
                WHERE 
                    {' AND '.join([f'f.{col} = tp.{col}' for col in key_cols])}
            """)

            cursor.execute(f"TRUNCATE TABLE temp_results_{datatype}{table_suffix}")

            self.conn.commit()
            
        elapsed_time = time.perf_counter() - start_time
        print(f'Finished entity rule-based classification in {elapsed_time} secs', flush=True)    

        cursor.execute(f"DROP TABLE temp_results_{datatype}{table_suffix}")

        self.conn.commit()

        print(f'Total time spent on rule-based classification (ONLY - without table updates): {self.rule_base_time:.2f} seconds', flush=True)
