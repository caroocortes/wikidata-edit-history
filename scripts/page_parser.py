import html
import json
import Levenshtein
from lxml import etree
import sys
import re
import hashlib
import time
from datetime import datetime
from collections import defaultdict

from scripts.feature_creation import FeatureCreation
from scripts.utils import get_time_feature, haversine_metric, get_time_dict, gregorian_to_julian, id_to_int
from scripts.const import *

class PageParser():
    def __init__(self, file_path, page_elem_str, config, 
                 connection, property_labels, astronomical_object_types, scholarly_article_types):
        
        # Change storage
        self.changes = []
        self.revision = []
        # self.changes_metadata = []
        self.qualifier_changes = []
        self.reference_changes = []
        self.datatype_metadata_changes = []
        # self.datatype_metadata_changes_metadata = []

        # Feature storage
        self.quantity_features = []
        self.time_features = []
        self.entity_features = []
        self.text_features = []
        self.globecoordinate_features = []
        self.property_replacement_features = []

        self.config = config

        self.current_revision_redirect = False

        self.revision_meta = {}

        self.entity_data = {
            'label': '',
            'alias': '',
            'description': '',
            'p31_types': set(),
            'p279_types': set(),
            'p31_labels_list': '',
            'p279_labels_list': ''
        }

        self.file_path = file_path # file_path of XML where the page is stored
        self.page_elem = etree.fromstring(page_elem_str) # XML page for the entity

        self.conn = connection

        self.feature_creation = FeatureCreation(connection)

        self.PROPERTY_LABELS = property_labels
        self.ASTRONOMICAL_OBJECT_TYPES = astronomical_object_types
        self.SCHOLARLY_ARTICLE_TYPES = scholarly_article_types

        # FOR REVERTED EDIT TAGGING
        self.changes_by_pv = defaultdict(list)  # (property, value) -> [changes]

        # FOR ML FEATURES FRO PROPERTY_REPLACEMENT
        self.property_replacement_changes = []  # property replacement changes


        self.pending_changes = defaultdict(lambda: {'CREATE': None, 'DELETE': None})  # value_hash -> {'CREATE': change, 'DELETE': change}

        self.entity_stats = {
            'entity_id': None,
            'entity_label': '',
            'entity_types_31': '',

            'num_revisions': 0,
            
            'num_value_changes': 0, # this includes all changes to property values (creates, deletes, updates) 
            'num_value_change_creates': 0,
            'num_value_change_deletes': 0,
            'num_value_change_updates': 0,

            'num_rank_changes': 0,
            'num_rank_creates': 0,
            'num_rank_deletes': 0,
            'num_rank_updates': 0,

            'num_qualifier_changes': 0,
            'num_reference_changes': 0,

            'num_datatype_metadata_changes': 0,
            'num_datatype_metadata_creates': 0,
            'num_datatype_metadata_deletes': 0,
            'num_datatype_metadata_updates': 0,
            
            'first_revision_timestamp': None,  # For calculating entity age
            'last_revision_timestamp': None,
            
            'num_bot_edits': 0, 
            'num_anonymous_edits': 0,
            'num_human_edits': 0
        }

        self.entity_property_time = defaultdict(lambda: {
            'num_value_changes': 0,
            'num_value_additions': 0,
            'num_value_deletions': 0,
            'num_value_updates': 0,

            'num_rank_changes': 0,
            'num_rank_creates': 0,
            'num_rank_deletes': 0,
            'num_rank_updates': 0,

            'num_reference_additions': 0,
            'num_reference_deletions': 0,
            'num_qualifier_additions': 0,
            'num_qualifier_deletions': 0,

            'num_statement_additions': 0,
            'num_statement_deletions': 0,
            'num_soft_insertions': 0,
            'num_soft_deletions': 0,

            'revisions': set(),
            'revisions_bot': set(),
            'revisions_human': set(),
            'revisions_anonymous': set(),
            'unique_editors': set()
        })

    def calculate_entity_property_time_period_stats(
        self,
        entity_id,
        property_id,
        timestamp,
        revision_id,
        user_id,
        user_type,
        action,
        target,
        change_target,
        label,
        time_granularity='year_month'
    ):
        """
        Aggregate change statistics per (entity, property, time_period).
        """

        # Compute time bucket
        time_period = get_time_feature(timestamp, time_granularity)

        key = (entity_id, property_id, time_period)
        stats = self.entity_property_time[key]

        # --- revision + editor tracking ---
        stats['revisions'].add(revision_id)
        stats['unique_editors'].add(user_id)

        if user_type == 'bot':
            stats['revisions_bot'].add(revision_id)
        elif user_type == 'anonymous':
            stats['revisions_anonymous'].add(revision_id)
        else:
            stats['revisions_human'].add(revision_id)

        if label in ('value_insertion', 'value_deletion', 'value_update'):
            stats['num_value_changes'] += 1

        # --- value-level changes ---
        if label == 'value_insertion':
            stats['num_value_additions'] += 1

        elif label == 'value_deletion':
            stats['num_value_deletions'] += 1

        elif label == 'value_update':
            stats['num_value_updates'] += 1
        
        elif label == 'statement_deletion':
            stats['num_statement_deletions'] += 1
        
        elif label == 'statement_insertion':
            stats['num_statement_additions'] += 1

        elif label == 'soft_insertion':
            stats['num_soft_insertions'] += 1

        elif label == 'soft_deletion':
            stats['num_soft_deletions'] += 1

        # --- rank changes ---
        if change_target == 'rank':
            stats['num_rank_changes'] += 1

            if change_target == 'rank' and action == 'CREATE':
                stats['num_rank_creates'] += 1
            elif change_target == 'rank' and action == 'DELETE':
                stats['num_rank_deletes'] += 1
            elif change_target == 'rank' and action == 'UPDATE':
                stats['num_rank_updates'] += 1

        # --- reference changes ---
        if target == 'REFERENCE':
            if action == 'CREATE':
                stats['num_reference_additions'] += 1
            elif action == 'DELETE':
                stats['num_reference_deletions'] += 1

        # --- qualifier changes ---
        if target == 'QUALIFIER':
            if action == 'CREATE':
                stats['num_qualifier_additions'] += 1
            elif action == 'DELETE':
                stats['num_qualifier_deletions'] += 1

    def update_entity_stats(self, change_target, action):
        if change_target == '':
            self.entity_stats['num_value_changes'] += 1

            if action == 'CREATE':
                self.entity_stats['num_value_change_creates'] += 1
            
            elif action == 'DELETE':
                self.entity_stats['num_value_change_deletes'] += 1
            
            elif action == 'UPDATE':
                self.entity_stats['num_value_change_updates'] += 1

        if change_target == 'rank':
            self.entity_stats['num_rank_changes'] += 1
            if action == 'CREATE':
                self.entity_stats['num_rank_creates'] += 1
            elif action == 'DELETE':
                self.entity_stats['num_rank_deletes'] += 1
            elif action == 'UPDATE':
                self.entity_stats['num_rank_updates'] += 1

    def finalize_entity_property_time_stats(self):
        entity_property_time_stats = []
        for key in self.entity_property_time:
            self.entity_property_time[key]['num_revisions_anonymous'] = len(self.entity_property_time[key]['revisions_anonymous'])
            self.entity_property_time[key]['num_revisions_human'] = len(self.entity_property_time[key]['revisions_human'])
            self.entity_property_time[key]['num_unique_editors'] = len(self.entity_property_time[key]['unique_editors'])
            self.entity_property_time[key]['num_revisions'] = len(self.entity_property_time[key]['revisions'])
            self.entity_property_time[key]['num_revisions_bot'] = len(self.entity_property_time[key]['revisions_bot'])
        
            del self.entity_property_time[key]['revisions_anonymous']
            del self.entity_property_time[key]['revisions_human']
            del self.entity_property_time[key]['unique_editors']
            del self.entity_property_time[key]['revisions']
            del self.entity_property_time[key]['revisions_bot']

            entity_property_time_stats.append((
                key[0], # entity_id
                key[1], # property_id
                key[2], # time_period
                self.entity_property_time[key]['num_value_changes'],
                self.entity_property_time[key]['num_value_additions'],
                self.entity_property_time[key]['num_value_deletions'],
                self.entity_property_time[key]['num_value_updates'],
                self.entity_property_time[key]['num_statement_additions'],
                self.entity_property_time[key]['num_statement_deletions'],
                self.entity_property_time[key]['num_soft_insertions'],
                self.entity_property_time[key]['num_soft_deletions'],
                self.entity_property_time[key]['num_rank_changes'],
                self.entity_property_time[key]['num_rank_creates'],
                self.entity_property_time[key]['num_rank_deletes'],
                self.entity_property_time[key]['num_rank_updates'],
                self.entity_property_time[key]['num_reference_additions'],
                self.entity_property_time[key]['num_reference_deletions'],
                self.entity_property_time[key]['num_qualifier_additions'],
                self.entity_property_time[key]['num_qualifier_deletions'],
                self.entity_property_time[key]['num_revisions'],
                self.entity_property_time[key]['num_revisions_bot'],
                self.entity_property_time[key]['num_revisions_human'],
                self.entity_property_time[key]['num_revisions_anonymous'],
                self.entity_property_time[key]['num_unique_editors'],
            ))
        return entity_property_time_stats
    
    def _parse_json_revision(self, revision_elem, revision_text):
        # TODO: remove revision_elem from args - only for debugging
        """
            Returns the text of a revision as a json
        """

        try:
            # Most of the revisions are HTML escaped, but some aren't, that's why there's a second try/except
            json_text = html.unescape(revision_text.strip())
            
            # normalize to regular quotes & remove control characters so json parsing doesnt break
            json_text = json_text.replace('“', '"').replace('”', '"').replace('„', '"').replace('‟', '"')
            json_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', json_text)
        
            current_revision = json.loads(json_text)
            return current_revision
        except json.JSONDecodeError as e:
            pass

        try:
            return json.loads(revision_text.strip())
        except json.JSONDecodeError:

            print(f"Error decoding JSON in revision {self.revision_meta['revision_id']} for entity {self.revision_meta['entity_id']}: {e}. Revision skipped. See {ERROR_REVISION_TEXT_PATH} for details.")
            
            with open(ERROR_REVISION_TEXT_PATH, "a") as f:
                f.write(f"-------------------------------------------\n")
                f.write(f"Revision {self.revision_meta['revision_id']} for entity {self.revision_meta['entity_id']}:\n")
                revision_xml_str = etree.tostring(
                    revision_elem,
                    pretty_print=True,
                    encoding="unicode" 
                )
                f.write(revision_xml_str + "\n")
                f.write(f"-------------------------------------------\n")
            return None

    @staticmethod
    def get_target_action_from_change_type(change_type):
        """
            Splits a change_type into action and target (e.g. 'CREATE_PROPERTY_VALUE' into 'CREATE' and 'PROPERTY_VALUE').
        """
        if not change_type or "_" not in change_type:
            return change_type, None 

        parts = change_type.split("_", 1)
        action = parts[0]
        target = parts[1]
        return action, target

    @staticmethod
    def magnitude_of_change(old_value, new_value, datatype, metadata=False):
        """ 
            Calculates magnitude of change between new and old value that have the same datatype.
            The field metadata indicates if the old and new values correspond to datatype metadata values (True) or property value (False).
        """
        if new_value is not None and old_value is not None and not metadata:
            if datatype == 'quantity':
                new_num = float(new_value)
                old_num = float(old_value)
                return float(new_num - old_num) # don't use abs() so we have the "sign" and we can determine if it was an increase or decrease
            
            if datatype == 'time':
                old_dict = get_time_dict(old_value)
                new_dict = get_time_dict(new_value)
                
                if 'year' in new_dict and 'month' in new_dict and 'day' in new_dict and 'year' in old_dict and 'month' in old_dict and 'day' in old_dict: 
                    new_julian = gregorian_to_julian(new_dict['year'], new_dict['month'], new_dict['day'])
                    old_julian = gregorian_to_julian(old_dict['year'], old_dict['month'], old_dict['day'])
                    return float(new_julian - old_julian) # distance in days
                else:
                    return float(0)

            # Calculate distande in km between 2 points
            if datatype == 'globecoordinate' and isinstance(old_value, dict) and isinstance(new_value, dict):
                lat1, lon1 = float(old_value['latitude']), float(old_value['longitude'])
                lat2, lon2 = float(new_value['latitude']), float(new_value['longitude'])
                return float(haversine_metric(lon1, lat1, lon2, lat2))
            
           
            if datatype in WD_STRING_TYPES:
                # TODO: check if this makes sense for monolingual text, comparing between different languages, what happens with languages like chinese, arabic...?
                return float(Levenshtein.distance(old_value, new_value))
        
        elif new_value is not None and old_value is not None and metadata:
            # Calculate magnitude of change for datatype metadata
            # the values will be:
            # - monolingual text: language
            # - globecoordinate: precision
            # - quantity: lowerBound and upperBound
            # - time: timezone and precision
            if datatype not in WD_STRING_TYPES:
                new_num = float(new_value)
                old_num = float(old_value)
                return new_num - old_num
            else:
                return float(Levenshtein.distance(old_value, new_value))
        else:
            return None

    @staticmethod
    def _get_property_mainsnak(stmt, property_=None):
        """
            Returns the value for a property in the mainsnak
        """
        try:
            return stmt["mainsnak"].get(property_, None)
        except (KeyError, TypeError) as e:
            print(f'Error when retrieving property {property_} in statement {stmt}')
            raise e
        
    @staticmethod
    def _safe_get_nested(d, *keys):
        """
            Safely access nested dictionary keys, avoiding issues with unexpected list types.
            Wikidata sets claims, labels and descriptions to [] if there's no value. If some value exists, they turn to dicts ({})

            Example:
                _safe_get_nested(revision, 'labels', 'en', 'value')

            Will return {} if any part of the path is invalid or not a dict.
        """
        default = {}
        current = d
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key, default)
            else:
                return default

        if isinstance(current, list):
            return default
        else:
            return current
    
    def get_label_alias_description(self, revision):
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        label = PageParser._safe_get_nested(revision, 'labels', lang, 'value') 

        description = PageParser._safe_get_nested(revision, 'descriptions', lang, 'value')
        
        if isinstance(revision.get('aliases', None), dict): # there can be multiple aliases for a single language
            aliases_list = revision['aliases'].get(lang, [])
            alias = aliases_list[0]['value'] if len(aliases_list) > 0 else ''
        else:
            alias = ''

        return label if not isinstance(label, dict) else '', alias, description if not isinstance(description, dict) else ''
    
    @staticmethod
    def parse_datavalue_json(value_json, datatype):

        value = None
        datatype_metadata = {}
        if isinstance(value_json, dict):
            # complex datatypes - time, quantity, globecoordinate, monolingualtext
            # we consider entity as a simple type
            if datatype == 'globecoordinate':
                value = {
                    "longitude": value_json['longitude'],
                    "latitude": value_json['latitude']
                }
            if datatype != 'wikibase-entityid':
                for k, v in value_json.items():
                    # time, amount, text, latitude, longitude hold the actual value of time, quantity, 
                    # monolingualtext and globecoordinate datatypes, the rest is metadata
                    if k not in ("time", "amount", "text", "latitude", "longitude", "altitude", "before", "after", "timezone"): # altitude (DEPRECATED), before, after and timezone (UNUSED)
                        datatype_metadata[k] = v
                    else:
                        if datatype != 'globecoordinate' and k not in ("altitude", "before", "after", "timezone"):
                            value = v
            else:
                if 'id' in value_json:
                    value = value_json.get('id')
                else: # not all entities have numeric-id or id
                    value = 'Q' + str(value_json.get('numeric-id'))
        else:
            value = value_json

        return value, datatype, datatype_metadata

    @staticmethod
    def _parse_datavalue(statement):
        """
            Returns the value, datatype and datatype_metadata of a statement, from the datavalue field
            If datatype == 'globecoordinate', then value is a json with latitude and longitude
        """
        if not statement:
            return None, None, None

        snaktype = PageParser._get_property_mainsnak(statement, 'snaktype')

        if snaktype == 'value':

            datavalue = PageParser._get_property_mainsnak(statement, 'datavalue')
            
            value_json = datavalue.get("value", None)
            datatype = datavalue.get("type", None)

            return PageParser.parse_datavalue_json(value_json, datatype)

        else:
            value = NO_VALUE if snaktype == 'novalue' else SOME_VALUE
            return value, 'unknown-values', None

    @staticmethod
    def _description_label_change_type(old_value, new_value):
        """
            Returns the change type for labels and descriptions (only have one value) 
        """
 
        if not old_value and new_value:
            return CREATE_PROPERTY
        elif old_value and not new_value:
            return DELETE_PROPERTY
        elif old_value and new_value and old_value != new_value:
            return UPDATE_PROPERTY_VALUE

    def process_pair_changes(self, change):
        """Track and immediately process pairs
        for property replacement features"""
        
        action = change[11] # action
        if action == 'DELETE':
            value_hash = change[13] # old_hash is not None
            opposite_action = 'CREATE'
        else:
            value_hash = change[14] # new_hash is not None
            opposite_action = 'DELETE'
        
        if self.pending_changes[value_hash][opposite_action] is not None:
            opposite_change = self.pending_changes[value_hash][opposite_action]
            
            if (opposite_change[1] != change[1]): # different property_id and same value since I get it with value_hash
                
                features = self.feature_creation.create_property_replacement_features(change, opposite_change)

                self.property_replacement_features.append(features) # already has all columns needed
                
                # Clear the oldest one, so the most recent one can be compared to future ones
                timestamp_change = datetime.strptime(change[15].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
                timestamp_opposite_change = datetime.strptime(opposite_change[15].replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
                if timestamp_change < timestamp_opposite_change and action == 'DELETE':
                    self.pending_changes[value_hash]['DELETE'] = None
                else:
                    self.pending_changes[value_hash]['CREATE'] = None
                
        # No match - store as pending 
        self.pending_changes[value_hash][action] = change
    
    def calculate_features(self, revision_id, property_id, value_id, old_value, new_value, old_datatype, new_datatype, change_target, action):
        base_cols = (
            revision_id,
            property_id,
            value_id,
            change_target,
            new_datatype,
            old_datatype,
            action,
            old_value,
            new_value,
        )

        if  new_datatype == 'quantity':
            features = self.feature_creation.create_quantity_features(old_value, new_value)
            self.quantity_features.append(
                base_cols + features
            ) 
        if new_datatype == 'globecoordinate':
            features = self.feature_creation.create_globe_coordinate_features(old_value, new_value)
            self.globecoordinate_features.append(
                base_cols + features
            )

        if new_datatype == 'time':
            features = self.feature_creation.create_time_features(old_value, new_value)
            self.time_features.append(
                base_cols + features
            )
        
        if new_datatype in WD_STRING_TYPES:
            features = self.feature_creation.create_text_features('text', old_value, new_value)
            self.text_features.append(
                base_cols + features
            )

        if new_datatype in WD_ENTITY_TYPES:
            features  = self.feature_creation.create_entity_features(old_value, new_value)
            self.entity_features.append(
                base_cols + features
            )

    def save_changes(self, property_id, value_id, old_value, new_value, old_datatype, new_datatype, change_target, change_type, change_magnitude=None, old_hash=None, new_hash=None):
        """
            Store value + datatype metadata (of property value) + rank changes
        """
        old_value = json.dumps(old_value) if old_value else '{}' # in the DB can't be NULL because null = null is NULL in postgresql
        new_value = json.dumps(new_value) if new_value else '{}'

        action, target = PageParser.get_target_action_from_change_type(change_type)

        timestamp = self.revision_meta['timestamp']
        entity_id = self.revision_meta['entity_id']
        revision_id = self.revision_meta['revision_id']

        change_target = change_target if change_target else ''
        
        label = ''

        # Property value tagging
        if change_target == '':
            if new_datatype != old_datatype and action == 'UPDATE': # NOTE: the datatypes could be different but it could be a CREATE or DELETE
                label = 'value_update'

            if action == 'CREATE' and target == 'PROPERTY':
                label = 'statement_insertion'

            if action == 'DELETE' and target == 'PROPERTY':
                label = 'statement_deletion'

            if action == 'CREATE' and target == 'PROPERTY_VALUE':
                label = 'value_insertion'
        
            if action == 'DELETE' and target == 'PROPERTY_VALUE':
                label = 'value_deletion'

        # Soft insertion + deletion 
        if change_target == 'rank' and action == 'UPDATE':
            if old_value in ['normal', 'preferred'] and new_value == 'deprecated':
                label = 'soft_deletion'

            if new_value == 'preferred' and old_value in ['deprecated','normal']:
                label = 'soft_insertion'

        self.update_entity_stats(change_target, action)

        if change_target != 'rank':
            self.changes_by_pv[(property_id, value_id)].append({
                'timestamp': timestamp,
                'old_hash': old_hash if old_hash else '',
                'new_hash': new_hash if new_hash else '',
                'old_value': old_value,
                'new_value': new_value,
                'comment': self.revision_meta['comment'],
                'user_id': self.revision_meta['user_id'],
                'change_target': change_target if change_target else '',
                'revision_id': revision_id,
                'user_type': self.revision_meta['user_type'],
                'action': action,
                'new_datatype': new_datatype,
                'old_datatype': old_datatype
            })

        if change_target == '' and action == 'UPDATE' and new_datatype == old_datatype:
            self.calculate_features(
                revision_id,
                property_id,
                value_id,
                old_value,
                new_value,
                old_datatype,
                new_datatype,
                change_target,
                action
            )
            
        change = (
            revision_id,
            property_id,
            self.PROPERTY_LABELS.get(str(property_id), ''),
            value_id,
            old_value,
            new_value,
            old_datatype,
            new_datatype,
            change_target, # can't be None since change_target is part of the key of the table
            action,
            target,
            old_hash if old_hash else '',
            new_hash if new_hash else '',
            timestamp,
            get_time_feature(timestamp, 'week'),
            get_time_feature(timestamp, 'year_month'),
            get_time_feature(timestamp, 'year'),
            label,
            entity_id
        )
            
        self.changes.append(change)

        if action == 'CREATE' or action == 'DELETE': # only for create and deletes
            self.process_pair_changes(change)

        # change_metadata = ()
        # if change_magnitude is not None:
        #     change_metadata = (
        #         self.revision_meta['revision_id'],
        #         property_id,
        #         value_id,
        #         change_target if change_target else '', # can't be None since change_target is part of the key of the table
        #         'CHANGE_MAGNITUDE',
        #         change_magnitude
        #     )
        #     self.changes_metadata.append(change_metadata)
        
    
    def save_datatype_metadata_changes(self, property_id, value_id, old_value, new_value, old_datatype, new_datatype, change_target, change_type, change_magnitude=None, old_hash=None, new_hash=None):
        """
            Store value + datatype metadata (of property value) + rank changes
        """
        old_value = json.dumps(old_value) if old_value else '{}' # in the DB can't be NULL because null = null is NULL in postgresql
        new_value = json.dumps(new_value) if new_value else '{}'

        action, target = PageParser.get_target_action_from_change_type(change_type)
        timestamp = self.revision_meta['timestamp']

        label = ''
        if action == 'UPDATE':
            label = 'datatype_context_update'

        change = (
            self.revision_meta['revision_id'],
            property_id,
            self.PROPERTY_LABELS.get(str(property_id), ''),
            value_id,
            old_value,
            new_value,
            old_datatype,
            new_datatype,
            change_target if change_target else '', # can't be None since change_target is part of the key of the table
            action,
            target,
            old_hash if old_hash else '',
            new_hash if new_hash else '',
            timestamp,
            get_time_feature(timestamp, 'week'),
            get_time_feature(timestamp, 'year_month'),
            get_time_feature(timestamp, 'year'),
            self.revision_meta['entity_id'],
            label
        )

        self.datatype_metadata_changes.append(change)
        
        if action == 'CREATE':
            self.entity_stats['num_datatype_metadata_creates'] += 1
        if action == 'DELETE':
            self.entity_stats['num_datatype_metadata_deletes'] += 1
        elif action == 'UPDATE':
            self.entity_stats['num_datatype_metadata_updates'] += 1

        self.entity_stats['num_datatype_metadata_changes'] += 1
        
        # self.datatype_metadata_changes_by_pv[(property_id, value_id)].append({
        #     'timestamp': timestamp,
        #     'old_hash': old_hash if old_hash else '',
        #     'new_hash': new_hash if new_hash else '',
        #     'comment': self.revision_meta['comment'],
        #     'user_id': self.revision_meta['user_id'],
        #     'change_target': change_target if change_target else '',
        #     'revision_id': self.revision_meta['revision_id']
        # })

        # change_metadata = ()
        # if change_magnitude is not None:
        #     change_metadata = (
        #         self.revision_meta['revision_id'],
        #         property_id,
        #         value_id,
        #         change_target if change_target else '', # can't be None since change_target is part of the key of the table
        #         'CHANGE_MAGNITUDE',
        #         change_magnitude
        #     )
        #     self.datatype_metadata_changes_metadata.append(change_metadata)
    
    
    def save_qualifier_changes(self, property_id, value_id, qual_property_id, value_hash, old_value, new_value, old_datatype, new_datatype, change_target, change_type):
        """
            Store reference/qualifier changes
        """
        old_value = json.dumps(old_value) if old_value else '{}'
        new_value = json.dumps(new_value) if new_value else '{}'

        action, target = PageParser.get_target_action_from_change_type(change_type)
        timestamp = self.revision_meta['timestamp']
        
        label = ''
        # -- qualifiers adding end time
        # -- P582 = end time
        # -- P8554 = earliest end date - earliest date on which the statement could have begun to no longer be true
        # -- P12506 = latest end date - latest date beyond which the statement could no longer be true
        
        if action == 'CREATE':
            if qual_property_id in ['P582', 'P8554', 'P12506']:
                label = 'soft_insertion'
        
        change = (
            self.revision_meta['revision_id'],
            property_id,
            self.PROPERTY_LABELS.get(str(property_id), ''),
            value_id,
            qual_property_id,
            self.PROPERTY_LABELS.get(str(qual_property_id), ''),
            value_hash,
            old_value,
            new_value,
            old_datatype,
            new_datatype,
            change_target if change_target else '', # can't be None since change_target is part of the key of the table
            action,
            target,
            timestamp,
            get_time_feature(timestamp, 'week'),
            get_time_feature(timestamp, 'year_month'),
            get_time_feature(timestamp, 'year'),
            self.revision_meta['entity_id'],
            label
        )

        self.qualifier_changes.append(change)

        self.calculate_entity_property_time_period_stats(
            entity_id=self.revision_meta['entity_id'],
            property_id=property_id,
            timestamp=timestamp,
            revision_id=self.revision_meta['revision_id'],
            user_id=self.revision_meta['user_id'],
            user_type=self.revision_meta['user_type'],
            action=action,
            target=target,
            change_target=change_target,
            label=label,
            time_granularity='year_month'
        )

    def save_reference_changes(self, property_id, value_id, ref_property_id, ref_hash, value_hash, old_value, new_value, old_datatype, new_datatype, change_target, change_type):
        """
            Store reference changes
        """
        old_value = json.dumps(old_value) if old_value else '{}'
        new_value = json.dumps(new_value) if new_value else '{}'

        action, target = PageParser.get_target_action_from_change_type(change_type) 
        timestamp = self.revision_meta['timestamp']
        label = ''
        change = (
            self.revision_meta['revision_id'],
            property_id,
            self.PROPERTY_LABELS.get(str(property_id), ''),
            value_id,
            ref_property_id,
            self.PROPERTY_LABELS.get(str(ref_property_id), ''), 
            ref_hash,
            value_hash,
            old_value,
            new_value,
            old_datatype,
            new_datatype,
            change_target if change_target else '', # can't be None since change_target is part of the key of the table
            action,
            target,
            timestamp,
            get_time_feature(timestamp, 'week'),
            get_time_feature(timestamp, 'year_month'),
            get_time_feature(timestamp, 'year'),
            self.revision_meta['entity_id'],
            label
        )

        self.reference_changes.append(change)

        self.calculate_entity_property_time_period_stats(
            entity_id=self.revision_meta['entity_id'],
            property_id=property_id,
            timestamp=timestamp,
            revision_id=self.revision_meta['revision_id'],
            user_id=self.revision_meta['user_id'],
            user_type=self.revision_meta['user_type'],
            action=action,
            target=target,
            change_target=change_target,
            label=label,
            time_granularity='year_month'
        )

    def _handle_datatype_metadata_changes(self, old_datatype_metadata, new_datatype_metadata, value_id, old_datatype, new_datatype, property_id, change_type, old_hash=None, new_hash=None, type_='value', rq_property_id=None, value_hash=None, ref_hash=None):

        if old_datatype_metadata and not new_datatype_metadata: # deletion
            for key in old_datatype_metadata.keys():
                old_meta = old_datatype_metadata.get(key, None)

                if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                    old_meta = old_meta.split('/')[-1]
                
                if type_ == 'value':

                    if old_datatype == 'monolingualtext':
                        self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=None,
                            old_datatype=old_datatype,
                            new_datatype=new_datatype,
                            change_target=key,
                            change_type=change_type, 
                            old_hash=old_hash,
                            new_hash=None
                        )
                    else:
                        self.save_datatype_metadata_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=None,
                            old_datatype=old_datatype,
                            new_datatype=new_datatype,
                            change_target=key,
                            change_type=change_type, 
                            old_hash=old_hash,
                            new_hash=None
                        )
                # elif type_ == 'qualifier':
                #     self.save_qualifier_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         qual_property_id=id_to_int(rq_property_id),
                #         value_hash=value_hash,
                #         old_value=old_meta,
                #         new_value=None,
                #         old_datatype=old_datatype,
                #         new_datatype=new_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )
                # elif type_ == 'reference':
                #     self.save_reference_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         ref_property_id=id_to_int(rq_property_id),
                #         ref_hash=ref_hash,
                #         value_hash=value_hash,
                #         old_value=old_meta,
                #         new_value=None,
                #         old_datatype=old_datatype,  # Use old_datatype, not new_datatype
                #         new_datatype=new_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )
            return
        
        if new_datatype_metadata and not old_datatype_metadata: # creation
            for key in new_datatype_metadata.keys():
                new_meta = new_datatype_metadata.get(key, None)

                if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                    new_meta = new_meta.split('/')[-1]
                
                if type_ == 'value':
                    if new_datatype == 'monolingualtext':
                        self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=None,
                            new_value=new_meta,
                            new_datatype=new_datatype,
                            old_datatype=old_datatype,
                            change_target=key,
                            change_type=change_type, 
                            old_hash=None,
                            new_hash=new_hash
                        )
                    else:
                        self.save_datatype_metadata_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=None,
                            new_value=new_meta,
                            new_datatype=new_datatype,
                            old_datatype=old_datatype,
                            change_target=key,
                            change_type=change_type, 
                            old_hash=None,
                            new_hash=new_hash
                        )
                # elif type_ == 'qualifier':
                #     self.save_qualifier_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         qual_property_id=id_to_int(rq_property_id),
                #         value_hash=value_hash,
                #         old_value=None,
                #         new_value=new_meta,
                #         new_datatype=new_datatype,
                #         old_datatype=old_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )
                # elif type_ == 'reference':
                #     self.save_reference_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         ref_property_id=id_to_int(rq_property_id),
                #         ref_hash=ref_hash,
                #         value_hash=value_hash,
                #         old_value=None,
                #         new_value=new_meta,
                #         old_datatype=old_datatype,
                #         new_datatype=new_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )
            return

        if old_datatype == new_datatype:
        
            for key in set((old_datatype_metadata or {}).keys()):
                old_meta = (old_datatype_metadata or {}).get(key, None)
                new_meta = (new_datatype_metadata or {}).get(key, None)

                if key not in ('calendarmodel', 'globe', 'unit'): # this metadata stores an entity link so we don't calculate the magnitude of change
                    change_magnitude = PageParser.magnitude_of_change(old_meta, new_meta, new_datatype, metadata=True)
                else: 
                    change_magnitude = None

                if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                    new_meta = new_meta.split('/')[-1]

                if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                    old_meta = old_meta.split('/')[-1]

                if old_meta != new_meta: # save only what changed
                    
                    if type_ == 'value':

                        if old_datatype == 'monolingualtext':
                            self.save_changes(
                                id_to_int(property_id),
                                value_id=value_id,
                                old_value=old_meta,
                                new_value=new_meta,
                                old_datatype=old_datatype,
                                new_datatype=new_datatype,
                                change_target=key,
                                change_type=change_type, 
                                change_magnitude=change_magnitude,
                                old_hash=old_hash,
                                new_hash=new_hash
                            )
                        else:
                            self.save_datatype_metadata_changes(
                                id_to_int(property_id),
                                value_id=value_id,
                                old_value=old_meta,
                                new_value=new_meta,
                                old_datatype=old_datatype,
                                new_datatype=new_datatype,
                                change_target=key,
                                change_type=change_type, 
                                change_magnitude=change_magnitude,
                                old_hash=old_hash,
                                new_hash=new_hash
                            )
                    # elif type_ == 'qualifier':
                    #     self.save_qualifier_changes(
                    #         id_to_int(property_id),
                    #         value_id=value_id,
                    #         qual_property_id=id_to_int(rq_property_id),
                    #         value_hash=value_hash,
                    #         old_value=old_meta,
                    #         new_value=new_meta,
                    #         old_datatype=old_datatype,
                    #         new_datatype=new_datatype,
                    #         change_target=key,
                    #         change_type=change_type
                    #     )
                    # elif type_ == 'reference':
                    #     self.save_reference_changes(
                    #         id_to_int(property_id),
                    #         value_id=value_id,
                    #         ref_property_id=id_to_int(rq_property_id),
                    #         ref_hash=ref_hash,
                    #         value_hash=value_hash,
                    #         old_value=old_meta,
                    #         new_value=new_meta,
                    #         old_datatype=old_datatype,
                    #         new_datatype=new_datatype,
                    #         change_target=key,
                    #         change_type=change_type
                    #     )

        else: # different datatypes

            old_keys_set = set((old_datatype_metadata or {}).keys())
            new_keys_set = set((new_datatype_metadata or {}).keys())

            if len(old_keys_set) > len(new_keys_set):
                big_set = old_keys_set
                small_set = new_keys_set
                big_old = True
            else:
                big_set = new_keys_set
                small_set = old_keys_set
                big_old = False

            keys_to_skip = set()
            for key in small_set:

                if big_old:
                    
                    new_meta = (new_datatype_metadata or {}).get(key, None)
                    if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                        new_meta = new_meta.split('/')[-1]
                    
                    old_meta_key = next((k for k in old_keys_set if k not in keys_to_skip), None)
                    old_meta = (old_datatype_metadata or {}).get(old_meta_key, None)

                    if old_meta_key == 'calendarmodel' or old_meta_key == 'unit': # keep only the Q-id
                        old_meta = old_meta.split('/')[-1]

                    if old_meta_key is not None:
                        keys_to_skip.add(old_meta_key)
                else:
                    old_meta = (old_datatype_metadata or {}).get(key, None)
                    if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                        old_meta = old_meta.split('/')[-1]

                    new_meta_key = next((k for k in new_keys_set if k not in keys_to_skip), None)
                    new_meta = (new_datatype_metadata or {}).get(new_meta_key, None)

                    if new_meta_key == 'calendarmodel' or new_meta_key == 'unit': # keep only the Q-id
                        new_meta = new_meta.split('/')[-1]

                    if new_meta_key is not None:
                        keys_to_skip.add(new_meta_key)
                
                if type_ == 'value':
                    if key == 'language':
                        self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=new_meta,
                            old_datatype=old_datatype,
                            new_datatype=new_datatype,
                            change_target=key,
                            change_type=change_type,
                            old_hash=old_hash,
                            new_hash=new_hash
                        )
                    else:
                        self.save_datatype_metadata_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=new_meta,
                            old_datatype=old_datatype,
                            new_datatype=new_datatype,
                            change_target=key,
                            change_type=change_type,
                            old_hash=old_hash,
                            new_hash=new_hash
                        )
                # elif type_ == 'qualifier':
                #     self.save_qualifier_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         qual_property_id=id_to_int(rq_property_id),
                #         value_hash=value_hash,
                #         old_value=old_meta,
                #         new_value=new_meta,
                #         old_datatype=old_datatype,
                #         new_datatype=new_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )
                # elif type_ == 'reference':
                #     self.save_reference_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         ref_property_id=id_to_int(rq_property_id),
                #         ref_hash=ref_hash,
                #         value_hash=value_hash,
                #         old_value=old_meta,
                #         new_value=new_meta,
                #         old_datatype=old_datatype,
                #         new_datatype=new_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )
            
            remaining_keys = big_set - keys_to_skip
            for key in remaining_keys:
                
                if big_old:
                    old_meta = (old_datatype_metadata or {}).get(key, None)
                    if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                        old_meta = old_meta.split('/')[-1]

                    new_meta = None
                else:
                    new_meta = (new_datatype_metadata or {}).get(key, None)
                    if key == 'calendarmodel' or key == 'unit': # keep only the Q-id
                        new_meta = new_meta.split('/')[-1]
                        
                    old_meta = None
                
                if type_ == 'value':
                    if key == 'language':
                        self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=new_meta,
                            old_datatype=old_datatype,
                            new_datatype=new_datatype,
                            change_target=key,
                            change_type=change_type,
                            old_hash=old_hash,
                            new_hash=new_hash
                        )
                    else:
                        self.save_datatype_metadata_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=new_meta,
                            old_datatype=old_datatype,
                            new_datatype=new_datatype,
                            change_target=key,
                            change_type=change_type,
                            old_hash=old_hash,
                            new_hash=new_hash
                        )
                # elif type_ == 'qualifier':
                #     self.save_qualifier_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         qual_property_id=id_to_int(rq_property_id),
                #         value_hash=value_hash,
                #         old_value=old_meta,
                #         new_value=new_meta,
                #         old_datatype=old_datatype,
                #         new_datatype=new_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )

                # elif type_ == 'reference':
                #     self.save_reference_changes(
                #         id_to_int(property_id),
                #         value_id=value_id,
                #         ref_property_id=id_to_int(rq_property_id),
                #         value_hash=value_hash,
                #         ref_hash=ref_hash,
                #         old_value=old_meta,
                #         new_value=new_meta,
                #         old_datatype=old_datatype,
                #         new_datatype=new_datatype,
                #         change_target=key,
                #         change_type=change_type
                #     )
    
    def _handle_value_changes(self, old_datatype, new_datatype, new_value, old_value, value_id, property_id, change_type, old_hash, new_hash, change_magnitude=None):

        self.save_changes(
            id_to_int(property_id), 
            value_id=value_id,
            old_value=old_value,
            new_value=new_value,
            old_datatype=old_datatype,
            new_datatype=new_datatype,
            change_target=None,
            change_type=change_type,
            change_magnitude=change_magnitude,
            old_hash=old_hash,
            new_hash=new_hash
        )

    @staticmethod
    def homogenize_datavalue(prop_val):

        if 'datavalue' not in prop_val: # fallback for somevalue, novalue
            return prop_val
        
        type_ = prop_val['datavalue']['type']

        # Remove inconsistencies in time values + entities + unused/deprcated fields in time and globecoordinate
        if type_ == 'globecoordinate':
            prop_val['datavalue']['value'].pop("altitude", None)

        if type_ == 'time':
            # remove unused values
            prop_val['datavalue']['value'].pop("before", None)
            prop_val['datavalue']['value'].pop("after", None)
            
            # remove 0's at the beggining
            prop_val['datavalue']['value']['time'] = re.sub(r'^([+-])0*(\d+)', r'\1\2', prop_val['datavalue']['value']['time'])

        if type_ in WD_ENTITY_TYPES:
            # NOTE: From WD's doc, not all entities have a numeric-id
            # however, I've found revisions where the id is not present but the numeric-id is
            # therefore, I normalize and keep only 'id' or generate it from numeric-id
            if not 'id' in prop_val['datavalue']['value']:
                prop_val['datavalue']['value']['id'] = f"Q{prop_val['datavalue']['value']['numeric-id']}"
            
            # remove numeric-id, only keep id
            prop_val['datavalue']['value'].pop("numeric-id", None)

        return prop_val

    @staticmethod
    def generate_value_hash(hom_prop_val):
        """
            Generates a hash from the datavalue.
            Removes inconsistencies that happen in WD due to schema changes

            Input:
            - hom_prop_val: whole snak for a property value (includes snaktype, hash, datavalue)
                Has to go through the fucntion homogenize_datavalue before
            
            e.g.:
                - Lack of id between revisions:
                    r1: 
                        'datavalue': {
                            'value': {
                                'entity-type': 'item', 
                                'numeric-id': 15241312
                            }, 
                            'type': 'wikibase-entityid'}
                        }
                    r2: 
                        'datavalue': {
                            'value': {
                                'entity-type': 'item', 
                                'numeric-id': 15241312, 
                                'id': 'Q15241312'         <---------- The value is the same, but the hash differs because of the different JSON structure
                            }, 
                            'type': 'wikibase-entityid'
                        }
                - Extra 0's in dates
                    r1:
                        'datavalue': {
                            'value': {'time': '+2013-10-28T00:00:00Z', 'timezone': 0, 'before': 0, 'after': 0, 'precision': 11, 'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 
                            'type': 'time'
                        }
                    r2:
                        'datavalue': {
                            'value': {'time': '+00000002013-10-28T00:00:00Z', 'timezone': 0, 'before': 0, 'after': 0, 'precision': 11, 'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'}, 
                            'type': 'time'
                        }
        """
        if not hom_prop_val:
            return None

        snaktype = hom_prop_val.get('snaktype', None)
        current_hash = hom_prop_val.get('hash', None)
        
        if snaktype in (NO_VALUE, SOME_VALUE):
            return current_hash
        else:
            return hashlib.sha1(json.dumps(hom_prop_val['datavalue'], separators=(',', ':')).encode('utf-8')).hexdigest()

    def _handle_reference_changes(self, stmt_pid, stmt_value_id, prev_stmt, curr_stmt):
        """
        Handles addition/deletion of references by comparing snak value hashes.
        Deduplicates snaks within references and avoids unnecessary CREATE/DELETE.

        Structure of reference:
        "references": [
            {
                "hash": "fa278ebfc458360e5aed63d5058cca83c46134f1",
                "snaks": {
                    "P143": [
                        {
                            "snaktype": "value",
                            "property": "P143",
                            "hash": "e4f6d9441d0600513c4533c672b5ab472dc73694",
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 328,
                                    "id": "Q328"
                                },
                                "type": "wikibase-entityid"
                            }
                        }
                    ]
                },
                "snaks-order": [
                    "P143"
                ]
            }
        ]
        """
        change_detected = False

        prev_refs = prev_stmt.get('references', []) if prev_stmt else []
        curr_refs = curr_stmt.get('references', []) if curr_stmt else []

        if not prev_refs and not curr_refs:
            return False
                
        # map of (pid, hash): value 
        def build_hash_map(refs):
            hash_map = {}
            for ref in refs:
                # Create a stable "content hash" for the whole reference
                ref_snaks = []
                if isinstance(ref['snaks'], dict):

                    for pid, vals in ref['snaks'].items():
                        for prop_val in vals:
                            hom_prop_val = PageParser.homogenize_datavalue(prop_val)
                            value_hash = PageParser.generate_value_hash(hom_prop_val)
                            ref_snaks.append((pid, value_hash))
                else: 
                    continue
                
                # Sort and hash to get a stable reference-level id
                ref_content_hash = hashlib.sha1(
                    json.dumps(sorted(ref_snaks)).encode("utf-8")
                ).hexdigest()

                # Now map each snak individually
                if isinstance(ref['snaks'], dict):
                    for pid, vals in ref['snaks'].items():
                        for prop_val in vals:
                            hom_prop_val = PageParser.homogenize_datavalue(prop_val)
                            value_hash = PageParser.generate_value_hash(hom_prop_val)
                            hom_prop_val['hash'] = value_hash
                            hash_map[(ref_content_hash, pid, value_hash)] = hom_prop_val
                else: 
                    continue
                        
            return hash_map

        prev_hash_map = build_hash_map(prev_refs)
        curr_hash_map = build_hash_map(curr_refs)

        prev_keys = set(prev_hash_map.keys())
        curr_keys = set(curr_hash_map.keys())

        # Have to compare at the low-level hash (value hash)
        # because the high-level hash can change between revisions, but some of the inner values 
        # remains the same, just because at least one changed
        deleted = prev_keys - curr_keys
        created = curr_keys - prev_keys    

        # deletions
        for ref_hash, pid, value_hash in deleted:

            change_detected = True
            prop_value = prev_hash_map[(ref_hash, pid, value_hash)]

            if prop_value['snaktype'] in (NO_VALUE, SOME_VALUE):
                prev_val, prev_dtype, old_datatype_metadata = (prop_value['snaktype'], 'string', None)
            else:
                dv = prop_value['datavalue']
                prev_val, prev_dtype, old_datatype_metadata = PageParser.parse_datavalue_json(dv['value'], dv['type'])

            self.save_reference_changes(
                property_id=id_to_int(stmt_pid),
                value_id=stmt_value_id,
                ref_property_id=id_to_int(pid),
                ref_hash=ref_hash,
                value_hash=value_hash,
                old_value=prev_val,
                new_value=None,
                old_datatype=prev_dtype,
                new_datatype=None,
                change_target='',
                change_type=DELETE_REFERENCE
            )

            # if old_datatype_metadata:

            #     self._handle_datatype_metadata_changes(
            #         old_datatype_metadata=old_datatype_metadata,
            #         new_datatype_metadata=None,
            #         value_id=stmt_value_id,
            #         old_datatype=prev_dtype,
            #         new_datatype=None,
            #         property_id=stmt_pid,
            #         change_type=DELETE_REFERENCE,
            #         type_='reference',
            #         rq_property_id=pid,
            #         value_hash=value_hash,
            #         ref_hash=ref_hash
            #     )

        # creations
        for ref_hash, pid, value_hash in created:

            change_detected = True
            prop_value = curr_hash_map[(ref_hash, pid, value_hash)]

            if prop_value['snaktype'] in (NO_VALUE, SOME_VALUE):
                curr_val, curr_dtype, new_datatype_metadata = (prop_value['snaktype'], 'string', None)
            else:
                dv = prop_value['datavalue']
                curr_val, curr_dtype, new_datatype_metadata = PageParser.parse_datavalue_json(dv['value'], dv['type'])

            self.save_reference_changes(
                property_id=id_to_int(stmt_pid),
                value_id=stmt_value_id,
                ref_property_id=id_to_int(pid),
                ref_hash=ref_hash,
                value_hash=value_hash,
                old_value=None,
                new_value=curr_val,
                old_datatype=None,
                new_datatype=curr_dtype,
                change_target='',
                change_type=CREATE_REFERENCE
            )

            # if new_datatype_metadata:
        
            #     self._handle_datatype_metadata_changes(
            #         old_datatype_metadata=None,
            #         new_datatype_metadata=new_datatype_metadata,
            #         value_id=stmt_value_id,
            #         old_datatype=None,
            #         new_datatype=curr_dtype,
            #         property_id=stmt_pid,
            #         change_type=CREATE_REFERENCE,
            #         type_='reference',
            #         rq_property_id=pid,
            #         value_hash=value_hash,
            #         ref_hash=ref_hash
            #     )
        
        sys.stdout.flush()

        return change_detected
    
    def _handle_qualifier_changes(self, stmt_pid, stmt_value_id, prev_stmt, curr_stmt):
        """
        Handles addition, deletion of qualifiers values.
        Uses a simple CREATE/DELETE logic based on hashes.

        Structure of qualifiers:
        "qualifiers": {
            "P813": [
                {
                    "snaktype": "value",
                    "property": "P813",
                    "hash": "54358f6e346b19bed53f5b3a57e82a4f562940aa",
                    "datavalue": {
                        "value": {
                            "time": "+2021-12-31T00:00:00Z",
                            "timezone": 0,
                            "before": 0,
                            "after": 0,
                            "precision": 11,
                            "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
                        },
                        "type": "time"
                    }
                }
            ]
        },

        """

        change_detected = False

        prev = prev_stmt.get('qualifiers', {}) if prev_stmt else {}
        curr = curr_stmt.get('qualifiers', {}) if curr_stmt else {}

        # if there are no qualifiers, there's no 'qualifiers' in the mainsnak of the statement
        if not prev and not curr:
            return False

        all_pids = set(prev.keys()).union(curr.keys())

        for pid in all_pids:
            prev_stmts = prev.get(pid, []) # [{ 'snaktype': 'value', 'hash': '...', ...}]
            curr_stmts = curr.get(pid, [])

            # map by hash : stmt
            # we compare hashes because they are created from the actual values, so if the hashes are different, something changed
            
            # Because we are using hashes to identify values, if there are duplicate values we will have duplicate rows inserted
            # deduplicate prev_stmts by hash

            def build_hash_map(pid_values):
                hash_map = {}
                for prop_val in pid_values:
                    prop_val = PageParser.homogenize_datavalue(prop_val)
                    value_hash = PageParser.generate_value_hash(prop_val) 
                    prop_val['hash'] = value_hash # update hash
                    hash_map[value_hash] = prop_val
                return hash_map

            # deduplicate pevstmts by hash
            prev_map = build_hash_map(prev_stmts) # {hash: snak}

            # deduplicate curr_stmts by hash
            curr_map = build_hash_map(curr_stmts)
            
            # hashes are created from the actial values, so if there are different hashes something changed
            prev_hashes = set(prev_map.keys())
            curr_hashes = set(curr_map.keys())

            deleted = prev_hashes - curr_hashes
            added = curr_hashes - prev_hashes

            # --- Deleted values ---
            for h in deleted:
                change_detected = True
                prev_stmt = prev_map[h]

                snaktype = prev_stmt['snaktype']
                if snaktype in (NO_VALUE, SOME_VALUE):
                    prev_val, prev_dtype, old_datatype_metadata = (snaktype, 'string', None)
                else:
                    dv = prev_stmt['datavalue']
                    prev_val, prev_dtype, old_datatype_metadata = PageParser.parse_datavalue_json(dv['value'], dv['type'])
                
                self.save_qualifier_changes(
                    property_id=id_to_int(stmt_pid),
                    value_id=stmt_value_id,
                    qual_property_id=id_to_int(pid),
                    value_hash=h,
                    old_value=prev_val,
                    new_value=None,
                    old_datatype=prev_dtype,
                    new_datatype=None,
                    change_target='',
                    change_type=DELETE_QUALIFIER
                )

                # if old_datatype_metadata:
                #     self._handle_datatype_metadata_changes(
                #         old_datatype_metadata=old_datatype_metadata, 
                #         new_datatype_metadata=None, 
                #         value_id=stmt_value_id, 
                #         old_datatype=prev_dtype, 
                #         new_datatype=None, 
                #         property_id=stmt_pid, 
                #         change_type=DELETE_QUALIFIER, 
                #         type_='qualifier', 
                #         rq_property_id=pid, 
                #         value_hash=h
                #     )

            # --- Added values ---
            for h in added:
                change_detected = True
                curr_stmt = curr_map[h]

                snaktype = curr_stmt['snaktype']
                if snaktype in (NO_VALUE, SOME_VALUE):
                    curr_val, curr_dtype, new_datatype_metadata = (snaktype, 'string', None)
                else:
                    dv = curr_stmt['datavalue']
                    curr_val, curr_dtype, new_datatype_metadata = PageParser.parse_datavalue_json(dv['value'], dv['type'])

                self.save_qualifier_changes(
                    property_id=id_to_int(stmt_pid),
                    value_id=stmt_value_id,
                    qual_property_id=id_to_int(pid),
                    value_hash=h,
                    old_value=None,
                    new_value=curr_val,
                    old_datatype=None,
                    new_datatype=curr_dtype,
                    change_target='',
                    change_type=CREATE_QUALIFIER
                )

                # if new_datatype_metadata:
                #     self._handle_datatype_metadata_changes(
                #         old_datatype_metadata=None, 
                #         new_datatype_metadata=new_datatype_metadata, 
                #         value_id=stmt_value_id, 
                #         old_datatype=None, 
                #         new_datatype=curr_dtype, 
                #         property_id=stmt_pid, 
                #         change_type=CREATE_QUALIFIER, 
                #         type_='qualifier', 
                #         rq_property_id=pid, 
                #         value_hash=h
                #     )

        return change_detected
            
    def _changes_created_entity(self, revision):

        # Process claims
        claims = PageParser._safe_get_nested(revision, 'claims')
        
        for property_id, property_stmts in claims.items():
            for stmt in property_stmts:
                
                value, datatype, datatype_metadata = PageParser._parse_datavalue(stmt)
                new_hash = PageParser._get_property_mainsnak(stmt, 'hash') if stmt else None
                value_id = stmt.get('id', None)

                if property_id == 'P31':
                    self.entity_data['p31_types'].add((value_id, value))

                if property_id == 'P279':
                    self.entity_data['p279_types'].add((value_id, value))
                
                old_value = None
                new_value = value
                
                self.save_changes(
                    id_to_int(property_id), 
                    value_id=value_id,
                    old_value=old_value,
                    new_value=new_value,
                    old_datatype=None,
                    new_datatype=datatype,
                    change_target=None,
                    change_type=CREATE_PROPERTY,
                    old_hash=None,
                    new_hash=new_hash
                )

                if datatype_metadata:
                    for k, v in datatype_metadata.items():
                        old_value = None
                        new_value = v
                        
                        self.save_datatype_metadata_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_value,
                            new_value=new_value,
                            old_datatype=None,
                            new_datatype=datatype,
                            change_target=k,
                            change_type=CREATE_PROPERTY,
                            old_hash=None,
                            new_hash=new_hash
                        )

                # qualifier changes
                _ = self._handle_qualifier_changes(property_id, value_id, prev_stmt=None, curr_stmt=stmt)

                # references changes
                _ = self._handle_reference_changes(property_id, value_id, prev_stmt=None, curr_stmt=stmt)

        # If there's no description or label, the revisions shows them as []
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        labels = PageParser._safe_get_nested(revision, 'labels', lang, 'value')
        descriptions = PageParser._safe_get_nested(revision, 'descriptions', lang, 'value')

        # Process labels and descriptions (non-claim properties)
        for pid, val in [(LABEL_PROP_ID, labels), (DESCRIPTION_PROP_ID, descriptions)]:
            if val:
                old_value = None
                new_value = val

                self.save_changes(
                    pid, 
                    value_id='label' if pid == LABEL_PROP_ID else 'description',
                    old_value=old_value if not isinstance(old_value, dict) else None,
                    new_value=new_value if not isinstance(new_value, dict) else None,
                    old_datatype=None,
                    new_datatype='string',
                    change_target=None,
                    change_type=CREATE_PROPERTY,
                    old_hash='',
                    new_hash=''
                )
     
    def _changes_cleaned_entity(self, revision):
        # TODO: merge this one with the created, the only thing that changes is the old_value/new_value None
        # Process claims
        claims = PageParser._safe_get_nested(revision, 'claims')
        
        for property_id, property_stmts in claims.items():
            for stmt in property_stmts:
                
                value, datatype, datatype_metadata = PageParser._parse_datavalue(stmt)
                new_hash = PageParser._get_property_mainsnak(stmt, 'hash') if stmt else None
                value_id = stmt.get('id', None)

                if property_id == 'P31':
                    self.entity_data['p31_types'].remove((value_id, value))

                if property_id == 'P279':
                    self.entity_data['p279_types'].remove((value_id, value))
                
                old_value = value
                new_value = None
                
                self.save_changes(
                    id_to_int(property_id), 
                    value_id=value_id,
                    old_value=old_value,
                    new_value=new_value,
                    old_datatype=datatype,
                    new_datatype=None,
                    change_target=None,
                    change_type=DELETE_PROPERTY,
                    old_hash=None,
                    new_hash=new_hash
                )

                if datatype_metadata:
                    for k, v in datatype_metadata.items():
                        old_value = v
                        new_value = None
                        
                        self.save_datatype_metadata_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_value,
                            new_value=new_value,
                            old_datatype=datatype,
                            new_datatype=None,
                            change_target=k,
                            change_type=DELETE_PROPERTY,
                            old_hash=None,
                            new_hash=new_hash
                        )

                # qualifier changes
                _ = self._handle_qualifier_changes(property_id, value_id, prev_stmt=None, curr_stmt=stmt)

                # references changes
                _ = self._handle_reference_changes(property_id, value_id, prev_stmt=None, curr_stmt=stmt)

        # If there's no description or label, the revisions shows them as []
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        labels = PageParser._safe_get_nested(revision, 'labels', lang, 'value')
        descriptions = PageParser._safe_get_nested(revision, 'descriptions', lang, 'value')

        # Process labels and descriptions (non-claim properties)
        for pid, val in [(LABEL_PROP_ID, labels), (DESCRIPTION_PROP_ID, descriptions)]:
            if val:
                old_value = val
                new_value = None

                self.save_changes(
                    pid, 
                    value_id='label' if pid == LABEL_PROP_ID else 'description',
                    old_value=old_value if not isinstance(old_value, dict) else None, # _safe_get_nested returns {} instead of None (?why)
                    new_value=new_value if not isinstance(new_value, dict) else None,
                    old_datatype='string',
                    new_datatype=None,
                    change_target=None,
                    change_type=DELETE_PROPERTY,
                    old_hash='',
                    new_hash=''
                )

    def _handle_description_label_change(self, previous_revision, current_revision):
        """
            Handles changes in labels and descriptions between two revisions.
            Generates new hashes for labels and descriptions when they change.
            Returns True if any change was detected, False otherwise.
        """
        change_detected = False

        # --- Label change ---
        prev_label = None
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        if previous_revision:
            prev_label = PageParser._safe_get_nested(previous_revision, 'labels', lang, 'value')
        curr_label = PageParser._safe_get_nested(current_revision, 'labels', lang, 'value')
        
        if curr_label != prev_label:
            change_detected = True

            old_value = prev_label if not isinstance(prev_label, dict) else None
            new_value = curr_label if not isinstance(curr_label, dict) else None

            self.save_changes(
                property_id=LABEL_PROP_ID,
                value_id='label',
                old_value=old_value,
                new_value=new_value,
                old_datatype='string' if old_value is not None else None,
                new_datatype='string' if new_value is not None else None,
                change_target=None,
                change_type=PageParser._description_label_change_type(prev_label, curr_label),
                change_magnitude=PageParser.magnitude_of_change(old_value, new_value, 'string'),
                old_hash='',
                new_hash=''
            )
            
        # --- Description change ---
        prev_desc = None
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        if previous_revision:
            prev_desc = PageParser._safe_get_nested(previous_revision, 'descriptions', lang, 'value')
        curr_desc = PageParser._safe_get_nested(current_revision, 'descriptions', lang, 'value')

        if curr_desc != prev_desc:
            change_detected = True
            old_value = prev_desc if not isinstance(prev_desc, dict) else None
            new_value = curr_desc if not isinstance(curr_desc, dict) else None

            self.save_changes(
                property_id=DESCRIPTION_PROP_ID,
                value_id='description',
                old_value=old_value,
                new_value=new_value,
                old_datatype='string' if old_value is not None else None,
                new_datatype='string' if new_value is not None else None,
                change_target=None,
                change_type=PageParser._description_label_change_type(prev_desc, curr_desc),
                change_magnitude=PageParser.magnitude_of_change(old_value, new_value, 'string'),
                old_hash='',
                new_hash=''
            )

        return change_detected
    
    def _handle_type_change():
        """
            Handles changes in the entity type between two revisions.
        """
        pass

    def _handle_new_pids(self, new_pids, curr_claims):
        """
            Handles new properties (P-IDs) in the current revision.
        """
        for new_pid in new_pids:
            curr_statements = curr_claims.get(new_pid, [])
            for s in curr_statements:
                new_value, new_datatype, new_datatype_metadata = PageParser._parse_datavalue(s)
                value_id = s.get('id', None)

                # add new type, if it's duplicated it will not be duplicated because we save a set
                if new_pid == 'P31':
                    self.entity_data['p31_types'].add((value_id, new_value))

                if new_pid == 'P279':
                    self.entity_data['p279_types'].add((value_id, new_value))

                old_hash = None
                new_hash = PageParser._get_property_mainsnak(s, 'hash') if s else None

                self._handle_value_changes(None, new_datatype, new_value, None, value_id, new_pid, CREATE_PROPERTY, old_hash, new_hash)

                if new_datatype_metadata:
                    self._handle_datatype_metadata_changes(None, new_datatype_metadata, value_id, None, new_datatype, new_pid, CREATE_PROPERTY, old_hash, new_hash)

                # rank
                curr_rank = s.get('rank') if s else None
                self.save_changes(
                    property_id=id_to_int(new_pid),
                    value_id=value_id,
                    old_value=None,
                    new_value=curr_rank,
                    old_datatype=None,
                    new_datatype=new_datatype,
                    change_target='rank',
                    change_type=CREATE_PROPERTY,
                    old_hash=None,
                    new_hash=new_hash
                )

                # qualifier changes
                _ = self._handle_qualifier_changes(new_pid, value_id, prev_stmt=None, curr_stmt=s)

                # reference changes
                _ = self._handle_reference_changes(new_pid, value_id, prev_stmt=None, curr_stmt=s)
    
    def _handle_removed_pids(self, removed_pids, prev_claims):
        """
            Handles changes for properties that were removed in the current revision
        """
        for removed_pid in removed_pids:
            prev_statements = prev_claims.get(removed_pid, [])

            for s in prev_statements:
                old_value, old_datatype, old_datatype_metadata = PageParser._parse_datavalue(s)
                value_id = s.get('id', None)

                # add new type, if it's duplicated it will not be duplicated because we save a set
                if removed_pid == 'P31':
                    self.entity_data['p31_types'].remove((value_id, old_value))

                if removed_pid == 'P279':
                    self.entity_data['p279_types'].remove((value_id, old_value))

                new_hash = None
                old_hash = PageParser._get_property_mainsnak(s, 'hash') if s else None

                self._handle_value_changes(old_datatype, None, None, old_value, value_id, removed_pid, DELETE_PROPERTY, old_hash, new_hash)

                if old_datatype_metadata:
                    self._handle_datatype_metadata_changes(old_datatype_metadata, {}, value_id, old_datatype, None, removed_pid, DELETE_PROPERTY, old_hash, new_hash)

                # rank
                prev_rank = s.get('rank') if s else None
                self.save_changes(
                    property_id=id_to_int(removed_pid),
                    value_id=value_id,
                    old_value=prev_rank,
                    new_value=None,
                    old_datatype=old_datatype,
                    new_datatype=None,
                    change_target='rank',
                    change_type=DELETE_PROPERTY,
                    old_hash=old_hash,
                    new_hash=None
                )

                # qualifier changes
                _ = self._handle_qualifier_changes(removed_pid, value_id, prev_stmt=s, curr_stmt=None)

                # references changes
                _ = self._handle_reference_changes(removed_pid, value_id, prev_stmt=s, curr_stmt=None)

    def _handle_rank_changes(self, prev_stmt, curr_stmt, pid, sid):
        prev_rank = prev_stmt.get('rank') if prev_stmt else None
        curr_rank = curr_stmt.get('rank') if curr_stmt else None

        old_hash = PageParser._get_property_mainsnak(prev_stmt, 'hash') if prev_stmt else None
        new_hash = PageParser._get_property_mainsnak(curr_stmt, 'hash') if curr_stmt else None

        _, new_datatype, _ = PageParser._parse_datavalue(curr_stmt)
        _, old_datatype, _ = PageParser._parse_datavalue(prev_stmt)

        change_detected = False
        if not prev_stmt:
            change_detected = True
            self.save_changes(
                property_id=id_to_int(pid),
                value_id=sid,
                old_value=None,
                new_value=curr_rank,
                old_datatype=old_datatype,
                new_datatype=new_datatype,
                change_target='rank',
                change_type=CREATE_PROPERTY_VALUE,
                old_hash=None,
                new_hash=new_hash
            )
        elif not curr_stmt:
            change_detected = True
            self.save_changes(
                property_id=id_to_int(pid),
                value_id=sid,
                old_value=prev_rank,
                new_value=None,
                old_datatype=old_datatype,
                new_datatype=new_datatype,
                change_target='rank',
                change_type=DELETE_PROPERTY_VALUE,
                old_hash=old_hash,
                new_hash=None
            )
        elif prev_stmt and curr_stmt and prev_rank != curr_rank:
            change_detected = True
            self.save_changes(
                property_id=id_to_int(pid),
                value_id=sid,
                old_value=prev_rank,
                new_value=curr_rank,
                old_datatype=old_datatype,
                new_datatype=new_datatype,
                change_target='rank',
                change_type=UPDATE_RANK,
                old_hash=old_hash,
                new_hash=new_hash
            )
        return change_detected

    def _handle_remaining_pids(self, remaining_pids, prev_claims, curr_claims):
        """
            Handles changes in properties that appear in current and previous revision.
            Returns True if a change was detected, otherwise False.

            Statement structure:
            {
                "claims": {
                    "P17": [
                    {
                        "id": "q60$5083E43C-228B-4E3E-B82A-4CB20A22A3FB",
                        "mainsnak": {},
                        "type": "statement",
                        "rank": "normal",
                        "qualifiers": {
                            "P580": [],
                            "P5436": []
                        },
                        "references": [
                        {
                            "hash": "d103e3541cc531fa54adcaffebde6bef28d87d32",
                            "snaks": []
                        }
                        ]
                    }
                    ]
                }
            }
        """

        change_detected = False

        for pid in remaining_pids:
            # Get statement for the same P-ID in previous and current revision
            prev_statements = prev_claims.get(pid, []) 
            curr_statements = curr_claims.get(pid, [])

            # Map by property-value ID
            prev_by_id = {stmt["id"]: stmt for stmt in prev_statements}
            curr_by_id = {stmt["id"]: stmt for stmt in curr_statements}

            # Get all property-value IDs
            all_statement_ids = set(prev_by_id.keys()).union(curr_by_id.keys())

            for sid in all_statement_ids:
                prev_stmt = prev_by_id.get(sid, None)
                curr_stmt = curr_by_id.get(sid, None)

                old_hash = None
                if prev_stmt:
                    prev_stmt['mainsnak'] = PageParser.homogenize_datavalue(prev_stmt['mainsnak'])
                    old_hash = PageParser.generate_value_hash(prev_stmt['mainsnak'])

                new_hash = None
                if curr_stmt:
                    curr_stmt['mainsnak'] = PageParser.homogenize_datavalue(curr_stmt['mainsnak'])
                    new_hash = PageParser.generate_value_hash(curr_stmt['mainsnak'])

                new_value, new_datatype, new_datatype_metadata = PageParser._parse_datavalue(curr_stmt)
                old_value, old_datatype, old_datatype_metadata = PageParser._parse_datavalue(prev_stmt)

                # old_hash = PageParser._get_property_mainsnak(prev_stmt, 'hash') if prev_stmt else None
                # new_hash = PageParser._get_property_mainsnak(curr_stmt, 'hash') if curr_stmt else None

                # value changes
                if prev_stmt and not curr_stmt:
                    change_detected = True
                    # Property value was removed -> the datatype is the datatype of the old_value

                    if pid == 'P31':
                        self.entity_data['p31_types'].remove((sid, old_value))

                    if pid == 'P279':
                        self.entity_data['p279_types'].remove((sid, old_value))

                    self._handle_value_changes(old_datatype, new_datatype, new_value, old_value, sid, pid, DELETE_PROPERTY_VALUE, old_hash, new_hash)

                    if old_datatype_metadata:
                        # Add change record for the datatype_metadata fields
                        self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, old_datatype, old_datatype, pid, DELETE_PROPERTY_VALUE, old_hash, new_hash)

                elif curr_stmt and not prev_stmt:
                    change_detected = True
                    # Property value was created

                    if pid == 'P31':
                        self.entity_data['p31_types'].add((sid, new_value))

                    if pid == 'P279':
                        self.entity_data['p279_types'].add((sid, new_value))

                    self._handle_value_changes(old_datatype, new_datatype, new_value, old_value, sid, pid, CREATE_PROPERTY_VALUE, old_hash, new_hash)

                    if new_datatype_metadata:
                        # Add change record for the datatype_metadata fields
                        self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, None, new_datatype, pid, CREATE_PROPERTY_VALUE, old_hash, new_hash)
                
                elif prev_stmt and curr_stmt and old_hash != new_hash:
                    change_detected = True
                    # Property was updated
                    if (old_datatype != new_datatype) or (old_value != new_value):
                        # Datatype change implies a value change

                        change_magnitude = None
                        if old_datatype == new_datatype and old_datatype not in WD_ENTITY_TYPES:
                            # Only calculate magnitude of change for non-entity datatypes
                            # and for the same datatype
                            change_magnitude = PageParser.magnitude_of_change(old_value, new_value, new_datatype)
                        
                        if new_datatype == 'time':
                            # don't consider changes like +00002025-10-01T:00:00:00Z to +2025-10-01T:00:00:00Z
                            # this is internal to WD representation
                            old_value_cleaned = re.sub(r'^([+-])0+(?=\d{4}-)', r'\1', old_value)
                            new_value_cleaned = re.sub(r'^([+-])0+(?=\d{4}-)', r'\1', new_value)
                            if old_value_cleaned != new_value_cleaned:
                                self._handle_value_changes(old_datatype, new_datatype, new_value_cleaned, old_value_cleaned, sid, pid, UPDATE_PROPERTY_VALUE, old_hash, new_hash, change_magnitude=change_magnitude)
                        else:
                            self._handle_value_changes(old_datatype, new_datatype, new_value, old_value, sid, pid, UPDATE_PROPERTY_VALUE, old_hash, new_hash, change_magnitude=change_magnitude)

                        if pid == 'P31':
                            self.entity_data['p31_types'].remove((sid, old_value))
                            self.entity_data['p31_types'].add((sid, new_value))

                        if pid == 'P279':
                            self.entity_data['p279_types'].remove((sid, old_value))
                            self.entity_data['p279_types'].add((sid, new_value))

                    if (old_datatype != new_datatype) or (old_datatype_metadata != new_datatype_metadata):
                        # Datatype change imples a datatype_metadata change
                        self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, old_datatype, new_datatype, pid, UPDATE_PROPERTY_DATATYPE_METADATA, old_hash, new_hash)
                
                # rank changes
                rank_change_detected = self._handle_rank_changes(prev_stmt, curr_stmt, pid, sid)

                # qualifier changes
                qualifier_change_detected = self._handle_qualifier_changes(pid, sid, prev_stmt=prev_stmt, curr_stmt=curr_stmt)

                # reference changes
                reference_change_detected = self._handle_reference_changes(pid, sid, prev_stmt=prev_stmt, curr_stmt=curr_stmt)

                change_detected = change_detected or rank_change_detected or qualifier_change_detected or reference_change_detected

        return change_detected
    
    def get_changes_from_revisions(self, current_revision, previous_revision):
        """
            Extracts changes between cureent_revision and previous_revision
            Returns True if there were changes detected, otherwise False
        """
        change_detected = False # Returns True if there were any changes detected
        if previous_revision is None:
            # Entity was created 
            self._changes_created_entity(current_revision)
            return True
        else:
            
            curr_label = PageParser._safe_get_nested(current_revision, 'labels')
            curr_desc = PageParser._safe_get_nested(current_revision, 'descriptions')
            curr_claims = PageParser._safe_get_nested(current_revision, 'claims')

            if 'redirect' in current_revision:
                self.current_revision_redirect = True
                return True

            if not curr_claims and not curr_label and not curr_desc:

                curr_aliases = PageParser._safe_get_nested(current_revision, 'aliases')
                curr_sitelinks = PageParser._safe_get_nested(current_revision, 'sitelinks')

                if curr_aliases or curr_sitelinks:
                    # skip revision 
                    # Reasons: can be an initial reivsion that only has sitelinks/aliases
                    return False

                # completely empty revision -> the item was cleaned, probably because of a merge
                # the following revision is probably a redirect
                if not curr_aliases and not curr_sitelinks:
                    # print(f"Revision {self.revision_meta['revision_id']} of entity {self.revision_meta['entity_id']} cleaned the entity")
                    self._changes_cleaned_entity(current_revision)
                    return True
            
            # --- Labels and Description changes ---
            change_detected = self._handle_description_label_change(previous_revision, current_revision)

            # --- Claims (P-IDs) ---
            prev_claims = PageParser._safe_get_nested(previous_revision, 'claims')

            prev_claims_pids = set(prev_claims.keys())
            curr_claims_pids = set(curr_claims.keys())
            
            # --- New properties in current revision ---
            new_pids = curr_claims_pids - prev_claims_pids
            if new_pids:
                change_detected = True
                self._handle_new_pids(new_pids, curr_claims)

            # --- Deleted properties in current revision ---
            removed_pids = prev_claims_pids - curr_claims_pids
            if removed_pids:
                change_detected = True
                self._handle_removed_pids(removed_pids, prev_claims)

            # --- Check updates of statements between revisions ---
            remaining_pids = prev_claims_pids.intersection(curr_claims_pids)
            if remaining_pids:
                change_detected_rem_pids = self._handle_remaining_pids(remaining_pids, prev_claims, curr_claims)

                change_detected = change_detected or change_detected_rem_pids

        return change_detected

    
    def process_page(self):
        """
            Processes all the revisions in a <page></page> and stores the extracted data in the corresponding tables revision, change and change_metadata
        """

        title_tag = f"{{{NS}}}title"
        revision_tag = f'{{{NS}}}revision'
        revision_text_tag = f'{{{NS}}}text'

        entity_id = ''

        previous_revision = None

        last_non_deleted_revision_id = -1
        prev_revision_deleted = False

        # Extract title = entity_id
        title_elem = self.page_elem.find(title_tag)
        if title_elem is not None:
            entity_id = (title_elem.text or '').strip()

        entity_id = id_to_int(entity_id) # convert Q-ID to integer (remove the 'Q')

        self.entity_stats['entity_id'] = entity_id

        entity_types_31 = self.feature_creation.extract_entity_p31(entity_id)
        entity_types_279 = self.feature_creation.extract_entity_p279(entity_id)

        self.entity_data['p31_labels_list'] = entity_types_31
        self.entity_data['p279_labels_list'] = entity_types_279

        # Iterate over revisions
        for rev_elem in self.page_elem.findall(revision_tag):

            revision_id = int(rev_elem.findtext(f'{{{NS}}}id', '').strip()) # revision id
            revision_text_elem = rev_elem.find(revision_text_tag) # revision <text></text>
            if revision_text_elem is not None:
                # If the revision was deleted the text tag looks like: <text bytes="11179" sha1="ou0t1tihux9rw2wb939kv22axo3h2uh" deleted="deleted"/>
                # and there's no content inside
                
                deleted_attr = revision_text_elem.get("deleted")
                if not deleted_attr: # Revision was not deleted
                    
                    # Extract text, id, timestamp, comment, username, user_id
                    contrib_elem = rev_elem.find(f'{{{NS}}}contributor')

                    prev_revision_id = rev_elem.findtext(f'{{{NS}}}parentid', '').strip()
                    if prev_revision_id and not prev_revision_deleted:
                        prev_revision_id = int(prev_revision_id)
                    elif prev_revision_deleted:
                        prev_revision_id = last_non_deleted_revision_id
                    else:
                        prev_revision_id = None # initial revision

                    if prev_revision_deleted:
                        prev_revision_deleted = False
                
                    if contrib_elem is not None:
                        username = (contrib_elem.findtext(f'{{{NS}}}username') or '').strip()
                        user_id = (contrib_elem.findtext(f'{{{NS}}}id') or '').strip()
                    else:
                        username = ''
                        user_id = ''

                    user_type = ''
                    if 'bot' in username.lower():
                        user_type = 'bot'
                    elif username == '':
                        user_type = 'anonymous'
                    else:
                        user_type = 'human'

                    # Save revision metadata (what will be stored in the revision table)
                    self.revision_meta = {
                        'entity_id': entity_id,
                        'revision_id': revision_id,
                        'prev_revision_id': prev_revision_id if prev_revision_id else '-1', # for the first revision (doesn't have a parentid)
                        'timestamp': rev_elem.findtext(f'{{{NS}}}timestamp', '').strip(),
                        'comment': rev_elem.findtext(f'{{{NS}}}comment', '').strip(),
                        'username': username,
                        'user_id': user_id,
                        'user_type': user_type,
                        'file_path': self.file_path
                    }

                    # decode content inside <text></text>
                    if revision_text_elem.text:
                        revision_text = (revision_text_elem.text).strip()
                        current_revision = self._parse_json_revision(rev_elem, revision_text)
                    
                    if current_revision is None or revision_text_elem.text is None:
                        # The json parsing for the revision text failed.
                        change = False
                    else:
                        # update label and description
                        curr_label, curr_alias, curr_desc = self.get_label_alias_description(current_revision)
                        if curr_label and self.entity_data['label'] != curr_label and curr_label != '':
                            self.entity_data['label'] = curr_label

                        if curr_desc and self.entity_data['description'] != curr_desc and curr_desc != '':
                            self.entity_data['description'] = curr_desc

                        if curr_alias and self.entity_data['alias'] != curr_alias and curr_alias != '':
                            self.entity_data['alias'] = curr_alias

                        # get changes for revision
                        change = self.get_changes_from_revisions(current_revision, previous_revision)

                    if change: # store revision if there was any change detected
                        
                        # Because revisions that modify aliases/sitelinks are not stored. Therefore, we store the 
                        # prev_revision_id as the last non deleted revision id that we actually stored in the DB.
                        if last_non_deleted_revision_id != self.revision_meta['prev_revision_id']:
                            prev_rev_id = last_non_deleted_revision_id
                        else:
                            prev_rev_id = self.revision_meta['prev_revision_id']

                        def extract_redirect_qid(rev_text):
                            """
                                The revision text looks like: {"entity":"Q11085307","redirect":"Q4126"}
                            """
                            if not rev_text:
                                return ''
                            redirect_entity = json.loads(rev_text)
                            redirect_entity = redirect_entity.get('redirect', '')

                            return id_to_int(redirect_entity)

                        self.revision.append((
                            prev_rev_id,
                            self.revision_meta['revision_id'],
                            self.revision_meta['entity_id'],
                            self.revision_meta['timestamp'],
                            get_time_feature(self.revision_meta['timestamp'], 'week'),
                            get_time_feature(self.revision_meta['timestamp'], 'year_month'),
                            get_time_feature(self.revision_meta['timestamp'], 'year'),
                            self.revision_meta['user_id'],
                            self.revision_meta['username'],
                            self.revision_meta['user_type'],
                            self.revision_meta['comment'],
                            self.revision_meta['file_path'],
                            self.current_revision_redirect,
                            extract_redirect_qid(revision_text) if self.current_revision_redirect else ''
                        ))

                        if self.revision_meta['user_type'] == 'bot':
                            self.entity_stats['num_bot_edits'] += 1
                        elif self.revision_meta['user_type'] == 'anonymous':
                            self.entity_stats['num_anonymous_edits'] += 1
                        else:
                            self.entity_stats['num_human_edits'] += 1

                        if self.current_revision_redirect:
                            self.current_revision_redirect = False

                        # for revisions that have been deleted
                        # we store prev_revision_id as the last non deleted revision
                        # NOTE: if there are no changes, we don't store revision information, therefore we 
                        # have to update this here
                        last_non_deleted_revision_id = revision_id

                        # if parse_revisions_text returns None then
                        # we only update previous_revision with an actual revision (that has a json in the revision <text></text>)
                        # 
                        if current_revision is not None:
                            previous_revision = current_revision
                    
                else: # revision was deleted
                    prev_revision_deleted = True

            # free memory
            rev_elem.clear()

        ## -------------------------------------------------- ##
        # Tag reverted edits
        ## -------------------------------------------------- ##
        self.changes, self.entity_stats = self.feature_creation.tag_reverted_edits(self.changes_by_pv, self.changes, self.entity_stats)
        
        ## -------------------------------------------------- ##
        # Upadte entity label
        ## -------------------------------------------------- ##
        for i, r in enumerate(self.revision):
            self.revision[i] = r + (self.entity_data['label'],)

        for i, c in enumerate(self.changes):
            self.changes[i] = c + (self.entity_data['label'],)

        for i, c in enumerate(self.datatype_metadata_changes):
            self.datatype_metadata_changes[i] = c + (self.entity_data['label'],)

        for i, c in enumerate(self.reference_changes):
            self.reference_changes[i] = c + (self.entity_data['label'],)

        for i, c in enumerate(self.qualifier_changes):
            self.qualifier_changes[i] = c + (self.entity_data['label'],)

        ## -------------------------------------------------- ##
        # Add entity label, description and types to feature tables
        ## -------------------------------------------------- ##
        if len(self.entity_features) > 0:
            for i, f in enumerate(self.entity_features):
                self.entity_features[i] = f + (
                    self.entity_data['label'],
                    self.entity_data['description'],
                    self.entity_data['p31_labels_list'],
                    self.entity_data['p279_labels_list'],
                    0.0, 
                    0.0, 
                    0.0,
                    ''
                )

        if len(self.text_features) > 0:
            for i, f in enumerate(self.text_features):
                self.text_features[i] = f + (
                    self.entity_data['label'],
                    self.entity_data['description'],
                    self.entity_data['p31_labels_list'],
                    self.entity_data['p279_labels_list'],
                    0.0,
                    '',
                )

        if len(self.time_features) > 0:
            for i, f in enumerate(self.time_features):
                self.time_features[i] = f + (
                    self.entity_data['label'],
                    self.entity_data['description'],
                    self.entity_data['p31_labels_list'],
                    self.entity_data['p279_labels_list'],
                    0.0,
                    '',
                )

        if len(self.globecoordinate_features) > 0:
            for i, f in enumerate(self.globecoordinate_features):
                self.globecoordinate_features[i] = f + (
                    self.entity_data['label'],
                    self.entity_data['description'],
                    self.entity_data['p31_labels_list'],
                    self.entity_data['p279_labels_list'],
                    0.0,
                    '',
                )

        if len(self.quantity_features) > 0:
            for i, f in enumerate(self.quantity_features):
                self.quantity_features[i] = f + (
                    self.entity_data['label'],
                    self.entity_data['description'],
                    self.entity_data['p31_labels_list'],
                    self.entity_data['p279_labels_list'],
                    0.0,
                    '',
                )

        ## -------------------------------------------------- ##
        # Filter entities and send them to corresponding tables
        ## -------------------------------------------------- ##
        is_scholarly_article = False
        list_of_types_31 = list(set([type_id for val_id, type_id in self.entity_data['p31_types']]))
        if len(list_of_types_31) > 0:
            for et in list_of_types_31:
                if et in self.SCHOLARLY_ARTICLE_TYPES: # get the value of P31
                    is_scholarly_article = True
                    break

        is_astronomical_object = False
        if len(list_of_types_31) > 0:
            for et in list_of_types_31:
                if et in self.ASTRONOMICAL_OBJECT_TYPES: # get the value of P31
                    is_astronomical_object = True
                    break
        
        # only for the remaining entitie
        has_less_revisions = False
        if not is_astronomical_object and not is_scholarly_article and self.entity_stats['num_value_changes'] <= REVISION_THRESHOLD:
            has_less_revisions = True

        entity_property_time_stats = self.finalize_entity_property_time_stats()

        # batch_insert(self.conn, 
        #             self.revision, self.changes, 
        #             # self.changes_metadata, 
        #             self.qualifier_changes, self.reference_changes, self.datatype_metadata_changes,
        #             self.entity_features,
        #             self.text_features,
        #             self.time_features,
        #             self.globecoordinate_features,
        #             self.quantity_features,
        #             reverted_edit_features,
        #             self.property_replacement_features,
        #             entity_property_time_stats,
        #             # self.datatype_metadata_changes_metadata, 
        #             is_scholarly_article, is_astronomical_object, has_less_revisions)

        ## -------------------------------------------------- ##
        # Add entity stats
        ## -------------------------------------------------- ##

        self.entity_stats['num_revisions'] = len(self.revision)

        self.entity_stats['num_qualifier_changes'] = len(self.qualifier_changes)
        self.entity_stats['num_reference_changes'] = len(self.reference_changes)

        self.entity_stats['entity_label'] = self.entity_data['label'] if self.entity_data['label'] else self.entity_data['alias']
        
        str_list = ', '.join(list_of_types_31)
        self.entity_stats['entity_types_31'] = str_list
        
        self.entity_stats['first_revision_timestamp'] = self.revision[0][3] if len(self.revision) > 0 else None # timestamp is at position 3 in the tuple
        self.entity_stats['last_revision_timestamp'] = self.revision[-1][3] if len(self.revision) > 0 else None

        # Clear element to free memory
        self.page_elem.clear()
        while self.page_elem.getprevious() is not None:
            del self.page_elem.getparent()[0]

        return {
            'revision': self.revision,
            'value_change': self.changes,
            'qualifier_change': self.qualifier_changes,
            'reference_change': self.reference_changes,
            'datatype_metadata_change': self.datatype_metadata_changes,
            'features_entity': self.entity_features,
            'features_text': self.text_features,
            'features_time': self.time_features,
            'features_globecoordinate': self.globecoordinate_features,
            'features_quantity': self.quantity_features,
            # 'features_reverted_edit': reverted_edit_features,
            'features_property_replacement': self.property_replacement_features,
            'entity_property_time_stats': entity_property_time_stats,
            'is_scholarly_article': is_scholarly_article,
            'is_astronomical_object': is_astronomical_object,
            'has_less_revisions': has_less_revisions,
            'entity_stats': [tuple(self.entity_stats.get(col) for col in ENTITY_STATS_COLS)]
        }