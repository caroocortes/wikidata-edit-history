import html
import json
import Levenshtein
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
from lxml import etree
import hashlib

from scripts.utils import haversine_metric, get_time_dict, gregorian_to_julian, insert_rows, update_entity_label
from scripts.const import *

def batch_insert(conn, revision, changes, change_metadata):
    # NOTE: copy may be faster
    """Function to insert into DB in parallel."""
    
    try:
        insert_rows(conn, 'revision', revision, ['revision_id', 'entity_id', 'entity_label', 'timestamp', 'user_id', 'username', 'comment', 'file_path', 'class_id', 'class_label'])
        insert_rows(conn, 'change', changes, ['revision_id', 'property_id', 'value_id', 'old_value', 'new_value', 'datatype', 'datatype_metadata', 'action', 'target', 'old_hash', 'new_hash'])
        insert_rows(conn, 'change_metadata', change_metadata, ['revision_id', 'property_id', 'value_id', 'datatype_metadata', 'change_metadata', 'value'])
    except Exception as e:
        print(f'There was an error when batch inserting revisions and changes: {e}')
        sys.stdout.flush()

class PageParser():
    def __init__(self, file_path, page_elem_str, config):

        self.changes = []
        self.revision = []
        self.changes_metadata = []

        self.config = config

        self.label_hash_counter = 0
        self.description_hash_counter = 0

        self.label_hash = ''
        self.description_hash = ''

        self.revision_meta = {}

        self.file_path = file_path # file_path of XML where the page is stored
        self.page_elem = etree.fromstring(page_elem_str) # XML page for the entity

        self.db_executor = ThreadPoolExecutor(max_workers=1)  # db executor thread for DB inserts, so the parser can keep working

        dotenv_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(dotenv_path)

        # credentials for DB connection
        DB_USER = os.environ.get("DB_USER")
        DB_PASS = os.environ.get("DB_PASS")
        DB_NAME = os.environ.get("DB_NAME")
        DB_HOST = os.environ.get("DB_HOST")
        DB_PORT = os.environ.get("DB_PORT")

        self.conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS, 
            host=DB_HOST,
            port=DB_PORT
        )

    def _parse_json_revision(self, revision_elem, revision_text):
        # TODO: remove revision_elem from args - only for debugging
        """
            Returns the text of a revision as a json
        """
        json_text = html.unescape(revision_text.strip())
        try:
            current_revision = json.loads(json_text)
            return current_revision
        except json.JSONDecodeError as e:
            print(f'Error decoding JSON in revision {self.revision_meta['revision_id']} for entity {self.revision_meta['entity_id']}: {e}. Revision skipped. Revision text: {revision_text}')
            
            with open("error_revision_text.txt", "a") as f:
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
    def generate_unique_hash(counter):
        """
            Generates a unique hash from a counter
        """
        return hashlib.sha1(str(counter).encode()).hexdigest()

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

                new_julian = gregorian_to_julian(new_dict['year'], new_dict['month'], new_dict['day'])
                old_julian = gregorian_to_julian(old_dict['year'], old_dict['month'], old_dict['day'])

                return float(new_julian - old_julian) # distance in days
            
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
    
    def get_label(self, revision):
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        label = PageParser._safe_get_nested(revision, 'labels', lang, 'value') 
        return label if not isinstance(label, dict) else None
    
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

    def save_changes(self, property_id, value_id, old_value, new_value, datatype, datatype_metadata, change_type, change_magnitude=None, old_hash=None, new_hash=None):
        
        old_value = json.dumps(str(old_value)) if old_value else None
        new_value = json.dumps(str(new_value)) if new_value else None

        action, target = PageParser.get_target_action_from_change_type(change_type)

        change = (
            self.revision_meta['revision_id'] if self.revision_meta['revision_id'] else '',
            property_id if property_id else '',
            value_id if value_id else '',
            old_value,
            new_value,
            datatype,
            datatype_metadata if datatype_metadata else '', # can't be None since datatype_metadata is part of the key of the table
            action,
            target,
            old_hash,
            new_hash
        )

        self.changes.append(change)

        change_metadata = ()
        if change_magnitude is not None:
            change_metadata = (
                self.revision_meta['revision_id'] if self.revision_meta['revision_id'] else '',
                property_id if property_id else '',
                value_id if value_id else '',
                datatype_metadata if datatype_metadata else '', # can't be None since datatype_metadata is part of the key of the table
                'CHANGE_MAGNITUDE',
                change_magnitude
            )
            self.changes_metadata.append(change_metadata)
        
    def _handle_datatype_metadata_changes(self, old_datatype_metadata, new_datatype_metadata, datavalue_id, old_datatype, new_datatype, property_id, change_type, old_hash, new_hash):
        
        if old_datatype == new_datatype:
        
            for key in set((old_datatype_metadata or {}).keys()):
                old_meta = (old_datatype_metadata or {}).get(key, None)
                new_meta = (new_datatype_metadata or {}).get(key, None)

                if key not in ('calendarmodel', 'globe', 'unit'): # this metadata stores an entity link so we don't calculate the magnitude of change
                    change_magnitude = PageParser.magnitude_of_change(old_meta, new_meta, new_datatype, metadata=True)
                else: 
                    change_magnitude = None

                if old_meta != new_meta: # save only what changed

                    self.save_changes(
                        property_id,
                        value_id=datavalue_id,
                        old_value=old_meta,
                        new_value=new_meta,
                        datatype=new_datatype,
                        datatype_metadata=key,
                        change_type=change_type, 
                        change_magnitude=change_magnitude,
                        old_hash=old_hash,
                        new_hash=new_hash
                    )

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
                    
                    old_meta_key = next((k for k in old_keys_set if k not in keys_to_skip), None)
                    old_meta = (old_datatype_metadata or {}).get(old_meta_key, None)

                    if old_meta_key is not None:
                        keys_to_skip.add(old_meta_key)
                else:
                    old_meta = (old_datatype_metadata or {}).get(key, None)

                    new_meta_key = next((k for k in new_keys_set if k not in keys_to_skip), None)
                    new_meta = (new_datatype_metadata or {}).get(new_meta_key, None)

                    if new_meta_key is not None:
                        keys_to_skip.add(new_meta_key)
                
                self.save_changes(
                    property_id,
                    value_id=datavalue_id,
                    old_value=old_meta,
                    new_value=new_meta,
                    datatype=new_datatype,
                    datatype_metadata=key,
                    change_type=change_type,
                    old_hash=old_hash,
                    new_hash=new_hash
                )
            
            remaining_keys = big_set - keys_to_skip
            for key in remaining_keys:
                
                if big_old:
                    old_meta = (old_datatype_metadata or {}).get(key, None)
                    new_meta = None
                else:
                    new_meta = (new_datatype_metadata or {}).get(key, None)
                    old_meta = None
                
                self.save_changes(
                    property_id,
                    value_id=datavalue_id,
                    old_value=old_meta,
                    new_value=new_meta,
                    datatype=new_datatype,
                    datatype_metadata=key,
                    change_type=change_type,
                    old_hash=old_hash,
                    new_hash=new_hash
                )
    
    def _handle_value_changes(self, new_datatype, new_value, old_value, datavalue_id, property_id, change_type, old_hash, new_hash, change_magnitude=None):

        self.save_changes(
            property_id, 
            value_id=datavalue_id,
            old_value=old_value,
            new_value=new_value,
            datatype=new_datatype,
            datatype_metadata=None,
            change_type=change_type,
            change_magnitude=change_magnitude,
            old_hash=old_hash,
            new_hash=new_hash
        )

    def _changes_deleted_created_entity(self, revision, change_type):

        # Process claims
        claims = PageParser._safe_get_nested(revision, 'claims')
        
        for property_id, property_stmts in claims.items():
            for stmt in property_stmts:
                
                value, datatype, datatype_metadata = PageParser._parse_datavalue(stmt)
                new_hash = PageParser._get_property_mainsnak(stmt, 'hash') if stmt else None
                datavalue_id = stmt.get('id', None)
                
                old_value = None if change_type == CREATE_ENTITY else value
                new_value = value if change_type == CREATE_ENTITY else None
                
                self.save_changes(
                    property_id, 
                    value_id=datavalue_id,
                    old_value=old_value,
                    new_value=new_value,
                    datatype=datatype,
                    datatype_metadata=None,
                    change_type=change_type,
                    old_hash=None if change_type == CREATE_ENTITY else new_hash,
                    new_hash=new_hash if change_type == CREATE_ENTITY else None
                )

                if datatype_metadata:
                    for k, v in datatype_metadata.items():
                        old_value = None if change_type == CREATE_ENTITY else v
                        new_value = v if change_type == CREATE_ENTITY else None
                        
                        self.save_changes(
                            property_id,
                            value_id=datavalue_id,
                            old_value=old_value,
                            new_value=new_value,
                            datatype=datatype,
                            datatype_metadata=k,
                            change_type=change_type,
                            old_hash=None if change_type == CREATE_ENTITY else new_hash,
                            new_hash=new_hash if change_type == CREATE_ENTITY else None
                        )

        # If there's no description or label, the revisions shows them as []
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        labels = PageParser._safe_get_nested(revision, 'labels', lang, 'value')
        descriptions = PageParser._safe_get_nested(revision, 'descriptions', lang, 'value')

        # Process labels and descriptions (non-claim properties)
        for pid, val in [('label', labels), ('description', descriptions)]:
            if val:
                old_value = None if change_type == CREATE_ENTITY else val
                new_value = val if change_type == CREATE_ENTITY else None

                # Label and description don't have hashes, so
                # I manually create it
                if pid == 'label':
                    old_hash = None if change_type == CREATE_ENTITY else self.label_hash
                    if change_type == CREATE_ENTITY:
                        # generate hash for the label
                        self.label_hash = PageParser.generate_unique_hash(self.label_hash_counter)
                    
                    new_hash = self.label_hash if change_type == CREATE_ENTITY else None
  
                if pid == 'description':
                    old_hash = None if change_type == CREATE_ENTITY else self.description_hash
                    if change_type == CREATE_ENTITY:
                        # generate hash for the description
                        self.description_hash = PageParser.generate_unique_hash(self.description_hash_counter)
                    
                    new_hash = self.description_hash if change_type == CREATE_ENTITY else None

                self.save_changes(
                        pid, 
                        value_id=pid,
                        old_value=old_value if not isinstance(old_value, dict) else None,
                        new_value=new_value if not isinstance(new_value, dict) else None,
                        datatype='string',
                        datatype_metadata=None,
                        change_type=change_type,
                        old_hash=old_hash,
                        new_hash=new_hash
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

            # Generate new hash for label
            old_hash = self.label_hash
            self.label_hash_counter += 1
            self.label_hash = PageParser.generate_unique_hash(self.label_hash_counter)

            self.save_changes(
                property_id="label",
                value_id='label',
                old_value=old_value,
                new_value=new_value,
                datatype='string',
                datatype_metadata=None,
                change_type=PageParser._description_label_change_type(prev_label, curr_label),
                change_magnitude=PageParser.magnitude_of_change(old_value, new_value, 'string'),
                old_hash=old_hash,
                new_hash=self.label_hash
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

            # Generate new hash for description
            old_hash = self.description_hash
            self.description_hash_counter += 1
            self.description_hash = PageParser.generate_unique_hash(self.description_hash_counter)

            self.save_changes(
                property_id="description",
                value_id='description',
                old_value=old_value,
                new_value=new_value,
                datatype='string',
                datatype_metadata=None,
                change_type=PageParser._description_label_change_type(prev_desc, curr_desc),
                change_magnitude=PageParser.magnitude_of_change(old_value, new_value, 'string'),
                old_hash=old_hash,
                new_hash=self.description_hash
            )

        return change_detected
    
    def _handle_new_pids(self, new_pids, curr_claims):
        """
            Handles new properties (P-IDs) in the current revision.
        """
        for new_pid in new_pids:
            curr_statements = curr_claims.get(new_pid, [])
            for s in curr_statements:
                new_value, new_datatype, new_datatype_metadata = PageParser._parse_datavalue(s)
                datavalue_id = s.get('id', None)

                old_hash = None
                new_hash = PageParser._get_property_mainsnak(s, 'hash') if s else None

                self._handle_value_changes(new_datatype, new_value, None, datavalue_id, new_pid, CREATE_PROPERTY, old_hash, new_hash)

                if new_datatype_metadata:
                    self._handle_datatype_metadata_changes(None, new_datatype_metadata, datavalue_id, None, new_datatype, new_pid, CREATE_PROPERTY, old_hash, new_hash)
    
    def _handle_removed_pids(self, removed_pids, prev_claims):
        """
            Handles changes for properties that were removed in the current revision
        """
        for removed_pid in removed_pids:
            prev_statements = prev_claims.get(removed_pid, [])

            for s in prev_statements:
                old_value, old_datatype, old_datatype_metadata = PageParser._parse_datavalue(s)

                datavalue_id = s.get('id', None)

                new_hash = None
                old_hash = PageParser._get_property_mainsnak(s, 'hash') if s else None

                self._handle_value_changes(None, None, old_value, datavalue_id, removed_pid, DELETE_PROPERTY, old_hash, new_hash)

                if old_datatype_metadata:
                    self._handle_datatype_metadata_changes(old_datatype_metadata, {}, datavalue_id, old_datatype, None, removed_pid, DELETE_PROPERTY, old_hash, new_hash)

    def _handle_remaining_pids(self, remaining_pids, prev_claims, curr_claims):
        """
            Handles changes in properties that appear in current and previous revision.
            Returns True if a change was detected, otherwise False.
        """

        change_detected = False

        for pid in remaining_pids:
            # Get statement for the same P-ID in previous and current revision
            prev_statements = prev_claims.get(pid, []) 
            curr_statements = curr_claims.get(pid, [])

            # Map by value ID
            prev_by_id = {stmt["id"]: stmt for stmt in prev_statements}
            curr_by_id = {stmt["id"]: stmt for stmt in curr_statements}

            all_statement_ids = set(prev_by_id.keys()).union(curr_by_id.keys())

            for sid in all_statement_ids:
                prev_stmt = prev_by_id.get(sid)
                curr_stmt = curr_by_id.get(sid)

                new_value, new_datatype, new_datatype_metadata = PageParser._parse_datavalue(curr_stmt)
                old_value, old_datatype, old_datatype_metadata = PageParser._parse_datavalue(prev_stmt)

                old_hash = PageParser._get_property_mainsnak(prev_stmt, 'hash') if prev_stmt else None
                new_hash = PageParser._get_property_mainsnak(curr_stmt, 'hash') if curr_stmt else None

                if prev_stmt and not curr_stmt:
                    change_detected = True
                    # Property value was removed -> the datatype is the datatype of the old_value
                    self._handle_value_changes(old_datatype, new_value, old_value, sid, pid, DELETE_PROPERTY_VALUE, old_hash, new_hash)

                    if old_datatype_metadata:
                        # Add change record for the datatype_metadata fields
                        self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, old_datatype, old_datatype, pid, DELETE_PROPERTY_VALUE, old_hash, new_hash)

                elif curr_stmt and not prev_stmt:
                    change_detected = True
                    # Property value was created
                    self._handle_value_changes(new_datatype, new_value, old_value, sid, pid, CREATE_PROPERTY_VALUE, old_hash, new_hash)

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
                        
                        self._handle_value_changes(new_datatype, new_value, old_value, sid, pid, UPDATE_PROPERTY_VALUE, old_hash, new_hash, change_magnitude=change_magnitude)

                    if (old_datatype != new_datatype) or (old_datatype_metadata != new_datatype_metadata):
                        # Datatype change imples a datatype_metadata change
                        self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, old_datatype, new_datatype, pid, UPDATE_PROPERTY_DATATYPE_METADATA, old_hash, new_hash)

        return change_detected
    
    def get_changes_from_revisions(self, current_revision, previous_revision):
        """
            Extracts changes between cureent_revision and previous_revision
            Returns True if there were changes detected, otherwise False
        """
        change_detected = False # Returns True if there were any changes detected
        if previous_revision is None:
            # Entity was created again or for the first time
            self._changes_deleted_created_entity(current_revision, CREATE_ENTITY)
            return True
        else:
            
            curr_label = PageParser._safe_get_nested(current_revision, 'labels')
            curr_desc = PageParser._safe_get_nested(current_revision, 'descriptions')
            curr_claims = PageParser._safe_get_nested(current_revision, 'claims')

            if not curr_claims and not curr_label and not curr_desc:
                # Skipped revision -> could be a deleted revision, not necessarily a deleted entity
                print(f'Revision does not contain labels, descriptions, nor claims. Skipped revision {self.revision_meta['revision_id']} for entity {self.revision_meta['entity_id']}')
                return False
            
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
                prev_change_detected = change_detected # in case there was a new/removed property and no changes in existing properties
                change_detected = self._handle_remaining_pids(remaining_pids, prev_claims, curr_claims)

                if prev_change_detected:
                    change_detected = True

        return change_detected

    def process_page(self):
        """
            Processes all the revisions in a <page></page> and stores the extracted data in the corresponding tables revision, change and change_metadata
        """

        title_tag = f"{{{NS}}}title"
        revision_tag = f'{{{NS}}}revision'
        revision_text_tag = f'{{{NS}}}text'

        entity_id = ''
        entity_label = ''

        previous_revision = None

        # Extract title = entity_id
        title_elem = self.page_elem.find(title_tag)
        if title_elem is not None:
            entity_id = (title_elem.text or '').strip()
    
        # Iterate over revisions
        for rev_elem in self.page_elem.findall(revision_tag):

            # Get revision text
            revision_id = rev_elem.findtext(f'{{{NS}}}id', '').strip()
            revision_text = rev_elem.findtext(revision_text_tag)
            if revision_text is not None:
                # If the revision was deleted the text tag looks like: <text bytes="11179" sha1="ou0t1tihux9rw2wb939kv22axo3h2uh" deleted="deleted"/>
                # and there's no content inside
                deleted_attr = rev_elem.get("deleted")
                if deleted_attr == "deleted":
                    file_path = 'deleted_revisions.json'
                    if os.path.exists(file_path):
                        with open(file_path, "r", encoding="utf-8") as f:
                            try:
                                deleted_revisions = json.load(f)
                            except json.JSONDecodeError:
                                deleted_revisions = {}
                    else:
                        deleted_revisions = {}

                    deleted_revisions[entity_id] = revision_id

                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(deleted_revisions, f, ensure_ascii=False, indent=2)
                else:
                    # Revision was not deleted

                    # Extract text, id, timestamp, comment, username, user_id
                    contrib_elem = rev_elem.find(f'{{{NS}}}contributor')
                
                    if contrib_elem is not None:
                        username = (contrib_elem.findtext(f'{{{NS}}}username') or '').strip()
                        user_id = (contrib_elem.findtext(f'{{{NS}}}id') or '').strip()
                    else:
                        username = ''
                        user_id = ''

                    # Save revision metadata (what will be stored in the revision table)
                    self.revision_meta = {
                        'entity_id': entity_id,
                        'entity_label': entity_label,
                        'revision_id': revision_id,
                        'timestamp': rev_elem.findtext(f'{{{NS}}}timestamp', '').strip(),
                        'comment': rev_elem.findtext(f'{{{NS}}}comment', '').strip(),
                        'username': username,
                        'user_id': user_id,
                        'file_path': self.file_path,
                        'class_id': '',
                        'class_label': ''
                    }

                    # decode content inside <text></text>
                    revision_text = revision_text.strip()
                    current_revision = self._parse_json_revision(rev_elem, revision_text)
                    
                    if current_revision is None:
                        # The json parsing for the revision text failed.
                        change = False
                    else:
                        # update label
                        curr_label = self.get_label(current_revision)
                        if curr_label and entity_label != curr_label and curr_label != '':
                            entity_label = curr_label

                        # get changes for revision
                        change = self.get_changes_from_revisions(current_revision, previous_revision)

                    if change: # store revision if there was any change detected

                        self.revision.append((
                            self.revision_meta['revision_id'],
                            self.revision_meta['entity_id'],
                            self.revision_meta['entity_label'],
                            self.revision_meta['timestamp'],
                            self.revision_meta['user_id'],
                            self.revision_meta['username'],
                            self.revision_meta['comment'],
                            self.revision_meta['file_path'],
                            self.revision_meta['class_id'],
                            self.revision_meta['class_label']
                        ))

                    # if parse_revisions_text returns None then
                    # we only update previous_revision with an actual revision (that has a json in the revision <text></text>)
                    if current_revision is not None:
                        previous_revision = current_revision
                    
                    # Batch insert (changes >= revision because one revision can have multiple changes)
                    batch_size = int(self.config.get('batch_changes_store', 10000))
                    if len(self.changes) >= batch_size:
                        self.db_executor.submit(batch_insert, self.conn, self.revision, self.changes, self.changes_metadata)
                        # remove already stored changes + revisions to avoid duplicates
                        self.changes = []
                        self.revision = []
                        self.changes_metadata = []

            # free memory
            rev_elem.clear()
        
        # Insert remaining changes + revision + changes_metadata in case the batch size was not reached
        if self.changes:
            batch_insert(self.conn, self.revision, self.changes, self.changes_metadata)
            self.changes = []
            self.revision = []
            self.changes_metadata = []

        # Update entity label with last existing label
        update_entity_label(self.conn, entity_id, entity_label)

        # Clear element to free memory
        self.page_elem.clear()
        while self.page_elem.getprevious() is not None:
            del self.page_elem.getparent()[0]