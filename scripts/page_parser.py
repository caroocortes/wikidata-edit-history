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
import re
import hashlib

from scripts.utils import haversine_metric, get_time_dict, gregorian_to_julian, insert_rows, update_entity_label, id_to_int
from scripts.const import *

def batch_insert(conn, revision, changes, change_metadata, reference_qualifier_changes):
    # NOTE: copy may be faster
    """Function to insert into DB in parallel."""
    
    try:
        insert_rows(conn, 'revision', revision, ['prev_revision_id', 'revision_id', 'entity_id', 'entity_label', 'timestamp', 'user_id', 'username', 'comment', 'file_path', 'redirect'])
        insert_rows(conn, 'value_change', changes, ['revision_id', 'property_id', 'value_id', 'old_value', 'new_value', 'datatype', 'change_target', 'action', 'target', 'old_hash', 'new_hash'])
        insert_rows(conn, 'value_change_metadata', change_metadata, ['revision_id', 'property_id', 'value_id', 'change_target', 'change_metadata', 'value'])
        insert_rows(conn, 'reference_qualifier_change', reference_qualifier_changes, ['revision_id', 'property_id', 'value_id', 'rq_property_id', 'value_hash', 'old_value', 'new_value', 'datatype', 'change_target', 'action', 'target'])
    except Exception as e:
        print(f'There was an error when batch inserting revisions and changes: {e}')
        sys.stdout.flush()

class PageParser():
    def __init__(self, file_path, page_elem_str, config):
        
        self.changes = []
        self.revision = []
        self.changes_metadata = []
        self.reference_qualifier_changes = []

        self.config = config

        self.current_revision_redirect = False

        self.revision_meta = {}

        self.file_path = file_path # file_path of XML where the page is stored
        self.page_elem = etree.fromstring(page_elem_str) # XML page for the entity

        self.db_executor = ThreadPoolExecutor(max_workers=1)  # db executor thread for DB inserts, so the parser can keep working

        self.batch_size = int(self.config.get('batch_changes_store', 10000))

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

            print(f'Error decoding JSON in revision {self.revision_meta['revision_id']} for entity {self.revision_meta['entity_id']}: {e}. Revision skipped. See {ERROR_REVISION_TEXT_PATH} for details.')
            
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
    
    def get_label(self, revision):
        lang = self.config['language'] if 'language' in self.config and self.config['language'] else 'en'
        label = PageParser._safe_get_nested(revision, 'labels', lang, 'value') 
        return label if not isinstance(label, dict) else ''
    
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

    def save_changes(self, property_id, value_id, old_value, new_value, datatype, change_target, change_type, change_magnitude=None, old_hash=None, new_hash=None):
        """
            Store value + datatype metadata (of property value) + rank changes
        """
        old_value = json.dumps(str(old_value)) if old_value else None
        new_value = json.dumps(str(new_value)) if new_value else None

        action, target = PageParser.get_target_action_from_change_type(change_type)

        change = (
            self.revision_meta['revision_id'],
            property_id,
            value_id,
            old_value if old_value else {},
            new_value if new_value else {},
            datatype,
            change_target if change_target else '', # can't be None since change_target is part of the key of the table
            action,
            target,
            old_hash if old_hash else '',
            new_hash if new_hash else ''
        )

        self.changes.append(change)

        change_metadata = ()
        if change_magnitude is not None:
            change_metadata = (
                self.revision_meta['revision_id'],
                property_id,
                value_id,
                change_target if change_target else '', # can't be None since change_target is part of the key of the table
                'CHANGE_MAGNITUDE',
                change_magnitude
            )
            self.changes_metadata.append(change_metadata)
        
    def save_reference_qualifier_changes(self, property_id, value_id, rq_property_id, value_hash, old_value, new_value, datatype, change_target, change_type):
        """
            Store reference/qualifier changes
        """
        old_value = json.dumps(str(old_value)) if old_value else None
        new_value = json.dumps(str(new_value)) if new_value else None

        action, target = PageParser.get_target_action_from_change_type(change_type)

        change = (
            self.revision_meta['revision_id'],
            property_id,
            value_id,
            rq_property_id,
            value_hash,
            old_value,
            new_value,
            datatype,
            change_target if change_target else '', # can't be None since change_target is part of the key of the table
            action,
            target
        )

        self.reference_qualifier_changes.append(change)

    def _handle_datatype_metadata_changes(self, old_datatype_metadata, new_datatype_metadata, value_id, old_datatype, new_datatype, property_id, change_type, old_hash=None, new_hash=None, type_='value', rq_property_id=None, value_hash=None):
        
        if old_datatype_metadata and not new_datatype_metadata: # deletion
            for key in old_datatype_metadata.keys():
                old_meta = old_datatype_metadata.get(key, None)
                
                if type_ == 'value':
                    self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=None,
                            datatype=old_datatype,
                            change_target=key,
                            change_type=change_type, 
                            old_hash=old_hash,
                            new_hash=None
                        )
                else:
                    self.save_reference_qualifier_changes(
                        id_to_int(property_id),
                        value_id=value_id,
                        rq_property_id=id_to_int(rq_property_id),
                        value_hash=value_hash,
                        old_value=old_meta,
                        new_value=None,
                        datatype=old_datatype,  # Use old_datatype, not new_datatype
                        change_target=key,
                        change_type=change_type
                    )
            return
        
        if new_datatype_metadata and not old_datatype_metadata: # creation
            for key in new_datatype_metadata.keys():
                new_meta = new_datatype_metadata.get(key, None)
                
                if type_ == 'value':
                    self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=None,
                            new_value=new_meta,
                            datatype=new_datatype,
                            change_target=key,
                            change_type=change_type, 
                            old_hash=None,
                            new_hash=new_hash
                        )
                else:
                    self.save_reference_qualifier_changes(
                        id_to_int(property_id),
                        value_id=value_id,
                        rq_property_id=id_to_int(rq_property_id),
                        value_hash=value_hash,
                        old_value=None,
                        new_value=new_meta,
                        datatype=new_datatype,
                        change_target=key,
                        change_type=change_type
                    )
            return

        if old_datatype == new_datatype:
        
            for key in set((old_datatype_metadata or {}).keys()):
                old_meta = (old_datatype_metadata or {}).get(key, None)
                new_meta = (new_datatype_metadata or {}).get(key, None)

                if key not in ('calendarmodel', 'globe', 'unit'): # this metadata stores an entity link so we don't calculate the magnitude of change
                    change_magnitude = PageParser.magnitude_of_change(old_meta, new_meta, new_datatype, metadata=True)
                else: 
                    change_magnitude = None

                if old_meta != new_meta: # save only what changed
                    
                    if type_ == 'value':
                        self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_meta,
                            new_value=new_meta,
                            datatype=new_datatype,
                            change_target=key,
                            change_type=change_type, 
                            change_magnitude=change_magnitude,
                            old_hash=old_hash,
                            new_hash=new_hash
                        )
                    else: # value == 'reference_qualifier'
                        self.save_reference_qualifier_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            rq_property_id=id_to_int(rq_property_id),
                            value_hash=value_hash,
                            old_value=old_meta,
                            new_value=new_meta,
                            datatype=new_datatype,
                            change_target=key,
                            change_type=change_type
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
                
                if type_ == 'value':
                    self.save_changes(
                        id_to_int(property_id),
                        value_id=value_id,
                        old_value=old_meta,
                        new_value=new_meta,
                        datatype=new_datatype,
                        change_target=key,
                        change_type=change_type,
                        old_hash=old_hash,
                        new_hash=new_hash
                    )
                else: # value == 'reference_qualifier'
                    self.save_reference_qualifier_changes(
                        id_to_int(property_id),
                        value_id=value_id,
                        rq_property_id=id_to_int(rq_property_id),
                        value_hash=value_hash,
                        old_value=old_meta,
                        new_value=new_meta,
                        datatype=new_datatype,
                        change_target=key,
                        change_type=change_type
                    )
            
            remaining_keys = big_set - keys_to_skip
            for key in remaining_keys:
                
                if big_old:
                    old_meta = (old_datatype_metadata or {}).get(key, None)
                    new_meta = None
                else:
                    new_meta = (new_datatype_metadata or {}).get(key, None)
                    old_meta = None
                
                if type_ == 'value':
                    self.save_changes(
                        id_to_int(property_id),
                        value_id=value_id,
                        old_value=old_meta,
                        new_value=new_meta,
                        datatype=old_datatype,
                        change_target=key,
                        change_type=change_type,
                        old_hash=old_hash,
                        new_hash=new_hash
                    )
                else: # value == 'reference_qualifier'
                    self.save_reference_qualifier_changes(
                        id_to_int(property_id),
                        value_id=value_id,
                        rq_property_id=id_to_int(rq_property_id),
                        value_hash=value_hash,
                        old_value=old_meta,
                        new_value=new_meta,
                        datatype=old_datatype,
                        change_target=key,
                        change_type=change_type
                    )
    
    def _handle_value_changes(self, new_datatype, new_value, old_value, value_id, property_id, change_type, old_hash, new_hash, change_magnitude=None):

        self.save_changes(
            id_to_int(property_id), 
            value_id=value_id,
            old_value=old_value,
            new_value=new_value,
            datatype=new_datatype,
            change_target=None,
            change_type=change_type,
            change_magnitude=change_magnitude,
            old_hash=old_hash,
            new_hash=new_hash
        )

    @staticmethod
    def generate_value_hash(prop_val):
        """
            Input:
            - prop_val: whole snak for a property value (includes snaktype, hash, datavalue)
            Generates a hash from the datavalue.
            Removes inconsistencies that happen in WD due to schema changes
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
        if not prop_val:
            return None

        snaktype = prop_val.get('snaktype', None)
        current_hash = prop_val.get('hash', None)
        
        if snaktype in (NO_VALUE, SOME_VALUE) or \
            (
                snaktype == 'value' and 
                prop_val['datavalue']['type'] not in WD_ENTITY_TYPES and
                prop_val['datavalue']['type'] not in ('time', 'globecoordinate')
             ):
            return current_hash
        else:
            type_ = prop_val['datavalue']['type']
            # Remove inconsistencies in time values + entities + unused/deprcated fields in time and globecoordinate
            if type_ == 'globecoordinate':
                prop_val['datavalue']['value'].pop("altitude", None)

            if type_ == 'time':
                # remove unused values
                prop_val['datavalue']['value'].pop("before", None)
                prop_val['datavalue']['value'].pop("after", None)
                
                # remove 0's at the beggining
                prop_val['datavalue']['value']['time'] = re.sub(r'^([+-])0+(?=\d{4}-)', r'\1', prop_val['datavalue']['value']['time'])

            if type_ in WD_ENTITY_TYPES:
                # NOTE: From WD's doc, not all entities have a numeric-id
                # however, I've found revisions where the id is not present but the numeric-id is
                # therefore, I normalize and keep only 'id' or generate it from numeric-id
                if not 'id' in prop_val['datavalue']['value']:
                    prop_val['datavalue']['value']['id'] = f'Q{prop_val['datavalue']['value']['numeric-id']}'
                
                # remove numeric-id, only keep id
                prop_val['datavalue']['value'].pop("numeric-id", None)

            return hashlib.sha1(json.dumps(prop_val['datavalue'], separators=(',', ':')).encode('utf-8')).hexdigest()

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
            for ref in refs: # refs is a list of { 'hash': '', 'snaks': {}}
                for pid, vals in ref['snaks'].items(): # snaks contains P-id: [{}] -> list of value
                    for prop_val in vals:
                        # NOTE: don't use the hash provided by WD since it's not stable
                        # the same value appeared with != hashes and that implies a create/delete even though there
                        # was no change
                        value_hash = PageParser.generate_value_hash(prop_val) 
                        prop_val['hash'] = value_hash # update hash 
                        hash_map[(pid, value_hash)] = prop_val # need to keep the p-id in case the value repeats for different pids (same value, same hash)
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

        if self.revision_meta['revision_id'] == 157416061:
            print('DEBUG REVISION 157416061')
            print('DELETED:', deleted)
            print('CREATED:', created)
            print('PREV HASH MAP: ', prev_hash_map)
            print('CURR HASH MAP: ', curr_hash_map)
            print('PREVIOUS STMT REFS: ', prev_refs)
            print('CURRENT STMT REFS', curr_refs)

        # deletions
        for pid, value_hash in deleted:
            change_detected = True
            prop_value = prev_hash_map[(pid, value_hash)]

            if prop_value['snaktype'] in (NO_VALUE, SOME_VALUE):
                prev_val, prev_dtype, old_datatype_metadata = (prop_value['snaktype'], 'string', None)
            else:
                dv = prop_value['datavalue']
                prev_val, prev_dtype, old_datatype_metadata = PageParser.parse_datavalue_json(dv['value'], dv['type'])

            self.save_reference_qualifier_changes(
                property_id=id_to_int(stmt_pid),
                value_id=stmt_value_id,
                rq_property_id=id_to_int(pid),
                value_hash=value_hash,
                old_value=prev_val,
                new_value=None,
                datatype=prev_dtype,
                change_target='',
                change_type=DELETE_REFERENCE
            )

            if old_datatype_metadata:
   
                self._handle_datatype_metadata_changes(
                    old_datatype_metadata=old_datatype_metadata,
                    new_datatype_metadata=None,
                    value_id=stmt_value_id,
                    old_datatype=prev_dtype,
                    new_datatype=None,
                    property_id=stmt_pid,
                    change_type=DELETE_REFERENCE,
                    type_='reference_qualifier',
                    rq_property_id=pid,
                    value_hash=value_hash
                )

        # creations
        for pid, value_hash in created:
            change_detected = True
            prop_value = curr_hash_map[(pid, value_hash)]

            if prop_value['snaktype'] in (NO_VALUE, SOME_VALUE):
                curr_val, curr_dtype, new_datatype_metadata = (prop_value['snaktype'], 'string', None)
            else:
                dv = prop_value['datavalue']
                curr_val, curr_dtype, new_datatype_metadata = PageParser.parse_datavalue_json(dv['value'], dv['type'])

            self.save_reference_qualifier_changes(
                property_id=id_to_int(stmt_pid),
                value_id=stmt_value_id,
                rq_property_id=id_to_int(pid),
                value_hash=value_hash,
                old_value=None,
                new_value=curr_val,
                datatype=curr_dtype,
                change_target='',
                change_type=CREATE_REFERENCE
            )

            if new_datatype_metadata:
           
                self._handle_datatype_metadata_changes(
                    old_datatype_metadata=None,
                    new_datatype_metadata=new_datatype_metadata,
                    value_id=stmt_value_id,
                    old_datatype=None,
                    new_datatype=curr_dtype,
                    property_id=stmt_pid,
                    change_type=CREATE_REFERENCE,
                    type_='reference_qualifier',
                    rq_property_id=pid,
                    value_hash=value_hash
                )
        
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

            unchanged = prev_hashes & curr_hashes
            deleted = prev_hashes - unchanged
            added = curr_hashes - unchanged

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

                value_hash = prev_stmt['hash']
                
                self.save_reference_qualifier_changes(
                    property_id=id_to_int(stmt_pid),
                    value_id=stmt_value_id,
                    rq_property_id=id_to_int(pid),
                    value_hash=value_hash,
                    old_value=prev_val,
                    new_value=None,
                    datatype=prev_dtype,
                    change_target='',
                    change_type=DELETE_QUALIFIER
                )

                if old_datatype_metadata:
                    self._handle_datatype_metadata_changes(
                        old_datatype_metadata=old_datatype_metadata, 
                        new_datatype_metadata=None, 
                        value_id=stmt_value_id, 
                        old_datatype=prev_dtype, 
                        new_datatype=None, 
                        property_id=stmt_pid, 
                        change_type=DELETE_QUALIFIER, 
                        type_='reference_qualifier', 
                        rq_property_id=pid, 
                        value_hash=value_hash
                    )

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

                value_hash = curr_stmt['hash']

                self.save_reference_qualifier_changes(
                    property_id=id_to_int(stmt_pid),
                    value_id=stmt_value_id,
                    rq_property_id=id_to_int(pid),
                    value_hash=value_hash,
                    old_value=None,
                    new_value=curr_val,
                    datatype=curr_dtype,
                    change_target='',
                    change_type=CREATE_QUALIFIER
                )

                if new_datatype_metadata:
                    self._handle_datatype_metadata_changes(
                        old_datatype_metadata=None, 
                        new_datatype_metadata=new_datatype_metadata, 
                        value_id=stmt_value_id, 
                        old_datatype=None, 
                        new_datatype=curr_dtype, 
                        property_id=stmt_pid, 
                        change_type=CREATE_QUALIFIER, 
                        type_='reference_qualifier', 
                        rq_property_id=pid, 
                        value_hash=value_hash
                    )

        return change_detected
            
    def _changes_created_entity(self, revision):

        # Process claims
        claims = PageParser._safe_get_nested(revision, 'claims')
        
        for property_id, property_stmts in claims.items():
            for stmt in property_stmts:
                
                value, datatype, datatype_metadata = PageParser._parse_datavalue(stmt)
                new_hash = PageParser._get_property_mainsnak(stmt, 'hash') if stmt else None
                value_id = stmt.get('id', None)
                
                old_value = None
                new_value = value
                
                self.save_changes(
                    id_to_int(property_id), 
                    value_id=value_id,
                    old_value=old_value,
                    new_value=new_value,
                    datatype=datatype,
                    change_target=None,
                    change_type=CREATE_ENTITY,
                    old_hash=None,
                    new_hash=new_hash
                )

                if datatype_metadata:
                    for k, v in datatype_metadata.items():
                        old_value = None
                        new_value = v
                        
                        self.save_changes(
                            id_to_int(property_id),
                            value_id=value_id,
                            old_value=old_value,
                            new_value=new_value,
                            datatype=datatype,
                            change_target=k,
                            change_type=CREATE_ENTITY,
                            old_hash=None,
                            new_hash=new_hash
                        )

                # qualifier changes
                _ = self._handle_qualifier_changes(pid, value_id, prev_stmt=None, curr_stmt=stmt)

                # references changes
                _ = self._handle_reference_changes(pid, value_id, prev_stmt=None, curr_stmt=stmt)

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
                    datatype='string',
                    change_target=None,
                    change_type=CREATE_ENTITY,
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
                datatype='string',
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
                datatype='string',
                change_target=None,
                change_type=PageParser._description_label_change_type(prev_desc, curr_desc),
                change_magnitude=PageParser.magnitude_of_change(old_value, new_value, 'string'),
                old_hash='',
                new_hash=''
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
                value_id = s.get('id', None)

                old_hash = None
                new_hash = PageParser._get_property_mainsnak(s, 'hash') if s else None

                self._handle_value_changes(new_datatype, new_value, None, value_id, new_pid, CREATE_PROPERTY, old_hash, new_hash)

                if new_datatype_metadata:
                    self._handle_datatype_metadata_changes(None, new_datatype_metadata, value_id, None, new_datatype, new_pid, CREATE_PROPERTY, old_hash, new_hash)

                # rank
                curr_rank = s.get('rank') if s else None
                self.save_changes(
                    property_id=id_to_int(new_pid),
                    value_id=value_id,
                    old_value=None,
                    new_value=curr_rank,
                    datatype=new_datatype,
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

                new_hash = None
                old_hash = PageParser._get_property_mainsnak(s, 'hash') if s else None

                self._handle_value_changes(None, None, old_value, value_id, removed_pid, DELETE_PROPERTY, old_hash, new_hash)

                if old_datatype_metadata:
                    self._handle_datatype_metadata_changes(old_datatype_metadata, {}, value_id, old_datatype, None, removed_pid, DELETE_PROPERTY, old_hash, new_hash)

                # rank
                prev_rank = s.get('rank') if s else None
                self.save_changes(
                    property_id=id_to_int(removed_pid),
                    value_id=value_id,
                    old_value=prev_rank,
                    new_value=None,
                    datatype=None,
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

        change_detected = False
        if not prev_stmt:
            change_detected = True
            self.save_changes(
                property_id=id_to_int(pid),
                value_id=sid,
                old_value=None,
                new_value=curr_rank,
                datatype=new_datatype,
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
                datatype=None,
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
                datatype=new_datatype,
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
                prev_stmt = prev_by_id.get(sid)
                curr_stmt = curr_by_id.get(sid)

                new_value, new_datatype, new_datatype_metadata = PageParser._parse_datavalue(curr_stmt)
                old_value, old_datatype, old_datatype_metadata = PageParser._parse_datavalue(prev_stmt)

                old_hash = PageParser.generate_value_hash(prev_stmt)
                new_hash = PageParser.generate_value_hash(curr_stmt)

                # old_hash = PageParser._get_property_mainsnak(prev_stmt, 'hash') if prev_stmt else None
                # new_hash = PageParser._get_property_mainsnak(curr_stmt, 'hash') if curr_stmt else None

                # value changes
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
                        
                        if new_datatype == 'time':
                            # don't consider changes like +00002025-10-01T:00:00:00Z to +2025-10-01T:00:00:00Z
                            # this is internal to WD representation
                            old_value_cleaned = re.sub(r'^([+-])0+(?=\d{4}-)', r'\1', old_value)
                            new_value_cleaned = re.sub(r'^([+-])0+(?=\d{4}-)', r'\1', new_value)
                            if old_value_cleaned != new_value_cleaned:
                                self._handle_value_changes(new_datatype, new_value_cleaned, old_value_cleaned, sid, pid, UPDATE_PROPERTY_VALUE, old_hash, new_hash, change_magnitude=change_magnitude)
                        else:
                            self._handle_value_changes(new_datatype, new_value, old_value, sid, pid, UPDATE_PROPERTY_VALUE, old_hash, new_hash, change_magnitude=change_magnitude)

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
                print(f'The revision {self.revision_meta['revision_id']} of entity {self.revision_meta['entity_id']} is a redirect')
                return True

            if not curr_claims and not curr_label and not curr_desc:
                # skip revision 
                # Reasons: can be an initial reivsion that only has sitelinks/aliases
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
        entity_label = ''

        previous_revision = None

        last_non_deleted_revision_id = -1
        prev_revision_deleted = False

        # Extract title = entity_id
        title_elem = self.page_elem.find(title_tag)
        if title_elem is not None:
            entity_id = (title_elem.text or '').strip()

        entity_id = id_to_int(entity_id) # convert Q-ID to integer (remove the 'Q')

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

                    # Save revision metadata (what will be stored in the revision table)
                    self.revision_meta = {
                        'entity_id': entity_id,
                        'entity_label': entity_label,
                        'revision_id': revision_id,
                        'prev_revision_id': prev_revision_id if prev_revision_id else '-1', # for the first revision (doesn't have a parentid)
                        'timestamp': rev_elem.findtext(f'{{{NS}}}timestamp', '').strip(),
                        'comment': rev_elem.findtext(f'{{{NS}}}comment', '').strip(),
                        'username': username,
                        'user_id': user_id,
                        'file_path': self.file_path
                    }

                    # decode content inside <text></text>
                    revision_text = (revision_text_elem.text).strip()
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
                        
                        # Because revisions that modify aliases/sitelinks are not stored. Therefore, we store the 
                        # prev_revision_id as the last non deleted revision id that we actually stored in the DB.
                        if last_non_deleted_revision_id != self.revision_meta['prev_revision_id']:
                            prev_rev_id = last_non_deleted_revision_id
                        else:
                            prev_rev_id = self.revision_meta['prev_revision_id']

                        self.revision.append((
                            prev_rev_id,
                            self.revision_meta['revision_id'],
                            self.revision_meta['entity_id'],
                            self.revision_meta['entity_label'],
                            self.revision_meta['timestamp'],
                            self.revision_meta['user_id'],
                            self.revision_meta['username'],
                            self.revision_meta['comment'],
                            self.revision_meta['file_path'],
                            self.current_revision_redirect
                        ))

                        if self.current_revision_redirect:
                            self.current_revision_redirect = False

                        # for revisions that have been deleted
                        # we store prev_revision_id as the last non deleted revision
                        # NOTE: if there are no changes, we don't store revision information, therefore we 
                        # have to update this here
                        last_non_deleted_revision_id = revision_id

                    # if parse_revisions_text returns None then
                    # we only update previous_revision with an actual revision (that has a json in the revision <text></text>)
                    if current_revision is not None:
                        previous_revision = current_revision
                    
                    # Batch insert (changes >= revision because one revision can have multiple changes)
                    
                    if len(self.changes) >= self.batch_size:
                        self.db_executor.submit(batch_insert, self.conn, self.revision, self.changes, self.changes_metadata, self.reference_qualifier_changes)
                        # remove already stored changes + revisions to avoid duplicates
                        self.changes = []
                        self.revision = []
                        self.changes_metadata = []
                        self.reference_qualifier_changes = []
                else: # revision was deleted
                    prev_revision_deleted = True

            # free memory
            rev_elem.clear()
        
        # Insert remaining changes + revision + changes_metadata in case the batch size was not reached
        if self.changes:
            batch_insert(self.conn, self.revision, self.changes, self.changes_metadata, self.reference_qualifier_changes)
            self.changes = []
            self.revision = []
            self.changes_metadata = []
            self.reference_qualifier_changes = []

        # Update entity label with last existing label
        update_entity_label(self.conn, entity_id, entity_label)

        # Clear element to free memory
        self.page_elem.clear()
        while self.page_elem.getprevious() is not None:
            del self.page_elem.getparent()[0]