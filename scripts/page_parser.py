import html
import json
import time
import Levenshtein
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import os
import sys
from dotenv import load_dotenv
from pathlib import Path
from lxml import etree

from scripts.utils import haversine_metric, get_time_dict, gregorian_to_julian, insert_rows, update_entity_label
from scripts.const import *

def batch_insert(conn, revision, changes):
    # NOTE: copy may be faster
    """Function to insert into DB asynchronously."""
    
    try:
        insert_rows(conn, 'revision', revision, ['revision_id', 'entity_id', 'timestamp', 'user_id', 'username', 'comment'])
        insert_rows(conn, 'change', changes, ['revision_id', 'entity_id', 'property_id', 'value_id', 'old_value', 'new_value', 'datatype', 'datatype_metadata', 'change_type', 'change_magnitude'])
    except Exception as e:
        print(f'There was an error when batch inserting revisions and changes: {e}')
        sys.stdout.flush()

class PageParser():
    def __init__(self, file_path, page_elem_str):
        self.changes = []
        self.revision = []
        self.entity_id = ''
        self.entity_label = ''

        self.revision_meta = {}
        self.revision_text = ""
        self.previous_revision = None

        self.file_path = file_path
        self.page_elem = etree.fromstring(page_elem_str)

        self.db_executor = ThreadPoolExecutor(max_workers=2)  # for DB inserts

        dotenv_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(dotenv_path)

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

    def _parse_json_revision(self, revision_text):
        """
            Returns the text of a revision as a json
        """
        json_text = html.unescape(revision_text.strip())
        try:
            current_revision = json.loads(json_text)
            return current_revision
        except json.JSONDecodeError as e:
            print(f'Error decoding JSON in revision {self.revision_meta['revision_id']} for entity {self.entity_id}: {e}. Revision skipped.')
    
    @staticmethod
    def magnitude_of_change(old_value, new_value, datatype, metadata=False):
        
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

                return float(new_julian - old_julian)
            
            # Calculate distande in km between 2 points
            if datatype == 'globecoordinate' and isinstance(old_value, dict) and isinstance(new_value, dict):
                lat1, lon1 = float(old_value['latitude']), float(old_value['longitude'])
                lat2, lon2 = float(new_value['latitude']), float(new_value['longitude'])
                return float(haversine_metric(lon1, lat1, lon2, lat2))
            
            if datatype == 'string' or datatype == 'monolingualtext': # for entities doesn't make sense to compare ids
                return float(Levenshtein.distance(old_value, new_value))
        elif new_value is not None and old_value is not None and metadata:
            # Calculate magnitude of change for datatype metadata
            # the values will be:
            # - monolingual text: language
            # - globecoordinate: precision
            # - quantity: lowerBound and upperBound
            # - time: timezone and precision
            if datatype != 'monolingualtext':
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
    
    @staticmethod
    def _get_english_label(revision):
        label = PageParser._safe_get_nested(revision, 'labels', 'en', 'value') 
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
            return value, None, None

    def change_json(self, property_id, value_id, old_value, new_value, datatype, datatype_metadata, change_type, change_magnitude=None):
        
        old_value = json.dumps(str(old_value)) if old_value else None
        new_value = json.dumps(str(new_value)) if new_value else None
        return (
            self.revision_meta['revision_id'] if self.revision_meta['revision_id'] else '',
            self.entity_id if self.entity_id else '',
            property_id if property_id else '',
            value_id if value_id else '',
            old_value,
            new_value,
            datatype,
            datatype_metadata if datatype_metadata else '', # can't be None since datatype_metadata is part of the key of the table
            change_type,
            change_magnitude
        )

    def _handle_datatype_metadata_changes(self, old_datatype_metadata, new_datatype_metadata, datavalue_id, old_datatype, new_datatype, property_id, change_type):
        
        changes = []
        
        if old_datatype == new_datatype:
        
            for key in set((old_datatype_metadata or {}).keys()):
                old_meta = (old_datatype_metadata or {}).get(key, None)
                new_meta = (new_datatype_metadata or {}).get(key, None)

                if key not in ('calendarmodel', 'globe', 'unit'): # this metadata stores an entity link so we don't calculate the magnitude of change
                    change_magnitude = PageParser.magnitude_of_change(old_meta, new_meta, new_datatype, metadata=True)
                else: 
                    change_magnitude = None

                if old_meta != new_meta: # save only what changed
                    changes.append(self.change_json(
                        property_id,
                        value_id=datavalue_id,
                        old_value=old_meta,
                        new_value=new_meta,
                        datatype=new_datatype,
                        datatype_metadata=key,
                        change_type=change_type, 
                        change_magnitude=change_magnitude
                    ))

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
                
                changes.append(self.change_json(
                    property_id,
                    value_id=datavalue_id,
                    old_value=old_meta,
                    new_value=new_meta,
                    datatype=new_datatype,
                    datatype_metadata=key,
                    change_type=change_type
                ))
            
            remaining_keys = big_set - keys_to_skip
            for key in remaining_keys:
                
                if big_old:
                    old_meta = (old_datatype_metadata or {}).get(key, None)
                    new_meta = None
                else:
                    new_meta = (new_datatype_metadata or {}).get(key, None)
                    old_meta = None
                
                changes.append(self.change_json(
                    property_id,
                    value_id=datavalue_id,
                    old_value=old_meta,
                    new_value=new_meta,
                    datatype=new_datatype,
                    datatype_metadata=key,
                    change_type=change_type
                ))

        return changes
    
    def _handle_value_changes(self, new_datatype, new_value, old_value, datavalue_id, property_id, change_type, change_magnitude=None):

        return self.change_json(
                property_id, 
                value_id=datavalue_id,
                old_value=old_value,
                new_value=new_value,
                datatype=new_datatype,
                datatype_metadata=None,
                change_type=change_type,
                change_magnitude=change_magnitude
            )
        
    
    def _changes_deleted_created_entity(self, revision, change_type):
        changes = []

        # Process claims
        claims = PageParser._safe_get_nested(revision, 'claims')
        
        for property_id, property_stmts in claims.items():
            for stmt in property_stmts:
                
                
                value, datatype, datatype_metadata = PageParser._parse_datavalue(stmt)
                datavalue_id = stmt.get('id', None)
                
                old_value = None if change_type == CREATE_ENTITY else value
                new_value = value if change_type == CREATE_ENTITY else None
                
                changes.append(
                    self.change_json(
                        property_id, 
                        value_id=datavalue_id,
                        old_value=old_value,
                        new_value=new_value,
                        datatype=datatype,
                        datatype_metadata=None,
                        change_type=change_type
                    )
                )

                if datatype_metadata:
                    for k, v in datatype_metadata.items():
                        old_value = None if change_type == CREATE_ENTITY else v
                        new_value = v if change_type == CREATE_ENTITY else None
                        
                        changes.append(
                            self.change_json(
                                property_id,
                                value_id=datavalue_id,
                                old_value=old_value,
                                new_value=new_value,
                                datatype=datatype,
                                datatype_metadata=k,
                                change_type=change_type
                            )
                        )

        # If there's no description or label, the revisions shows them as []
        labels = PageParser._safe_get_nested(revision, 'labels', 'en', 'value')
        descriptions = PageParser._safe_get_nested(revision, 'descriptions', 'en', 'value')

        # Process labels and descriptions (non-claim properties)
        for pid, val in [('label', labels), ('description', descriptions)]:
            if val:
                old_value = None if change_type == CREATE_ENTITY else val
                new_value = val if change_type == CREATE_ENTITY else None

                changes.append(
                    self.change_json(
                        pid, 
                        value_id=pid,
                        old_value=old_value if not isinstance(old_value, dict) else None,
                        new_value=new_value if not isinstance(new_value, dict) else None,
                        datatype='string',
                        datatype_metadata=None,
                        change_type=change_type
                    )
                )
        
        return changes
    
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
     

    def _handle_description_label_change(self, previous_revision, current_revision):
        
        changes = []
        # --- Label change ---
        prev_label = None
        if previous_revision:
            prev_label = PageParser._safe_get_nested(previous_revision, 'labels', 'en', 'value')
        curr_label = PageParser._safe_get_nested(current_revision, 'labels', 'en', 'value')
        
        if curr_label != prev_label:
            changes.append(
                self.change_json(
                    property_id="label",
                    value_id='label',
                    old_value=prev_label if not isinstance(prev_label, dict) else None,
                    new_value=curr_label if not isinstance(curr_label, dict) else None,
                    datatype='string',
                    datatype_metadata=None,
                    change_type=PageParser._description_label_change_type(prev_label, curr_label)
                )
            )

        # --- Description change ---
        prev_desc = None
        if previous_revision:
            prev_desc = PageParser._safe_get_nested(previous_revision, 'descriptions', 'en', 'value')
        curr_desc = PageParser._safe_get_nested(current_revision, 'descriptions', 'en', 'value')

        if curr_desc != prev_desc:
            changes.append(
                self.change_json(
                    property_id="description",
                    value_id='description',
                    old_value=prev_desc if not isinstance(prev_desc, dict) else None,
                    new_value=curr_desc if not isinstance(curr_desc, dict) else None,
                    datatype='string',
                    datatype_metadata=None,
                    change_type=PageParser._description_label_change_type(prev_desc, curr_desc)
                )
            )

        return changes
    
    def _handle_new_pids(self, new_pids, curr_claims):
        changes = []
        for new_pid in new_pids:
            curr_statements = curr_claims.get(new_pid, [])
            for s in curr_statements:
                new_value, new_datatype, new_datatype_metadata = PageParser._parse_datavalue(s)
                datavalue_id = s.get('id', None)

                changes.append(self._handle_value_changes(new_datatype, new_value, None, datavalue_id, new_pid, CREATE_PROPERTY))

                if new_datatype_metadata:
                    changes.extend(self._handle_datatype_metadata_changes(None, new_datatype_metadata, datavalue_id, None, new_datatype, new_pid, CREATE_PROPERTY))

        return changes
    
    def _handle_removed_pids(self, removed_pids, prev_claims):
        changes = []
        for removed_pid in removed_pids:
            prev_statements = prev_claims.get(removed_pid, [])

            for s in prev_statements:
                old_value, old_datatype, old_datatype_metadata = PageParser._parse_datavalue(s)

                datavalue_id = s.get('id', None)

                changes.append(self._handle_value_changes(None, None, old_value, datavalue_id, removed_pid, DELETE_PROPERTY))

                if old_datatype_metadata:

                    changes.extend(self._handle_datatype_metadata_changes(old_datatype_metadata, {}, datavalue_id, old_datatype, None, removed_pid, DELETE_PROPERTY))

        return changes

    def _handle_remaining_pids(self, remaining_pids, prev_claims, curr_claims):
        changes = []
        for pid in remaining_pids:
            # Get statement for the same P-ID in previous and current revision
            prev_statements = prev_claims.get(pid, []) 
            curr_statements = curr_claims.get(pid, [])

            # Map by statement ID
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
                    # Property value was removed -> We set datatype = None
                    changes.append(self._handle_value_changes(None, new_value, old_value, sid, pid, DELETE_PROPERTY_VALUE))

                    if old_datatype_metadata:
                        # Add change record for the datatype_metadata fields
                        changes = self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, old_datatype, None, pid, DELETE_PROPERTY_VALUE)

                elif curr_stmt and not prev_stmt:
                    # Property value was created
                    changes.append(self._handle_value_changes(new_datatype, new_value, old_value, sid, pid, CREATE_PROPERTY_VALUE))

                    if new_datatype_metadata:
                        # Add change record for the datatype_metadata fields
                        changes.extend(self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, None, new_datatype, pid, CREATE_PROPERTY_VALUE))
                    
                elif prev_stmt and curr_stmt and old_hash != new_hash:
                    # Property was updated
                    if (old_datatype != new_datatype) or (old_value != new_value):
                        # Datatype change -> value and metadata change
                        if old_datatype == new_datatype and old_datatype != 'wikibase-entityid':
                            # only value change
                            change_magnitude = PageParser.magnitude_of_change(old_value, new_value, new_datatype)
                            changes.append(self._handle_value_changes(new_datatype, new_value, old_value, sid, pid, UPDATE_PROPERTY_VALUE, change_magnitude=change_magnitude))
                        else:
                            changes.append(self._handle_value_changes(new_datatype, new_value, old_value, sid, pid, UPDATE_PROPERTY_VALUE))
                    
                    if (old_datatype != new_datatype) or (old_datatype_metadata != new_datatype_metadata):
                        # Datatype change -> value and metadata change
                        changes.extend(self._handle_datatype_metadata_changes(old_datatype_metadata, new_datatype_metadata, sid, old_datatype, new_datatype, pid, UPDATE_PROPERTY_DATATYPE_METADATA))

        return changes
    
    def get_changes_from_revisions(self, current_revision, previous_revision):
        if not previous_revision:
            # Entity was created again or for the first time
            return self._changes_deleted_created_entity(current_revision, CREATE_ENTITY)
        else:
            changes = []
            
            curr_label = PageParser._safe_get_nested(current_revision, 'labels')
            curr_desc = PageParser._safe_get_nested(current_revision, 'descriptions')
            curr_claims = PageParser._safe_get_nested(current_revision, 'claims')

            if not curr_claims and not curr_label and not curr_desc:
                # Skipped revision -> could be a deleted revision, not necessarily a deleted entity
                print(f'Revision does not contain labels, descriptions, nor claims. Skipped revision {self.revision_meta['revision_id']} for entity {self.revision_meta['entity_id']}')
                return []

            # --- Labels and Description changes ---
            changes.extend(self._handle_description_label_change(previous_revision, current_revision))

            # --- Claims (P-IDs) ---
            prev_claims = PageParser._safe_get_nested(previous_revision, 'claims')

            prev_claims_pids = set(prev_claims.keys())
            curr_claims_pids = set(curr_claims.keys())
            
            # --- New properties in current revision ---
            new_pids = curr_claims_pids - prev_claims_pids
            if new_pids:
                changes.extend(self._handle_new_pids(new_pids, curr_claims))

            # --- Deleted properties in current revision ---
            removed_pids = prev_claims_pids - curr_claims_pids
            if removed_pids:
                changes.extend(self._handle_removed_pids(removed_pids, prev_claims))

            # --- Check updates of statements between revisions ---
            remaining_pids = prev_claims_pids.intersection(curr_claims_pids)
            if remaining_pids:
                changes.extend(self._handle_remaining_pids(remaining_pids, prev_claims, curr_claims))
            
            return changes

    def process_page(self):
        
        duplicated_entity = False
        timestamps = []

        num_revisions = 0
        revisions_without_changes = 0
        ns = "http://www.mediawiki.org/xml/export-0.11/"

        title_tag = f"{{{ns}}}title"
        revision_tag = f'{{{ns}}}revision'
        revision_text_tag = f'{{{ns}}}text'

        # Extract title / entity_id
        title_elem = self.page_elem.find(title_tag)
        if title_elem is not None:
            self.entity_id = (title_elem.text or '').strip()
            print(self.entity_id)
            # start_time_entity = time.time()
            # Insert entity row
            result = insert_rows(self.conn, 'entity', [(self.entity_id, self.entity_label, self.file_path)],
                        columns=['entity_id', 'entity_label', 'file_path'])
            if result == 0:
                duplicated_entity = True
            else:
                print(f'Inserted entity {self.entity_id}')
    
        # Iterate over revisions
        for rev_elem in self.page_elem.findall(revision_tag):

            if duplicated_entity:
                # Only get timestamps and store them -> allow to check if there are more revisions or if the entity is duplicated
                ts_elem = rev_elem.findtext(f'{{{ns}}}timestamp', '')
                if ts_elem is not None:
                    timestamps.append(ts_elem.text)
            else:
                # Extract text, id, timestamp, comment, username, user_id

                contrib_elem = rev_elem.find(f'{{{ns}}}contributor')
            
                if contrib_elem is not None:
                    username = (contrib_elem.findtext(f'{{{ns}}}username') or '').strip()
                    user_id = (contrib_elem.findtext(f'{{{ns}}}id') or '').strip()
                else:
                    username = ''
                    user_id = ''

                self.revision_meta = {
                    'entity_id': self.entity_id,
                    'revision_id': rev_elem.findtext(f'{{{ns}}}id', '').strip(),
                    'timestamp': rev_elem.findtext(f'{{{ns}}}timestamp', '').strip(),
                    'comment': rev_elem.findtext(f'{{{ns}}}comment', '').strip(),
                    'username': username,
                    'user_id': user_id
                }
                
                # Get revision text
                revision_text = (rev_elem.findtext(revision_text_tag) or '').strip()
                current_revision = self._parse_json_revision(revision_text)
                
                if not current_revision:
                    print(f'Revision text is empty. Revision {self.revision_meta['revision_id']} for entity {self.revision_meta['entity_id']} skipped')
                else:
                    curr_label = self._get_english_label(current_revision)
                    if curr_label and self.entity_label != curr_label and curr_label != '':
                        self.entity_label = curr_label
                    change = self.get_changes_from_revisions(current_revision, self.previous_revision)

                if change:
                    self.changes.extend(change)

                    self.revision.append((
                        self.revision_meta['revision_id'],
                        self.revision_meta['entity_id'],
                        self.revision_meta['timestamp'],
                        self.revision_meta['user_id'],
                        self.revision_meta['username'],
                        self.revision_meta['comment'],
                    ))
                else:
                    revisions_without_changes += 1

                self.previous_revision = current_revision
                num_revisions += 1

                # Batch insert
                if len(self.changes) >= BATCH_SIZE_CHANGES:
                    self.db_executor.submit(batch_insert, self.conn, self.revision, self.changes)
                    self.changes = []
                    self.revision = []

            # free memory
            rev_elem.clear()

        if duplicated_entity:
            if timestamps:
                first_ts = timestamps[0]   # first revision
                last_ts = timestamps[-1]   # last revision
                print(f'Entity {self.entity_id} was already inserted in DB. Skipped')
                with open("duplicate_entities.txt", "a") as f:
                    f.write(f"{self.entity_id}\t{self.entity_label}\t{self.file_path} - First revision: {first_ts}, Last revision: {last_ts} \n")
        else:
            # Insert remaining changes if the BATCH_SIZE was not reached
            if self.changes:
                batch_insert(self.conn, self.revision, self.changes)

            # Update entity label with last label
            update_entity_label(self.conn, self.entity_id, self.entity_label)
            # end_time_entity = time.time()
            # print(f'Finished processing entity (in PageParser.page_parser) {self.entity_id} - {num_revisions} revisions in {end_time_entity - start_time_entity:.2f}s')

        # Clear element to free memory
        self.page_elem.clear()
        while self.page_elem.getprevious() is not None:
            del self.page_elem.getparent()[0]