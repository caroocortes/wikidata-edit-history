from xml.sax.handler import ContentHandler
import html
import json
import pandas as pd
import time

from scripts.const import *
from scripts.utils import initialize_csv_files

class PageParser(ContentHandler):
    def __init__(self):
        # TODO: remove this since it will be save in a DB
        _, self.change_file_path, self.revision_file_path = initialize_csv_files()
        self.changes = []
        self.revision = []
        self.set_initial_state()    

    def set_initial_state(self):
        self.entity_id = ''
        self.entity_label = ''
        
        self.in_revision = False             # True if inside a <revision> block
        self.in_revision_id = False          # True if inside the <id> of a revision
        self.in_timestamp = False            # True if inside the <timestamp> tag of a revision
        self.in_comment = False              # True if inside the <comment> tag of a revision
        self.in_revision_text = False        # True if inside the <text> of a revision

        self.in_contributor = False          # True if inside a <contributor> block
        self.in_contributor_id = False       # True if inside the contributor's <id>
        self.in_contributor_username = False # True if inside the contributor's <username>

        self.previous_revision = None
        self.current_revision = None

        # TODO: remove, just for debugging/printing
        self.start_time_entity = 0
        self.end_time_entity = 0

        self.revision_meta = {}
        self.revision_text = ""

        self.revisions_without_changes = 0
        self.num_revisions = 0

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
            raise e
    
    @staticmethod
    def _get_property_mainsnak(stmt, property_=None):
        """
            Returns the value for a property in the mainsnak
        """
        try:
            prop_value = stmt["mainsnak"].get(property_, None)
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
                # complex datatypes - time, quantity, globecoordinate
                # we consider entity as a simple type
                if datatype == 'globecoordinate':
                    value = {
                        "longitude": value_json['longitude'],
                        "latitude": value_json['latitude']
                    }
                if datatype != 'wikibase-entityid':
                    for k, v in value_json.items():
                        if k not in ("time", "amount", "latitude", "longitude", "altitude", "before", "after"): # altitude (DEPRECATED), before and after (UNUSED)
                            datatype_metadata[k] = v
                        else:
                            if datatype != 'globecoordinate' and k not in ("altitude", "before", "after"):
                                value = v
                else:
                    if 'id' in value_json:
                        value = value_json.get('id')
                    else:
                        value = 'Q' + value_json.get('numeric-id')
            else:
                value = value_json

            return value, datatype, datatype_metadata

        else:
            value = NO_VALUE if snaktype == 'novalue' else SOME_VALUE
            return value, None, None
    
    @staticmethod
    def _jsonify_value(val):
        """
            Converts val to a valid JSON string.
            Used to cast new_value and old_value
        """
        # None, bool, int, float are already valid JSON types
        if val is None or isinstance(val, (bool, int, float, dict, list)):
            return val
        # everything else -> dump as JSON string
        return json.dumps(str(val))


    def change_json(self, property_id, value_id, old_value, new_value, datatype, datatype_metadata, change_type):
        return {
            "entity_id": self.entity_id,
            "revision_id": self.revision_meta['revision_id'],
            "property_id": property_id,
            "value_id": value_id,
            "old_value": PageParser._jsonify_value(old_value),
            "new_value": PageParser._jsonify_value(new_value),
            "datatype": datatype,
            "datatype_metadata": datatype_metadata,
            "change_type": change_type
        }

    def _handle_datatype_metadata_changes(self, old_datatype_metadata, new_datatype_metadata, datavalue_id, old_datatype, new_datatype, property_id, change_type):
        
        changes = []
        
        if old_datatype == new_datatype:
        
            for key in set((old_datatype_metadata or {}).keys()):
                old_meta = (old_datatype_metadata or {}).get(key, None)
                new_meta = (new_datatype_metadata or {}).get(key, None)

                if old_meta != new_meta: # save only what changed
                    changes.append(self.change_json(
                        property_id,
                        value_id=datavalue_id,
                        old_value=old_meta,
                        new_value=new_meta,
                        datatype=new_datatype,
                        datatype_metadata=key,
                        change_type=change_type
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
                    datatype_metadata=key if not big_old else None, # We only want datatype_metadata != None for the new_datatype
                    change_type=change_type
                ))

        return changes
    
    def _handle_value_changes(self, new_datatype, new_value, old_value, datavalue_id, property_id, change_type):
        """
            Helper function to store value changes.
        """
        return self.change_json(
                property_id, 
                value_id=datavalue_id,
                old_value=old_value,
                new_value=new_value,
                datatype=new_datatype,
                datatype_metadata=None,
                change_type=change_type
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
            
            curr_label = PageParser._safe_get_nested(current_revision, 'labels', 'en', 'value')
            curr_desc = PageParser._safe_get_nested(current_revision, 'descriptions', 'en', 'value')
            curr_claims = PageParser._safe_get_nested(current_revision, 'claims')

            if not curr_claims and not curr_label and not curr_desc:
                # Entity was deleted
                return self._changes_deleted_created_entity(current_revision, DELETE_ENTITY)

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

    def startElement(self, name, attrs):
        """
        Called when the parser finds a starting tag (e.g. <page>)
        """
        if name == 'title':
            self.entity_id = ''
            self.in_title = True

        if name == 'revision':
            self.in_revision = True

        if self.in_revision:
            if name == 'id' and not self.in_contributor:
                self.in_revision_id = True
            elif name == 'timestamp':
                self.in_timestamp = True
            elif name == 'comment':
                self.in_comment = True
            elif name == 'contributor':
                self.in_contributor = True
            elif name == 'text':
                self.in_revision_text = True

        if self.in_contributor:
            if name == 'id':
                self.in_contributor_id = True
            elif name == 'username':
                self.in_contributor_username = True

    def characters(self, content):
        """ 
            Called when parser finds text inside tags (e.g. <title>Q12</title>)
        """
        
        if self.in_revision:

            if 'entity_id' not in self.revision_meta:
                self.revision_meta['entity_id'] = self.entity_id

            if self.in_revision_id:
                self.revision_meta['revision_id'] = content
            
            if self.in_comment:
                if not 'comment' in self.revision_meta:
                    self.revision_meta['comment'] = ''
                self.revision_meta['comment'] += content
            
            if self.in_timestamp:
                self.revision_meta['timestamp'] = content
            
            if self.in_contributor_id or self.in_contributor_username:
                if 'user' not in self.revision_meta:
                    self.revision_meta.setdefault('user', '')
                    self.revision_meta['user'] = content
                else:
                    self.revision_meta['user'] += ' - ' + content

            if self.in_revision_text:
                self.revision_text += content

        if self.in_title:
            self.start_time_entity = time.time() # TODO: remove
            self.entity_id += content
        
    def endElement(self, name):
        """ 
            Called when a tag ends (e.g. </page>)
        """
        
        if self.in_title: # at </title> 
            self.in_title = False

        if name == 'revision': # at </revision> of revision and we keep the entity

            # Transform revision text to json
            current_revision = self._parse_json_revision(self.revision_text)

            # Revision text exists but is empty -> entity was deleted in this revision
            if not current_revision:
                # NOTE: Not sure if this ever happens
                print(f'Revision text exists but is empty -> Entity {self.entity_id} was deleted in revision: {self.revision_meta["revision_id"]}')

                change = self._changes_deleted_created_entity(self.previous_revision, DELETE_ENTITY)
                if change:
                    current_revision = None
            else:
       
                curr_label = PageParser._get_english_label(current_revision)
                if curr_label and self.entity_label != curr_label: 
                    # Keep most current label
                    self.entity_label = curr_label

                # Extract changes from revision
                change = self.get_changes_from_revisions(
                            current_revision, 
                            self.previous_revision
                        )

            if change:
                # Save changes to Change
                self.changes.extend(change)

                # Save revision metadata to Revision
                self.revision.append(self.revision_meta)
            else:
                self.revisions_without_changes += 1

            self.previous_revision = current_revision
            self.num_revisions += 1
            
            self.in_revision = False
            self.revision_meta = {}
            self.revision_text = ''

        if self.in_revision:
            if name == 'id': # at </id>
                self.in_revision_id = False
            elif name == 'timestamp': # at </timestamp>
                self.in_timestamp = False
            elif name == 'comment': # at </comment>
                self.in_comment = False
            elif name == 'contributor': # at </contributor>
                self.in_contributor = False
            elif name == 'text': # at </text>
                self.in_revision_text = False

        if self.in_contributor:
            if name == 'id': # at </id> inside of <contributor></contributor>
                self.in_contributor_id = False
            elif name == 'username': # at </username> inside of <contributor></contributor>
                self.in_contributor_username = False

        if name == 'page': # at </page>
        
            self.end_time_entity = time.time()
            print(f'Time it took to process entity {self.entity_id} with {self.num_revisions} revisions: {self.end_time_entity - self.start_time_entity} seconds')
            print(f'Number of revisions without changes: {self.revisions_without_changes}')

            # # TODO: change to save in DB
            # df_changes = pd.DataFrame(self.changes)
            # df_changes.to_csv(self.change_file_path, mode='a', index=False, header=False)

            # df_revisions = pd.DataFrame(self.revision)
            # df_revisions.to_csv(self.revision_file_path, mode='a', index=False, header=False)
