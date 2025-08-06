import mwxml
import html
import json
import os
import bz2
from concurrent.futures import ProcessPoolExecutor
import shutil
import time
from lxml import etree
import xml.etree.ElementTree as ET
from const import *
import traceback

class DumpParser():

    def __init__(self, logging):
        self.entities = []
        self.entities_processing_times = []
        self.logging = logging
        pass

    def get_property_mainsnak(self, stmt, property_=None):
        try:
            return stmt["mainsnak"].get(property_, None)
        except (KeyError, TypeError):
            return None
        
    def safe_get_nested(d, *keys, default={}):
        """
        Safely access nested dictionary keys, avoiding issues with unexpected list types.
        Wikidata sets claims, labels and descriptions to [] if there's no value

        Example:
            safe_get_nested(revision, 'labels', 'en', 'value')

        Will return {} if any part of the path is invalid or not a dict.
        """
        current = d
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key, default)
            else:
                return default
        return current
    
    def get_english_label(self, revision):
        return self.safe_get_nested(revision, 'labels', 'en', 'value')
        
    def changes_deleted_created_entity(self, revision, revision_meta, change_type):
        changes = []

        # Determine old/new values based on change type
        if change_type == CREATE_ENTITY:
            get_old_new = lambda v: (None, v)
        elif change_type == DELETE_ENTITY:
            get_old_new = lambda v: (v, None)
        
        # If there's no description or label, the revisions shows them as []
        labels = self.safe_get_nested(revision, 'labels', 'en', 'value')
        descriptions = self.safe_get_nested(revision, 'descriptions', 'en', 'value')
        
        # Process claims
        claims = revision.get('claims')
        if isinstance(claims, dict):
            for property_id, property_stmts in claims.items():
                for stmt in property_stmts:
                    old_value, new_value = get_old_new(stmt)
                    changes.append({
                        **revision_meta,
                        "property_id": property_id,
                        "old_value": old_value,
                        "new_value": new_value,
                        "change_type": change_type
                    })

        # Process labels and descriptions (non-claim properties)
        for pid, val in [('label', labels), ('description', descriptions)]:
            if val:
                old_value, new_value = get_old_new(val)
                changes.append({
                    **revision_meta,
                    "property_id": pid,
                    "old_value": old_value,
                    "new_value": new_value,
                    "change_type": change_type
                })
        
        return changes
    
    def change_type(self, old_value, new_value, case, old_hash=None, new_hash=None):
        """ 
            Returns the change type.

            section can have 3 types:
                - CASE_0 : labels and descriptions -> only have 1 value 
                - CASE_1 : new properties in curr_rev
                - CASE_2 : deleted properties in curr_rev
                - CASE_3 : same properties in both revisions
        """

        if case == CASE_0:
            if not old_value and new_value:
                return CREATE_PROPERTY
            elif old_value and not new_value:
                return DELETE_PROPERTY
        
        if case == CASE_1:
            return CREATE_PROPERTY
        
        if case == CASE_2:
            return DELETE_PROPERTY
        
        elif case == CASE_3:
            if old_value and not new_value:
                return DELETE_PROPERTY_VALUE
            elif new_value and not old_value:
                return CREATE_PROPERTY_VALUE
            elif old_value and new_value and old_hash != new_hash:
                return UPDATE_PROPERTY_VALUE
        

    def get_changes_from_revisions(self, entity_id, current_revision, previous_revision, revision_meta):
        
        if not previous_revision:
            # Entity was created again or for the first time
            return self.changes_deleted_created_entity(current_revision, revision_meta, CREATE_ENTITY)

        else:
            changes = []

            # --- Label change ---
            prev_label = None
            if previous_revision:
                prev_label = self.safe_get_nested(previous_revision, 'labels', 'en', 'value')
            curr_label = self.safe_get_nested(current_revision, 'labels', 'en', 'value')

            if curr_label != prev_label:
                changes.append({
                    **revision_meta,
                    "entity_id": entity_id if entity_id else current_revision.get('id'),
                    "property_id": "label",
                    "old_value": prev_label,
                    "new_value": curr_label,
                    "change_type": self.change_type(prev_label, curr_label, CASE_0)
                })

            # --- Description change ---
            prev_desc = None
            if previous_revision:
                prev_desc = previous_revision.get('descriptions', {}).get('en', {}).get('value')
            curr_desc = current_revision.get('descriptions', {}).get('en', {}).get('value')

            if curr_desc != prev_desc:
                changes.append({
                    **revision_meta,
                    "entity_id": entity_id,
                    "property_id": "description",
                    "old_value": prev_desc,
                    "new_value": curr_desc,
                    "change_type": self.change_type(prev_desc, curr_desc, CASE_0)
                })

            # --- Statements (P-IDs) ---
            prev_claims_raw = previous_revision.get('claims') if previous_revision else {}
            curr_claims_raw = current_revision.get('claims')

            # The first revisions' 'claims' is []
            prev_claims = prev_claims_raw if isinstance(prev_claims_raw, dict) else {} 
            curr_claims = curr_claims_raw if isinstance(curr_claims_raw, dict) else {}

            prev_claims_pids = set(prev_claims.keys())
            curr_claims_pids = set(curr_claims.keys())
            
            # --- New statements in current revision ---
            if curr_claims_pids - prev_claims_pids:
                new_pids = curr_claims_pids - prev_claims_pids
                for new_pid in new_pids:
                    curr_statements = curr_claims.get(new_pid, [])
                    for s in curr_statements:
                        new_value = s.get('mainsnak', {}).get('datavalue')
                        # New property : p_id: {new_pid}, value: {new_value}'
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": new_pid,
                            "old_value": None,
                            "new_value": new_value,
                            "change_type": self.change_type(None, new_value, CASE_1)
                        })

            # --- Deleted statements in current revision ---
            if prev_claims_pids - curr_claims_pids:

                removed_pids = prev_claims_pids - curr_claims_pids
                for removed_pid in removed_pids:
                    prev_statements = prev_claims.get(removed_pid, [])
                    for s in prev_statements:
                        old_value = s.get('mainsnak', {}).get('datavalue')
                        # Property removed: p_id: {removed_pid}, value: {old_value}'
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": removed_pid,
                            "old_value": old_value,
                            "new_value": None,
                            "change_type": self.change_type(old_value, None, CASE_2)
                        })

            # --- Check updates of statements between revisions ---
            remaining_pids = prev_claims_pids.intersection(curr_claims_pids)
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

                    new_value = self.get_property_mainsnak(curr_stmt, 'datavalue')
                    old_value = self.get_property_mainsnak(prev_stmt, 'datavalue')

                    if prev_stmt and not curr_stmt:
                        change_type = self.change_type(old_value, None, CASE_3)

                    elif curr_stmt and not prev_stmt:
                        change_type = self.change_type(None, new_value, CASE_3)

                    elif prev_stmt and curr_stmt:
                        change_type = self.change_type(old_value, new_value, CASE_3,
                                                    old_hash=self.get_property_mainsnak(prev_stmt, 'hash') if prev_stmt else None,
                                                    new_hash=self.get_property_mainsnak(curr_stmt, 'hash') if curr_stmt else None)

                    if change_type:
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": pid,
                            "old_value": old_value if change_type != CREATE_PROPERTY_VALUE else None,
                            "new_value": new_value if change_type != DELETE_PROPERTY_VALUE else None,
                            "change_type": change_type
                        })

            return changes

    """ Auxiliary function to extract full snapshot from revision - can be removed later """
    def get_snapshot_from_dump(self, file):
        """Parse a bz2 dump file and extract text from revision."""
        
        parent_dir = os.path.dirname(file)
        snapshot_dir = os.path.join(parent_dir, "snapshots")
        os.makedirs(snapshot_dir, exist_ok=True)

        # Use name of bz2 file as base name
        filename = os.path.basename(file)
        base = filename.replace(".xml", "").replace(".bz2", "")

        with bz2.open(file, mode="rt", encoding="utf-8", errors="ignore") as f:
            dump = mwxml.Dump.from_file(f)
  
            for page in dump:

                print('Processing page:', page.title)
                page_revisions = []
                
                if page.title.startswith("Q"):
                    for revision in page:
                        try:
                            if not revision.text: # No JSON data in revision
                                print(f'No JSON data in revision {revision.id} for page {page.title}')
                                current_revision = {}
                            else:
                                json_text = html.unescape(revision.text)
                                current_revision = json.loads(json_text)
                                to_remove = ['aliases', 'sitelink', 'reference', 'qualifiers']
                                comment = revision.comment or ""
                                if any(word in comment for word in to_remove):
                                    continue # Skip revisions with these comments (updates to aliases, sitelinks, labels, descriptions, references, qualifiers)

                                # Remove labels, descriptions in other languages + sitelinks and aliases
                                if isinstance(current_revision['labels'], dict):
                                    labels = current_revision['labels']
                                    current_revision['labels'] = {k: v for k, v in labels.items() if k == 'en'}

                                if isinstance(current_revision['descriptions'], dict):
                                    descriptions = current_revision['descriptions']
                                    current_revision['descriptions'] = {k: v for k, v in descriptions.items() if k == 'en'}
                                
                                if 'sitelinks' in current_revision:
                                    del current_revision['sitelinks']
                                
                                if 'aliases' in current_revision:
                                    del current_revision['aliases']

                            # --- Revision data ---
                            revision = {
                                "revision_id": revision.id,
                                "entity_id": page.title if page.title is not None else current_revision.get('id'),
                                "timestamp": str(revision.timestamp),
                                "user_id": revision.user.id if revision.user else None,
                                "comment": revision.comment,
                                "snapshot": current_revision
                            }

                            page_revisions.append(revision)

                        except Exception as e:
                            print(f'Error parsing revision: {e}')
                            raise e

                    entity_id = page.title if page.title is not None else current_revision.get('id')           
                    jsonl_output = f"{snapshot_dir}/{entity_id}_snapshot.jsonl" 
                    with open(jsonl_output, 'a', encoding='utf-8') as f_out:
                        for rev in page_revisions:
                            f_out.write(json.dumps(rev, ensure_ascii=False) + '\n')

    def process_revisions(self, page_data):
        """
            Process entity's revisions.

            Args: tuple of (page title, page)
            Returns: list of changes and number of revisions
        """
        page_title, page = page_data
        entity_id = page_title

        previous_revision = None
        number_revisions = 0
        total_changes = []

        # Iterate through each revision in the page using lxml XPath
        # Need to register namespace for XPath queries
        ns_map = {'ns': 'http://www.mediawiki.org/xml/export-0.11/'}

        start = time.time()
        # Find all revision elements with namespace
        revisions = page.xpath('./ns:revision', namespaces=ns_map)
        for rev in revisions:

            try:
                # --- Revision metadata ---
                revision_meta = {
                    "revision_id": rev.findtext('ns:id', namespaces=ns_map),
                    "timestamp": rev.findtext('ns:timestamp', namespaces=ns_map),
                    "user_id": rev.findtext('ns:contributor/ns:id', namespaces=ns_map) or '',
                    'user_name': rev.findtext('ns:contributor/ns:username', namespaces=ns_map) or '',
                    "comment": rev.findtext('ns:comment', namespaces=ns_map),
                    "entity_id": entity_id,
                }

                revision_text = rev.findtext('ns:text', namespaces=ns_map) or ''

                if not revision_text: # No JSON data in current revision
                    if not previous_revision:
                        # Previous revision is None -> iterate until we find a revision with JSON data
                        continue
                    
                    # If previous revision exists, we assume the entity was deleted in this revision
                    total_changes.extend(self.changes_deleted_created_entity(previous_revision, revision_meta, DELETE_ENTITY))
                    current_revision = None
                    print(f'Revision text doesnt exist -> Entity {entity_id} was deleted in revision: {revision_meta["revision_id"]}')

                else:

                    if revision_text.strip():
                        json_text = html.unescape(revision_text)
                        try:
                            current_revision = json.loads(json_text)
                        except json.JSONDecodeError as e:
                            print(f'Error decoding JSON in revision {revision_meta["revision_id"]} for entity {entity_id}: {e}. Revision skipped.')
                            raise e
                    else:
                        current_revision = None

                    # Revision text exists but is empty -> entity was deleted in this revision
                    if previous_revision and not current_revision:
                        print(f'Revision text exists but is empty -> Entity {entity_id} was deleted in revision: {revision_meta["revision_id"]}')
                        total_changes.extend(self.changes_deleted_created_entity(previous_revision, revision_meta, DELETE_ENTITY))
                        current_revision = None # to be consistent with the other case (revision.text == None)
                    else:
                        total_changes.extend(
                            self.get_changes_from_revisions(
                                entity_id, 
                                current_revision, 
                                previous_revision, 
                                revision_meta
                            )
                        )

                number_revisions += 1
                previous_revision = current_revision

            except Exception as e:
                print(f'Error parsing revision: {e}')
                traceback.print_exc()
                raise e
            
            # Clear the element to free memory
            rev.clear()

        end = time.time()
        if previous_revision:
            label = self.get_english_label(previous_revision)
            self.entities.append({"id": entity_id, "label": label})

        self.entities_processing_times.append({
            "entity_id": entity_id,
            "number_revisions": number_revisions,
            "processing_time": end - start,
        })

        return total_changes, number_revisions

    def parse_pages_in_xml(self, file):
        """
            Parse a bz2 dump file and extract changes between revisions.
            Stores the changes in a JSONL file (bz2) and entities in a JSON file.
            
            Args: bz2 file path
            Returns: (number of entities, average number of revisions)
        """

        parent_dir = os.path.dirname(file)
        revision_dir = os.path.join(parent_dir, "revisions")
        print(f'creating revisions dir at: {revision_dir}')
        os.makedirs(revision_dir, exist_ok=True)

        # Use name of bz2 file as base name
        filename = os.path.basename(file)
        base = filename.replace(".xml", "").replace(".bz2", "")
        jsonl_output = f"{revision_dir}/{base}_changes.jsonl" 

        total_num_revisions = 0
        changes_saved = 0

        start_process = time.time()
        with bz2.open(file, mode="rb") as f:
            self.logging.info(f'Time to open file: {time.time() - start_process}')

            start = time.time()
            context = etree.iterparse(f, events=('end',), tag=f'{NS}page')
            self.logging.info(f'Time to generate iterator: {time.time() - start}')
            
            # Iterate over pages in the XML file
            for event, elem in context:
                
                if elem.tag == f'{NS}page': 
                  
                    page_title = elem.find(f'{NS}title')
                    page_title = page_title.text if page_title is not None else ""

                    if page_title and not page_title.startswith("Q"):
                        try:
                            self.logging.info(f'Skipping page {page_title} as it does not start with "Q".')
                            elem.clear()
                            parent = elem.getparent()
                            if parent is not None:
                                while elem.getprevious() is not None:
                                    del parent[0]
                            continue
                        except Exception as e:
                            self.logging.info(f'Error clearing element: {e}')
                            traceback.print_exc()
                    
                    try:
                        print('Processing page:', page_title)
                        changes, number_revisions = self.process_revisions((page_title, elem))
                        print(f'Page {page_title} processed with {number_revisions} revisions and {len(changes)} changes.')
                        total_num_revisions += number_revisions
                    except Exception as e:
                        self.logging.error(f'Error in process_revisions: {e}')
                        traceback.print_exc()
                    
                    try:
                        elem.clear()
                        parent = elem.getparent()
                        if parent is not None:
                            while elem.getprevious() is not None:
                                del parent[0]
                    except Exception as e:
                        self.logging.info(f'Error clearing element: {e}')
                        traceback.print_exc()
                        
                    try:
                        # Save each change as a line to JSONL file
                        # TODO: remove this so it saves everything in same file
                        jsonl_output = f"{revision_dir}/{page_title}_changes.jsonl" 
                        os.makedirs(os.path.dirname(jsonl_output), exist_ok=True)
                        start = time.time()
                        with open(jsonl_output, 'a', encoding='utf-8') as f_out:
                            for change in changes:
                                changes_saved += 1
                                f_out.write(json.dumps(change) + '\n')
                        self.logging.info(f'Time to save changes for entity {page_title}: {time.time() - start} seconds')
                    except Exception as e:
                        self.logging.error('Error writing changes to JSONL file: {e}')
                        traceback.print_exc()

        self.logging.info(f'Time to process all pages in file {filename}: {time.time() - start_process} seconds')
        
        # TODO: uncomment this so it zips change file
        # # Compress the JSONL file
        # bz2_output = jsonl_output + '.bz2'
        # with open(jsonl_output, 'rb') as f_in, bz2.open(bz2_output, 'wb') as f_out:
        #     shutil.copyfileobj(f_in, f_out)
        
        # # Remove the original JSONL file
        # os.remove(jsonl_output)

        log_file = os.path.abspath(f"entities_log_{base}.json")
        print(f"Writing log to: {log_file}")
        try:
            with open(log_file, 'a', encoding='utf-8') as log:
                for ent_pt in self.entities_processing_times:
                    log.write(json.dumps(ent_pt) + '\n')
        except Exception as e:
            self.logging.error(f'Error writing to log file: {e}')

        return self.entities, changes_saved, (total_num_revisions / len(self.entities)) if self.entities else 0
