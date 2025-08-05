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
from const import CREATE_ENTITY, UPDATE_PROPERTY_VALUE, DELETE_ENTITY, CREATE_PROPERTY, DELETE_PROPERTY, CREATE_PROPERTY_VALUE, DELETE_PROPERTY_VALUE, NS

class DumpParser():

    def __init__(self):
        self.entities = []
        self.entities_processing_times = []
        pass
    
    def get_english_label(self, revision):
        return revision.get('labels', {}).get('en', {}).get('value', None)

    def get_property_mainsnak(self, stmt, property_=None):
        try:
            return stmt["mainsnak"].get(property_, None)
        except (KeyError, TypeError):
            return None
        
    def changes_deleted_created_entity(self, previous_revision, revision_meta, change_type):
        changes = []
        claims = previous_revision.get('claims')
        labels = previous_revision.get('labels', {}).get('en', {}).get('value')
        descriptions = previous_revision.get('descriptions', {}).get('en', {}).get('value')

        # Determine old/new values based on change type
        if change_type == CREATE_ENTITY:
            get_old_new = lambda v: (None, v)
        elif change_type == DELETE_ENTITY:
            get_old_new = lambda v: (v, None)

        # Process claims
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
            old_value, new_value = get_old_new(val)
            changes.append({
                **revision_meta,
                "property_id": pid,
                "old_value": old_value,
                "new_value": new_value,
                "change_type": change_type
            })
        return changes

    def get_changes_from_revisions(self, entity_id, current_revision, previous_revision, revision_meta):
        
        if not previous_revision:
            # Entity was created again or for the first time
            return self.changes_deleted_created_entity(current_revision, revision_meta, CREATE_ENTITY)

        else:
            changes = []

            # --- Label change ---
            prev_label = None
            if previous_revision:
                prev_label = previous_revision.get('labels', {}).get('en', {}).get('value')
            curr_label = current_revision.get('labels', {}).get('en', {}).get('value')

            if curr_label != prev_label:
                changes.append({
                    **revision_meta,
                    "entity_id": entity_id if entity_id else current_revision.get('id'),
                    "property_id": "label",
                    "old_value": prev_label,
                    "new_value": curr_label,
                    "change_type": CREATE_PROPERTY if not prev_label else UPDATE_PROPERTY_VALUE
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
                    "change_type": CREATE_PROPERTY if not prev_desc else UPDATE_PROPERTY_VALUE
                })

            # --- Statements (P-IDs) ---
            prev_claims_raw = previous_revision.get('claims') if previous_revision else {}
            curr_claims_raw = current_revision.get('claims')

            prev_claims = prev_claims_raw if isinstance(prev_claims_raw, dict) else {} # The first revisions' 'claims' is []
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
                            "change_type": CREATE_PROPERTY
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
                            "change_type": DELETE_PROPERTY
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

                    if prev_stmt and curr_stmt is None:
                        # 'Statement removed for existing property: p_id: {pid}, value: {old_value}'
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": pid,
                            "old_value": old_value,
                            "new_value": None,
                            "change_type": DELETE_PROPERTY_VALUE
                        })

                    elif curr_stmt and prev_stmt is None:
                        # New value for existing property: p_id: {pid}, value: {new_value}'
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": pid,
                            "old_value": None,
                            "new_value": new_value,
                            "change_type": CREATE_PROPERTY_VALUE
                        })

                    prev_hash = self.get_property_mainsnak(prev_stmt, 'hash') if prev_stmt else None
                    curr_hash = self.get_property_mainsnak(curr_stmt, 'hash') if curr_stmt else None

                    if prev_hash and curr_hash and new_value and old_value and prev_hash != curr_hash:
                        # Statement updated: p_id: {pid}, old value: {old_value}, new value: {new_value}'
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": pid,
                            "old_value": old_value,
                            "new_value": new_value,
                            "change_type": UPDATE_PROPERTY_VALUE
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

                    entity_id = page.title if page.title is not None else current_revision.get('id')           
                    jsonl_output = f"{snapshot_dir}/{entity_id}_snapshot.jsonl" 
                    with open(jsonl_output, 'a', encoding='utf-8') as f_out:
                        for rev in page_revisions:
                            f_out.write(json.dumps(rev, ensure_ascii=False) + '\n')

    def process_revisions_etree(self, page_data):
        """"
            Process entity's revisions.
            
            Args: tuple of (page title, page)
            Returns: list of changes and number of revisions
        """
        page_title, page = page_data
        entity_id = page_title

        previous_revision = None
        number_revisions = 0
        total_changes = []

        # Iterate through each revision in the page
        start = time.time()
        for rev in page.findall(f'{NS}revision'):
            
            try:
                # --- Revision metadata ---
                revision_meta = {
                    "revision_id": rev.findtext(f'{NS}id'),
                    "timestamp": rev.findtext(f'{NS}timestamp'),
                    "user_id": rev.findtext(f'{NS}contributor/id') or '',
                    'user_name': rev.findtext(f'{NS}contributor/username') or '',
                    "comment": rev.findtext(f'{NS}comment'),
                    "entity_id": entity_id,
                }

                revision_text = rev.findtext(f'{NS}text') or ''

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
                            continue
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
            
            rev.clear()

        end = time.time()
        if previous_revision:
            label = self.get_english_label(previous_revision)
            self.entities.append({"id": entity_id, "label": label})
        
        self.entities_processing_times.append({
            "entity_id": entity_id,
            "number_revisions": number_revisions,
            'number_changes': len(total_changes),
            "revisions_processing_time": end - start
        })

        return total_changes, number_revisions
    
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
        ns_map = {'ns': NS}

        start = time.time()
        # Find all revision elements with namespace
        for rev in page.xpath('./ns:revision', namespaces=ns_map):
            
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
                            continue
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

    def parse_pages_in_xml_etree(self, file):
        """
            Parse a bz2 dump file and extract changes between revisions.
            Stores the changes in a JSONL file (bz2) and entities in a JSON file.
            
            Args: bz2 file path
            Returns: (number of entities, average number of revisions)
        """

        parent_dir = os.path.dirname(file)
        revision_dir = os.path.join(parent_dir, "revisions")
        os.makedirs(revision_dir, exist_ok=True)

        # Use name of bz2 file as base name
        filename = os.path.basename(file)
        base = filename.replace(".xml", "").replace(".bz2", "")
        jsonl_output = f"{revision_dir}/{base}_changes.jsonl" 

        total_num_revisions = 0
        changes_saved = 0

        with bz2.open(file, mode="rt", encoding="utf-8", errors="ignore") as f:
            
            context = ET.iterparse(f, events=('end',))
            # Iterate over pages in the XML file
            for event, elem in context:
                if elem.tag == f'{NS}page':
                    page_title = elem.find(f'{NS}title').text

                    if page_title.startswith("Q"):
                        print('Processing page:', page_title)
                        jsonl_output = f"{revision_dir}/{page_title}_changes.jsonl" # TODO: remove this so it saves everything in same file
                        with open(jsonl_output, 'a', encoding='utf-8') as f_out:
                            
                            changes, number_revisions = self.process_revisions((page_title, elem))
                            print(f'Page {page_title} processed with {number_revisions} revisions and {len(changes)} changes.')
                            total_num_revisions += number_revisions
                            # Save each change as a line to JSONL file
                            for change in changes:
                                changes_saved += 1
                                f_out.write(json.dumps(change) + '\n')
                    else:
                        print(f'Skipping page {page_title} as it does not start with "Q".')
                    elem.clear()

        # TODO: uncomment this so it zips change file
        # # Compress the JSONL file
        # bz2_output = jsonl_output + '.bz2'
        # with open(jsonl_output, 'rb') as f_in, bz2.open(bz2_output, 'wb') as f_out:
        #     shutil.copyfileobj(f_in, f_out)
        
        # # Remove the original JSONL file
        # os.remove(jsonl_output)

        log_file =  "file_log.json"
        with open(log_file, 'a', encoding='utf-8') as log:
            for ent_pt in self.entities_processing_times:
                log.write(ent_pt)

        return self.entities, changes_saved, (total_num_revisions / len(self.entities)) if self.entities else 0

    def parse_pages_in_xml(self, file):
            """
                Parse a bz2 dump file and extract changes between revisions.
                Stores the changes in a JSONL file (bz2) and entities in a JSON file.
                
                Args: bz2 file path
                Returns: (number of entities, average number of revisions)
            """

            parent_dir = os.path.dirname(file)
            revision_dir = os.path.join(parent_dir, "revisions")
            os.makedirs(revision_dir, exist_ok=True)

            # Use name of bz2 file as base name
            filename = os.path.basename(file)
            base = filename.replace(".xml", "").replace(".bz2", "")
            jsonl_output = f"{revision_dir}/{base}_changes.jsonl" 

            total_num_revisions = 0
            changes_saved = 0

            with bz2.open(file, mode="rt", encoding="utf-8", errors="ignore") as f:
                
                context = etree.iterparse(file, events=('end', 'start'))
                # Iterate over pages in the XML file
                for event, elem in context:
                    if elem.tag == f'{NS}page':
                        page_title = elem.find(f'{NS}title').text

                        if page_title.startswith("Q"):
                            print('Processing page:', page_title)
                            jsonl_output = f"{revision_dir}/{page_title}_changes.jsonl" # TODO: remove this so it saves everything in same file
                            with open(jsonl_output, 'a', encoding='utf-8') as f_out:
                                
                                changes, number_revisions = self.process_revisions((page_title, elem))
                                print(f'Page {page_title} processed with {number_revisions} revisions and {len(changes)} changes.')
                                total_num_revisions += number_revisions
                                # Save each change as a line to JSONL file
                                for change in changes:
                                    changes_saved += 1
                                    f_out.write(json.dumps(change) + '\n')
                        else:
                            print(f'Skipping page {page_title} as it does not start with "Q".')
                        elem.clear()

            # TODO: uncomment this so it zips change file
            # # Compress the JSONL file
            # bz2_output = jsonl_output + '.bz2'
            # with open(jsonl_output, 'rb') as f_in, bz2.open(bz2_output, 'wb') as f_out:
            #     shutil.copyfileobj(f_in, f_out)
            
            # # Remove the original JSONL file
            # os.remove(jsonl_output)

            log_file =  "file_log.json"
            with open(log_file, 'a', encoding='utf-8') as log:
                for ent_pt in self.entities_processing_times:
                    log.write(ent_pt)

            return self.entities, changes_saved, (total_num_revisions / len(self.entities)) if self.entities else 0
