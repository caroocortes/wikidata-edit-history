import mwxml
import html
import json
import os
import bz2
from concurrent.futures import ProcessPoolExecutor
import shutil

from const import CREATE_ENTITY, UPDATE_PROPERTY_VALUE

class DumpParser():

    def __init__(self):
        self.entities = []
        pass
    
    def get_english_label(self, revision):
        return revision.get('labels', {}).get('en', {}).get('value', None)

    def get_property_mainsnak(self, stmt, property_=None):
        try:
            return stmt["mainsnak"].get(property_, None)
        except (KeyError, TypeError):
            return None

    def parse_revision(self, entity_id, current_revision, previous_revision, revision_meta):
        changes = []

        if not previous_revision:
            # Entity was created again or for the first time
            for property_id, property_stmts in current_revision['claims'].items():
                for stmt in property_stmts:
                    changes.append( {
                        **revision_meta,
                        "entity_id": entity_id,
                        "property_id": property_id,
                        "old_value": None,  
                        "new_value": stmt,
                        "change_type": CREATE_ENTITY
                    })

        else:

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
                    "change_type": "CREATE_PROPERTY" if not prev_label else "UPDATE_PROPERTY"
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
                    "change_type": "CREATE_PROPERTY" if not prev_desc else "UPDATE_PROPERTY"
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
                        print(f'New statements found: p_id: {new_pid}, value: {new_value}')
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": new_pid,
                            "old_value": None,
                            "new_value": new_value,
                            "change_type": "CREATE_PROPERTY"
                        })

            # --- Deleted statements in current revision ---
            if prev_claims_pids - curr_claims_pids:

                removed_pids = prev_claims_pids - curr_claims_pids
                for removed_pid in removed_pids:
                    prev_statements = prev_claims.get(removed_pid, [])
                    for s in prev_statements:
                        old_value = s.get('mainsnak', {}).get('datavalue')
                        print(f'Statement removed: p_id: {removed_pid}, value: {old_value}')
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": removed_pid,
                            "old_value": old_value,
                            "new_value": None,
                            "change_type": "DELETE_PROPERTY"
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
                        # deleted
                        print(f'Statement removed for existing property: p_id: {pid}, value: {old_value}')
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": pid,
                            "old_value": old_value,
                            "new_value": None,
                            "change_type": "DELETE_PROPERTY_VALUE" 
                        })

                    elif curr_stmt and prev_stmt is None:
                        # new
                        print(f'New Statement for existing property: p_id: {pid}, value: {new_value}')
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": pid,
                            "old_value": None,
                            "new_value": new_value,
                            "change_type": "CREATE_PROPERTY_VALUE" 
                        })

                    prev_hash = self.get_property_mainsnak(prev_stmt, 'hash') if prev_stmt else None
                    curr_hash = self.get_property_mainsnak(curr_stmt, 'hash') if curr_stmt else None

                    if prev_hash and curr_hash and new_value and old_value and prev_hash != curr_hash:
                        # Statement was updated in current revision
                        print(f'Statement updated: p_id: {pid}, old value: {old_value}, new value: {new_value}')
                        changes.append({
                            **revision_meta,
                            "entity_id": entity_id,
                            "property_id": pid,
                            "old_value": old_value,
                            "new_value": new_value,
                            "change_type": "UPDATE_PROPERTY_VALUE"
                        })

        return changes

    """ Auxiliary function to extract full snapshot from revision - can be removed later """
    def get_snapshot_from_dump(self, file):
        """Parse a bz2 dump file and extract entity snapshots with revisions."""
        
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
                                to_remove = ['aliases', 'sitelink', 'wbsetlabel', 'wbsetdescription', 'reference', 'qualifiers']
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

    def extract_changes_from_page(self, page_data):

        """"
            Extract changes from an entity's revisions.
            
            Args: tuple of (page title, page)
            Returns: list of changes and number of revisions
        """
        page_title, page = page_data
        entity_id = page_title

        previous_revision = None
        number_revisions = 0

        total_changes = []

        if entity_id.startswith("Q"): # Only process entities pages
            
            # Iterate through each revision in the page
            for revision in page:
                try:
                    # --- Revision metadata ---
                    revision_meta = {
                        "revision_id": revision.id,
                        "timestamp": str(revision.timestamp),
                        "user_id": revision.user.id if revision.user else None,
                        "comment": revision.comment
                    }

                    if not revision.text: # No JSON data in revision
                        if not previous_revision:
                            continue
                        for property_id, property_stmts in previous_revision['claims'].items():
                            for stmt in property_stmts:
                                total_changes.append( {
                                    ** revision_meta,
                                    "entity_id": entity_id,
                                    "property_id": property_id,
                                    "old_value": stmt,  
                                    "new_value": None,
                                    "change_type": "DELETE_ENTITY"
                                })

                        current_revision = None

                    else:

                        json_text = html.unescape(revision.text)
                        current_revision = json.loads(json_text)

                        # Entity was removed in this revision
                        if previous_revision and (not current_revision or current_revision.keys() == 0 or len(current_revision) == 0):
                            for property_id, property_stmts in previous_revision['claims'].items():
                                for stmt in property_stmts:
                                    total_changes.append( {
                                        **revision_meta,
                                        "entity_id": entity_id,
                                        "property_id": property_id,
                                        "old_value": stmt,  
                                        "new_value": None,
                                        "change_type": "DELETE_ENTITY"
                                    })
                            
                            current_revision = None # to be consistent with the other case (revision.text == None)

                        total_changes.extend(self.parse_revision(entity_id, 
                                                                 current_revision, 
                                                                 previous_revision, 
                                                                 revision_meta))

                    number_revisions += 1
                    previous_revision = current_revision

                except Exception as e:
                    print(f'Error parsing revision: {e}')
        
        if previous_revision:
            label = self.get_english_label(previous_revision)
            self.entities.append({"id": entity_id, "label": label})

        return total_changes, number_revisions
    
    def parse_pages_in_dump(self, file):
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
        entities_output = f"{base}_entities.json"

        total_num_revisions = 0
        changes_saved = 0

        with bz2.open(file, mode="rt", encoding="utf-8", errors="ignore") as f:
            dump = mwxml.Dump.from_file(f)

            page_data = ((page.title, list(page)) for page in dump)
            
            with open(jsonl_output, 'a', encoding='utf-8') as f_out:
                # Extract changes from each page in parallel
                with ProcessPoolExecutor(max_workers=4) as executor:
                    for changes, number_revisions in executor.map(self.extract_changes_from_page, page_data):
                        total_num_revisions += number_revisions
                        # Save each change as a line to JSONL file
                        for change in changes:
                            changes_saved += 1
                            f_out.write(json.dumps(change) + '\n')

            bz2_output = jsonl_output + '.bz2'

        # Compress the JSONL file
        with open(jsonl_output, 'rb') as f_in, bz2.open(bz2_output, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        
        os.remove(jsonl_output)

        return self.entities, changes_saved, (total_num_revisions / len(self.entities)) if self.entities else 0