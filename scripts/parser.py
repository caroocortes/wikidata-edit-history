import mwxml
import html
import json

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

        # --- Label change ---
        prev_label = None
        if previous_revision:
            prev_label = previous_revision.get('labels', {}).get('en', {}).get('value')
        curr_label = current_revision.get('labels', {}).get('en', {}).get('value')

        if curr_label != prev_label:
            changes.append({
                **revision_meta,
                "entity_id": entity_id,
                "property_id": "label",
                "old_value": prev_label,
                "new_value": curr_label,
                "change_type": "create" if not prev_label else "update"
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
                "change_type": "create" if not prev_desc else "update"
            })

        # --- Statements (P-IDs) ---
        prev_claims_raw = previous_revision.get('claims') if previous_revision else {}
        curr_claims_raw = current_revision.get('claims')

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
                    print(f'New statements found: p_id: {new_pid}, value: {new_value}')
                    changes.append({
                        **revision_meta,
                        "entity_id": entity_id,
                        "property_id": new_pid,
                        "old_value": None,
                        "new_value": new_value,
                        "change_type": "create"
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
                        "change_type": "delete"
                    })

        # --- Check updates of statements between revisions ---
        remaining_pids = prev_claims_pids.intersection(curr_claims_pids)
        for pid in remaining_pids:

            # Get statement for the same P-ID in previous revision
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
                        "change_type": "delete"
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
                        "change_type": "create" 
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
                        "change_type": "update"
                    })

        return changes

    def get_revision_from_dump(self, file):
        dump = mwxml.Dump.from_file(open(file))

        # Use name of bz2 file as base name
        base = file.replace(".xml", "").replace(".bz2", "")
        jsonl_output = f"{base}_changes.jsonl" 
        entities_output = f"{base}_entities.json"

        for page in dump.pages:  # Each page is a Q-ID entity
            entity_id = page.title
            previous_revision = None

            for revision in page:
                try:
                    if not revision.text: # No JSON data in revision
                        continue

                    json_text = html.unescape(revision.text)
                    current_revision = json.loads(json_text)

                    if current_revision['type'] != 'wikibase-item': # Not a Wikibase item (entity)
                        continue

                    # --- Revision metadata ---
                    revision_meta = {
                        "revision_id": revision.id,
                        "timestamp": revision.timestamp.isoformat(),
                        "user_id": revision.user.id if revision.user else None,
                        "comment": revision.comment
                    }

                    changes = self.parse_revision(entity_id, current_revision, previous_revision, revision_meta)

                    with open(jsonl_output, 'w', encoding='utf-8') as f_out:
                        for change in changes:
                            f_out.write(json.dumps(change) + '\n')

                    previous_revision = current_revision

                except Exception as e:
                    print(f'Error parsing revision: {e}')
            
            label = self.get_english_label(current_revision)
            self.entities.append({"id": entity_id, "label": label})

        with open(entities_output, "w", encoding="utf-8") as f:
            json.dump(self.entities, f, ensure_ascii=False, indent=2)


if "__main__":

    with open("prueba.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    #  -- Remove labels, descriptions in other languages + sitelinks and aliases -- 
    # new_data = []
    # for item in data:
    #     if 'json' in item and 'labels' in item['json']:
    #         labels = item['json']['labels']
    #         if isinstance(labels, dict):
    #             # Keep only "en" label
    #             item['json']['labels'] = {k: v for k, v in labels.items() if k == 'en'}

    #     if 'json' in item and 'descriptions' in item['json']:
    #         labels = item['json']['descriptions']
    #         if isinstance(labels, dict):
    #             # Keep only "en" label
    #             item['json']['descriptions'] = {k: v for k, v in labels.items() if k == 'en'}
    #     if 'json' in item and 'sitelinks' in item['json']:
    #         del item['json']['sitelinks']
    #     if 'json' in item and 'aliases' in item['json']:
    #         del item['json']['aliases']
    #     new_data.append(item)
    
    # with open("prueba.json", "w", encoding="utf-8") as f:
    #     json.dump(data, f, ensure_ascii=False, indent=2)


    dump_parser = DumpParser()
    prev_rev = None
    these_changes = []
    for item in data:

        revision_metadata = {
            "revision_id": item['rev_id'], 
            "timestamp": item['timestamp'], 
            "user": item['user'], 
            "comment": item['comment'],
        }
        curr_rev = item['json']
        changes = dump_parser.parse_revision(item['entity_id'], curr_rev, prev_rev, revision_metadata)
        prev_rev = curr_rev
        these_changes.extend(changes)

    print(f'Total changes found: {len(these_changes)}')
    jsonl_output = 'cambios.json'
    with open(jsonl_output, 'w', encoding='utf-8') as f_out:
        for change in these_changes:
            f_out.write(json.dumps(change) + '\n')