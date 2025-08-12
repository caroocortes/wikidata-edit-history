# def parse_pages_in_xml(self, file):
#     """
#         Parse a bz2 dump file and extract changes between revisions.
#         Stores the changes in a JSONL file (bz2) and entities in a JSON file.
        
#         Args: bz2 file path
#         Returns: (number of entities, average number of revisions)
#     """

#     parent_dir = os.path.dirname(file)
#     revision_dir = os.path.join(parent_dir, "revisions")
#     print(f'creating revisions dir at: {revision_dir}')
#     os.makedirs(revision_dir, exist_ok=True)

#     # Use name of bz2 file as base name
#     filename = os.path.basename(file)
#     base = filename.replace(".xml", "").replace(".bz2", "")
#     jsonl_output = f"{revision_dir}/{base}_changes.jsonl" 

#     total_num_revisions = 0
#     changes_saved = 0

#     start_process = time.time()
#     with bz2.open(file, mode="rb") as f:
#         self.logging.info(f'Time to open file: {time.time() - start_process}')

#         start = time.time()
#         context = etree.iterparse(f, events=('end',), tag=f'{NS}page')
#         self.logging.info(f'Time to generate iterator: {time.time() - start}')
        
#         # Iterate over pages in the XML file
#         for event, elem in context:
            
#             if elem.tag == f'{NS}page': 
                
#                 page_title = elem.find(f'{NS}title')
#                 page_title = page_title.text if page_title is not None else ""

#                 if page_title and not page_title.startswith("Q"):
#                     try:
#                         self.logging.info(f'Skipping page {page_title} as it does not start with "Q".')
#                         elem.clear()
#                         parent = elem.getparent()
#                         if parent is not None:
#                             while elem.getprevious() is not None:
#                                 del parent[0]
#                         continue
#                     except Exception as e:
#                         self.logging.info(f'Error clearing element: {e}')
#                         traceback.print_exc()
                
#                 try:
#                     print('Processing page:', page_title)
#                     changes, number_revisions = self.process_revisions((page_title, elem))
#                     print(f'Page {page_title} processed with {number_revisions} revisions and {len(changes)} changes.')
#                     total_num_revisions += number_revisions
#                 except Exception as e:
#                     self.logging.error(f'Error in process_revisions: {e}')
#                     traceback.print_exc()
                
#                 try:
#                     elem.clear()
#                     parent = elem.getparent()
#                     if parent is not None:
#                         while elem.getprevious() is not None:
#                             del parent[0]
#                 except Exception as e:
#                     self.logging.info(f'Error clearing element: {e}')
#                     traceback.print_exc()
                    
#                 try:
#                     # Save each change as a line to JSONL file
#                     # TODO: remove this so it saves everything in same file
#                     jsonl_output = f"{revision_dir}/{page_title}_changes.jsonl" 
#                     os.makedirs(os.path.dirname(jsonl_output), exist_ok=True)
#                     start = time.time()
#                     with open(jsonl_output, 'a', encoding='utf-8') as f_out:
#                         for change in changes:
#                             changes_saved += 1
#                             f_out.write(json.dumps(change) + '\n')
#                     self.logging.info(f'Time to save changes for entity {page_title}: {time.time() - start} seconds')
#                 except Exception as e:
#                     self.logging.error('Error writing changes to JSONL file: {e}')
#                     traceback.print_exc()

#     self.logging.info(f'Time to process all pages in file {filename}: {time.time() - start_process} seconds')
    
#     # TODO: uncomment this so it zips change file
#     # # Compress the JSONL file
#     # bz2_output = jsonl_output + '.bz2'
#     # with open(jsonl_output, 'rb') as f_in, bz2.open(bz2_output, 'wb') as f_out:
#     #     shutil.copyfileobj(f_in, f_out)
    
#     # # Remove the original JSONL file
#     # os.remove(jsonl_output)

#     log_file = os.path.abspath(f"entities_log_{base}.json")
#     print(f"Writing log to: {log_file}")
#     try:
#         with open(log_file, 'a', encoding='utf-8') as log:
#             for ent_pt in self.entities_processing_times:
#                 log.write(json.dumps(ent_pt) + '\n')
#     except Exception as e:
#         self.logging.error(f'Error writing to log file: {e}')

#     return self.entities, changes_saved, (total_num_revisions / len(self.entities)) if self.entities else 0

# def process_revisions(self, page_data):
#         """
#             Process entity's revisions.

#             Args: tuple of (page title, page)
#             Returns: list of changes and number of revisions
#         """
#         page_title, page = page_data
#         entity_id = page_title

#         previous_revision = None
#         number_revisions = 0
#         total_changes = []

#         # Iterate through each revision in the page using lxml XPath
#         # Need to register namespace for XPath queries
#         ns_map = {'ns': 'http://www.mediawiki.org/xml/export-0.11/'}

#         start = time.time()
#         # Find all revision elements with namespace
#         revisions = page.xpath('./ns:revision', namespaces=ns_map)
#         for rev in revisions:

#             try:
#                 # --- Revision metadata ---
#                 revision_meta = {
#                     "revision_id": rev.findtext('ns:id', namespaces=ns_map),
#                     "timestamp": rev.findtext('ns:timestamp', namespaces=ns_map),
#                     "user_id": rev.findtext('ns:contributor/ns:id', namespaces=ns_map) or '',
#                     'user_name': rev.findtext('ns:contributor/ns:username', namespaces=ns_map) or '',
#                     "comment": rev.findtext('ns:comment', namespaces=ns_map),
#                     "entity_id": entity_id,
#                 }

#                 revision_text = rev.findtext('ns:text', namespaces=ns_map) or ''

#                 if not revision_text: # No JSON data in current revision
#                     if not previous_revision:
#                         # Previous revision is None -> iterate until we find a revision with JSON data
#                         continue
                    
#                     # If previous revision exists, we assume the entity was deleted in this revision
#                     total_changes.extend(self.changes_deleted_created_entity(previous_revision, revision_meta, DELETE_ENTITY))
#                     current_revision = None
#                     print(f'Revision text doesnt exist -> Entity {entity_id} was deleted in revision: {revision_meta["revision_id"]}')

#                 else:

#                     if revision_text.strip():
#                         json_text = html.unescape(revision_text)
#                         try:
#                             current_revision = json.loads(json_text)
#                         except json.JSONDecodeError as e:
#                             print(f'Error decoding JSON in revision {revision_meta["revision_id"]} for entity {entity_id}: {e}. Revision skipped.')
#                             raise e
#                     else:
#                         current_revision = None

#                     # Revision text exists but is empty -> entity was deleted in this revision
#                     if previous_revision and not current_revision:
#                         print(f'Revision text exists but is empty -> Entity {entity_id} was deleted in revision: {revision_meta["revision_id"]}')
#                         total_changes.extend(self.changes_deleted_created_entity(previous_revision, revision_meta, DELETE_ENTITY))
#                         current_revision = None # to be consistent with the other case (revision.text == None)
#                     else:
#                         total_changes.extend(
#                             self.get_changes_from_revisions(
#                                 entity_id, 
#                                 current_revision, 
#                                 previous_revision, 
#                                 revision_meta
#                             )
#                         )

#                 number_revisions += 1
#                 previous_revision = current_revision

#             except Exception as e:
#                 print(f'Error parsing revision: {e}')
#                 traceback.print_exc()
#                 raise e
            
#             # Clear the element to free memory
#             rev.clear()

#         end = time.time()
#         if previous_revision:
#             label = self.get_english_label(previous_revision)
#             self.entities.append({"id": entity_id, "label": label})

#         self.entities_processing_times.append({
#             "entity_id": entity_id,
#             "number_revisions": number_revisions,
#             "processing_time": end - start,
#         })

#         return total_changes, number_revisions

# def get_snapshot_from_dump(file):
#     """Parse a bz2 dump file and extract text from revision."""
    
#     parent_dir = os.path.dirname(file)
#     snapshot_dir = os.path.join(parent_dir, "snapshots")
#     os.makedirs(snapshot_dir, exist_ok=True)

#     # Use name of bz2 file as base name
#     filename = os.path.basename(file)
#     base = filename.replace(".xml", "").replace(".bz2", "")

#     with bz2.open(file, mode="rt", encoding="utf-8", errors="ignore") as f:
#         dump = mwxml.Dump.from_file(f)

#         for page in dump:

#             print('Processing page:', page.title)
#             page_revisions = []
            
#             if page.title.startswith("Q"):
#                 for revision in page:
#                     try:
#                         if not revision.text: # No JSON data in revision
#                             print(f'No JSON data in revision {revision.id} for page {page.title}')
#                             current_revision = {}
#                         else:
#                             json_text = html.unescape(revision.text)
#                             current_revision = json.loads(json_text)
#                             to_remove = ['aliases', 'sitelink', 'reference', 'qualifiers']
#                             comment = revision.comment or ""
#                             if any(word in comment for word in to_remove):
#                                 continue # Skip revisions with these comments (updates to aliases, sitelinks, labels, descriptions, references, qualifiers)

#                             # Remove labels, descriptions in other languages + sitelinks and aliases
#                             if isinstance(current_revision['labels'], dict):
#                                 labels = current_revision['labels']
#                                 current_revision['labels'] = {k: v for k, v in labels.items() if k == 'en'}

#                             if isinstance(current_revision['descriptions'], dict):
#                                 descriptions = current_revision['descriptions']
#                                 current_revision['descriptions'] = {k: v for k, v in descriptions.items() if k == 'en'}
                            
#                             if 'sitelinks' in current_revision:
#                                 del current_revision['sitelinks']
                            
#                             if 'aliases' in current_revision:
#                                 del current_revision['aliases']

#                         # --- Revision data ---
#                         revision = {
#                             "revision_id": revision.id,
#                             "entity_id": page.title if page.title is not None else current_revision.get('id'),
#                             "timestamp": str(revision.timestamp),
#                             "user_id": revision.user.id if revision.user else None,
#                             "comment": revision.comment,
#                             "snapshot": current_revision
#                         }

#                         page_revisions.append(revision)

#                     except Exception as e:
#                         print(f'Error parsing revision: {e}')
#                         raise e

#                 entity_id = page.title if page.title is not None else current_revision.get('id')           
#                 jsonl_output = f"{snapshot_dir}/{entity_id}_snapshot.jsonl" 
#                 with open(jsonl_output, 'a', encoding='utf-8') as f_out:
#                     for rev in page_revisions:
#                         f_out.write(json.dumps(rev, ensure_ascii=False) + '\n')



import os
import bz2
import html
import json
import xml.sax
import pandas as pd
from pathlib import Path
import logging
import time
from argparse import ArgumentParser

from utils import initialize_csv_files, human_readable_size

class SnapshotPageParser(xml.sax.ContentHandler):
    """
        Extracts text from revisions and saves them in a jsonl file.
        Also saves revisions and entities in csv files.
    """
    def __init__(self):
        # TODO: remove this since it will be save in a DB
        self.entity_file_path, self.snapshot_file_path, self.revision_file_path = initialize_csv_files(suffix='snapshots')
        self.set_initial_state()  
        self.entities = []     

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
    

    def set_initial_state(self):
        self.snapshots = []
        self.revision = []
        self.entity_id = None
        self.entity_label = None
        self.entity_label = None

        self.in_title = False                 # True if inside a <title> tag
        self.in_page = False                  # True if inside a <page> block
        self.keep = False                     # if True, keep the current page information
        
        self.in_revision = False             # True if inside a <revision> block
        self.in_revision_id = False          # True if inside the <id> of a revision
        self.in_timestamp = False            # True if inside the <timestamp> tag of a revision
        self.in_comment = False              # True if inside the <comment> tag of a revision
        self.in_revision_text = False        # True if inside the <text> of a revision

        self.in_contributor = False          # True if inside a <contributor> block
        self.in_contributor_id = False       # True if inside the contributor's <id>
        self.in_contributor_username = False # True if inside the contributor's <username>

        self.previous_revision = None

        self.revision_meta = {}
        self.revision_text = ""

    @staticmethod
    def _get_english_label(revision):
        label = SnapshotPageParser()._safe_get_nested(revision, 'labels', 'en', 'value') 
        return label if not isinstance(label, dict) else None

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
    
    def _handle_description_label_change(self, previous_revision, current_revision):
        changes = []
        # --- Label change ---
        prev_label = None
        if previous_revision:
            prev_label = SnapshotPageParser()._safe_get_nested(previous_revision, 'labels', 'en', 'value')
        curr_label = SnapshotPageParser()._safe_get_nested(current_revision, 'labels', 'en', 'value')
        
        if curr_label != prev_label:
            changes.append(1)

        # --- Description change ---
        prev_desc = None
        if previous_revision:
            prev_desc = SnapshotPageParser()._safe_get_nested(previous_revision, 'descriptions', 'en', 'value')
        curr_desc = SnapshotPageParser()._safe_get_nested(current_revision, 'descriptions', 'en', 'value')

        if curr_desc != prev_desc:
            changes.append(1)

        return changes

    def startElement(self, name, attrs):
        """
        Called when the parser finds a starting tag (e.g. <page>)
        """
        if name == 'page':
            # Reset state and start buffering a new page
            self.in_page = True
            self.keep = False
            self.buffer = ["<page>"]

        # Handle flags
        if name == 'title':
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
        
        if self.in_page and self.keep and self.in_revision:

            if 'entity_id' not in self.revision_meta:
                self.revision_meta['entity_id'] = self.entity_id

            if self.in_revision_id:
                self.revision_meta['revision_id'] = content
            
            if self.in_comment:
                self.revision_meta['comment'] = content
            
            if self.in_timestamp:
                self.revision_meta['timestamp'] = content
            
            if self.in_contributor_id or self.in_contributor_username:
                if 'user' not in self.revision_meta:
                    self.revision_meta.setdefault('user', None)
                    self.revision_meta['user'] = content
                else:
                    self.revision_meta['user'] += ' - ' + content

            if self.in_revision_text:
                self.revision_text += content

        if self.in_title and content.startswith("Q"): # If the page title starts with Q, we process the revision
            print(f"Keeping page with title: {content}")
            self.entity_id = content
            self.keep = True
        
    def endElement(self, name):
        """ 
            Called when a tag ends (e.g. </page>)
        """
        if not self.in_page:
            if name == 'mediawiki':
                # End of XML file
                print(f"Finished processing file with {len(self.entities)} entities")

                # TODO: change to save to DB
                df_entities = pd.DataFrame(self.entities)
                df_entities.to_csv(self.entity_file_path, mode='a', index=False, header=False)
            else:
                return
        
        if name == 'revision' and self.keep: # end of revision and we keep it

            # Transform revision text to json
            current_revision = self._parse_json_revision(self.revision_text)

            curr_label = SnapshotPageParser()._get_english_label(current_revision)
            if curr_label and self.entity_label != curr_label: 
                # Keep most current label
                self.entity_label = curr_label
            
            # Remove data that is not the focus of the change extraction
            # keep only english labels
            if isinstance(current_revision['labels'], dict):
                                labels = current_revision['labels']
                                current_revision['labels'] = {k: v for k, v in labels.items() if k == 'en'}

            # keep only english descriptions
            if isinstance(current_revision['descriptions'], dict):
                descriptions = current_revision['descriptions']
                current_revision['descriptions'] = {k: v for k, v in descriptions.items() if k == 'en'}
            
            # remove sitelinks
            if 'sitelinks' in current_revision:
                del current_revision['sitelinks']
            
            # remove aliases
            if 'aliases' in current_revision:
                del current_revision['aliases']

            changes = self._handle_description_label_change(self.previous_revision, current_revision)

            # only save snapshots of revisions with changes to description/label or that actually have claims
            # if this doesn't happen, then the revision may have changes to aliases or sitelinks and we remove them
            if changes or SnapshotPageParser()._safe_get_nested(current_revision, 'claims'):

                self.snapshots.append(current_revision)

                # Save revision metadata to Revision
                self.revision.append(self.revision_meta)

            self.previous_revision = current_revision
            
            self.in_revision = False
            self.revision_meta = {}
            self.revision_text = ''

        if name == 'title': # at </title> 
            self.in_title = False

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
            if self.keep:
                # Update with new entity
                self.entities.append({
                    'entity_id': self.entity_id,
                    'label': self.entity_label
                })

 
                with open(self.snapshot_file_path, 'w') as outfile:
                    for entry in self.snapshots:
                        json.dump(entry, outfile)
                        outfile.write('\n')

                df_revisions = pd.DataFrame(self.revision)
                df_revisions.to_csv(self.revision_file_path, mode='a', index=False, header=False)

                # Reset state
                self.set_initial_state()


if "__main__":

    arg_parser = ArgumentParser()
    arg_parser.add_argument("-n", "--number_files", type=int, help="Number of xml.bz2 files to process", metavar="NUMBER_OF_FILES")
    arg_parser.add_argument("-dir", "--directory", help="Directory where xml.bz2 files are stored", metavar="DUMP_DIR")

    args = arg_parser.parse_args()

    dump_dir = Path(args.directory)
    if not dump_dir.exists():
        print("The dump directory doesn't exist")
        raise SystemExit(1)

    all_files = [f for f in os.listdir(dump_dir) if os.path.isfile(os.path.join(dump_dir, f)) and f.endswith('.bz2') ]
    files_to_parse = all_files[:args.number_files] if args.number_files else all_files

    handler = SnapshotPageParser()
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    for input_bz2 in all_files:
        file_path = os.path.join(dump_dir, input_bz2)
        base = input_bz2.replace(".xml", "").replace(".bz2", "")

        logging.basicConfig(
            filename=f'snapshot_parser_log_{base}.log',
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO,
        )

        print(f"Processing: {input_bz2}. Logs are save in snapshot_parser_log_{base}.log")
        start_process = time.time()
        with bz2.open(file_path, 'rt', encoding='utf-8') as in_f:
            try:
                parser.parse(in_f)
            except xml.sax.SAXParseException as e:
                print(f"Parsing error: {e}")

        end_process = time.time()
        process_time = end_process - start_process
        size = os.path.getsize(input_bz2)

        logging.info(
            f"Extracted snapshots from {input_bz2} in {end_process - start_process:.2f} seconds.\t"
            f"Process information: \t"
            f"{base} size: {human_readable_size(size):.2f} MB\t"
            f"Number of entities: {len(handler.entities)}\t"
            f"Entities: {','.join(handler.entities)}\t"
        )
    # ------------------------------------------------------------------