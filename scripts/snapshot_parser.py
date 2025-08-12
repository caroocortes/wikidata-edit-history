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