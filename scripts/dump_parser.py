import xml.sax
import pandas as pd
import io
import concurrent.futures
from pathlib import Path
from concurrent.futures import wait, as_completed
from xml.sax.saxutils import escape
import psycopg2
from dotenv import load_dotenv
import os


from scripts.page_parser import PageParser
from scripts.const import *
from scripts.utils import insert_rows, initialize_csv_files

def process_page_xml(page_xml_str):
    parser = xml.sax.make_parser()
    
    handler = PageParser()
    parser.setContentHandler(handler)
    
    try:
        # Parse page content (revisions)

        parser.parse(io.StringIO(page_xml_str))

        return handler.entity_id, handler.entity_label, handler.changes, handler.revision
    except xml.sax.SAXParseException as e:
        print('ERROR IN PAGE PARSER')
        err_line = e.getLineNumber()
        err_col = e.getColumnNumber()

        print(f"Error at line {err_line}, column {err_col}")

        all_lines = page_xml_str.splitlines()
        start = max(err_line - 4, 0)  # 2 lines before
        end = min(err_line + 1, len(all_lines))  # 1 line after

        print("\n--- XML snippet around error ---")
        for i in range(start, end):
            prefix = ">>" if i + 1 == err_line else "  "
            print(f"{prefix} Line {i+1}: {all_lines[i]}")
        print("-------------------------------")
        raise e

class DumpParser(xml.sax.ContentHandler):
    def __init__(self, file_path=None, max_workers=None):
        self.entity_file_path, self.change_file_path, self.revision_file_path = initialize_csv_files()

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
        self.file_path = file_path # save file path
        self.set_initial_state()  
        self.num_entities = 0  
        self.futures = []  

        if max_workers is None:
            max_workers = 8
            print('Number of workers to use: ', max_workers)

        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)

    def set_initial_state(self):
        self.page_buffer = []

        self.entity_id = ''

        self.in_title = False                 # True if inside a <title> tag
        self.in_page = False                  # True if inside a <page> block
        self.keep = False                     # if True, keep the current page information

        self.in_revision = False             # True if inside a <revision> block
        self.in_revision_id = False          # True if inside the <id> of a revision
        self.in_comment = False              # True if inside the <comment> tag of a revision

        self.in_contributor = False          # True if inside a <contributor> block
        self.in_contributor_username = False # True if inside the contributor's <username>
        
    def process_page_xml_class(self, page_xml_str):
        parser = xml.sax.make_parser()
    
        handler = PageParser()
        parser.setContentHandler(handler)
        
        try:
            # Parse page content (revisions)

            parser.parse(io.StringIO(page_xml_str))

            return handler.entity_id, handler.entity_label, handler.changes, handler.revision
        except xml.sax.SAXParseException as e:
            print('ERROR IN PAGE PARSER')
            err_line = e.getLineNumber()
            err_col = e.getColumnNumber()

            print(f"Error at line {err_line}, column {err_col}")

            all_lines = page_xml_str.splitlines()
            start = max(err_line - 4, 0)  # 2 lines before
            end = min(err_line + 1, len(all_lines))  # 1 line after

            print("\n--- XML snippet around error ---")
            for i in range(start, end):
                prefix = ">>" if i + 1 == err_line else "  "
                print(f"{prefix} Line {i+1}: {all_lines[i]}")
            print("-------------------------------")
            raise e


    @staticmethod
    def _serialize_start_tag(name, attrs):
        """Convert a start tag and its attributes into an XML string."""
        if attrs:
            attr_str = " ".join(f'{k}="{v}"' for k, v in attrs.items())
            return f"<{name} {attr_str}>"
        else:
            return f"<{name}>"

    @staticmethod
    def _serialize_end_tag(name):
        """Convert an end tag into an XML string."""
        return f"</{name}>"

    def startElement(self, name, attrs):
        """
        Called when the parser finds a starting tag (e.g. <page>)
        """
        if name == 'page':
            # Reset state and start buffering a new page
            self.in_page = True
            self.keep = False
            self.page_buffer.append(DumpParser._serialize_start_tag(name, attrs))

        if name == 'title':
            self.in_title = True
            self.page_buffer.append(DumpParser._serialize_start_tag(name, attrs))

        if name == 'revision':
            self.in_revision = True

        # Fields whose content needs to be escaped because of &, <, > (not valid XML)
        if self.in_revision:
            if name == 'comment':
                self.in_comment = True
            elif name == 'contributor':
                self.in_contributor = True

        if self.in_contributor:
            if name == 'username':
                self.in_contributor_username = True

        if self.in_page and self.keep:
            self.page_buffer.append(DumpParser._serialize_start_tag(name, attrs))

    def characters(self, content):
        """ 
            Called when parser finds text inside tags (e.g. <title>Q12</title>)
        """

        if self.in_title :
            self.entity_id += content
        
        if self.in_revision: # Comment and Username aren't escaped (like <text></text> is) so it raises errors with the XML parser
            if self.in_comment or self.in_contributor_username:
                content = escape(content)

        if self.in_page and self.keep:
            self.page_buffer.append(escape(content))

    def endElement(self, name):
        """ 
            Called when a tag ends (e.g. </page>)
        """
        if name == 'mediawiki':
            # End of XML file
            print(f"Finished processing file with {len(self.num_entities)} entities")

            self.conn.close() # close connection to DB
            self.executor.shutdown(wait=True, cancel_futures=True)
            
        if self.in_revision:
            if name == 'comment': # at </comment>
                self.in_comment = False
            elif name == 'contributor': # at </contributor>
                self.in_contributor = False

        if self.in_contributor:
            if name == 'username': # at </username> inside of <contributor></contributor>
                self.in_contributor_username = False
        
        if name == 'title' and self.entity_id.startswith("Q"): # at </title> 
            # If the page title starts with Q, we process the revision
            # print(f"Keeping page with title: {self.entity_id}")
            self.keep = True
            self.in_title = False
            self.page_buffer.append(self.entity_id) # save entity_id

        elif name =='title' and not self.entity_id.startswith("Q"):
            print(f'Not keeping page {self.entity_id}')
        
        # saves all end tags for the page if it's kept
        if self.in_page and self.keep:
            self.page_buffer.append(DumpParser._serialize_end_tag(name))

        if name == 'page': # at </page>
            if self.keep:
                # NOTE: </page> is save in previous if
                raw_page_xml = ''.join(self.page_buffer)
                self.page_buffer.clear()
                # Submit the page processing to worker

                future = self.executor.submit(process_page_xml, raw_page_xml)
                self.futures.append(future)

                # if len(self.futures) >= 15: # limits number of running tasks at a time
                #     print('waiting for futures to complete')
                #     # wait(self.futures)
                
                batch_changes = []
                batch_revisions = []
                batch_entities = []
                done_futures = [f for f in self.futures if f.done()]
                for f in done_futures:
                    entity_id, entity_label, changes, revisions = f.result()
                    batch_revisions.extend(revisions)
                    batch_changes.extend(changes)
                    batch_entities.append((entity_id, entity_label, self.file_path))

                    self.num_entities += 1

                    if len(batch_entities) >= BATCH_SIZE_ENTITIES:
                    
                        insert_rows(self.conn, 'entity', batch_entities, columns=['entity_id', 'entity_label', 'file_path'])
                        batch_entities = []
                        # df_entities = pd.DataFrame([[entity_id, entity_label, self.file_path]], columns=['entity_id', 'entity_label', 'file_path'])
                        # df_entities.to_csv(self.entity_file_path, mode='a', index=False, header=False)

                    if len(batch_changes) >= BATCH_SIZE_CHANGES: # check changes since # changes >= #revisions (worst case: 1 revision has multiple changes)

                        # df_changes = pd.DataFrame(batch_changes)
                        # df_changes.to_csv(self.change_file_path, mode='a', index=False, header=False)

                        # df_revisions = pd.DataFrame(batch_revisions)
                        # df_revisions.to_csv(self.revision_file_path, mode='a', index=False, header=False)

                        insert_rows(self.conn, 'revision', batch_revisions, columns=['revision_id', 'entity_id', 'timestamp', 'user_id', 'username', 'comment'])

                        insert_rows(self.conn, 'change', batch_changes, columns=['revision_id', 'entity_id', 'property_id', 'value_id', 'old_value', 'new_value', 'datatype', 'datatype_metadata', 'change_type', 'change_magnitude'])
                        
                        # set to empty so there are no double inserts
                        batch_changes = []
                        batch_revisions = []
                
                self.futures = [f for f in self.futures if not f.done()]

                if batch_entities:
                    insert_rows(self.conn, 'entity', batch_entities, columns=['entity_id', 'entity_label', 'file_path'])

                if batch_revisions:
                    # df_revisions = pd.DataFrame(batch_revisions)
                    # df_revisions.to_csv(self.revision_file_path, mode='a', index=False, header=False)
                    insert_rows(self.conn, 'revision', batch_revisions, columns=['revision_id', 'entity_id', 'timestamp', 'user_id', 'username', 'comment'])

                if batch_changes:
                    # df_changes = pd.DataFrame(batch_changes)
                    # df_changes.to_csv(self.change_file_path, mode='a', index=False, header=False)
                    insert_rows(self.conn, 'change', batch_changes, columns=['revision_id', 'entity_id', 'property_id', 'value_id', 'old_value', 'new_value', 'datatype', 'datatype_metadata', 'change_type', 'change_magnitude'])


            # Reset state because I reached a </page>
            self.set_initial_state()

