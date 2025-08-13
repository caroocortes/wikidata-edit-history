import xml.sax
import pandas as pd
import io
import re
import concurrent.futures
import multiprocessing
from xml.sax.saxutils import escape

from scripts.page_parser import PageParser
from scripts.const import *
from scripts.utils import initialize_csv_files

class TestParser(xml.sax.ContentHandler):
    def __init__(self, max_workers=None):
        self.entity_file_path, self.change_file_path, self.revision_file_path = initialize_csv_files()
        
        self.set_initial_state()  
        self.entities = []   
        self.futures = []  

        if max_workers is None:
            max_workers = multiprocessing.cpu_count() - 8
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
        
    def process_page_xml(self, page_xml_str):
        parser = xml.sax.make_parser()
    
        handler = PageParser()
        parser.setContentHandler(handler)
        
        try:
            # Parse page content (revisions)

            parser.parse(io.StringIO(page_xml_str))

            # Update with new entity
            self.entities.append({
                'entity_id': handler.entity_id,
                'label': handler.entity_label
            })

            return handler.changes, handler.revision
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
            # self.page_buffer.append(DumpParser._serialize_start_tag(name, attrs))

        if name == 'title':
            self.in_title = True
            # self.page_buffer.append(DumpParser._serialize_start_tag(name, attrs))


    def characters(self, content):
        """ 
            Called when parser finds text inside tags (e.g. <title>Q12</title>)
        """

        if self.in_title :
            self.entity_id += content
    

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

                self.executor.shutdown(wait=True)
            else:
                return
            
        
        if name == 'title' and self.entity_id.startswith("Q"): # at </title> 
            # If the page title starts with Q, we process the revision
            print(f"Keeping page with title: {self.entity_id}")
            self.keep = True
            self.in_title = False
            self.entities.append(self.entity_id)
            # self.page_buffer.append(self.entity_id) # save entity_id

        elif name =='title' and not self.entity_id.startswith("Q"):
            print(f'Not keeping page {self.entity_id}')
        
        # saves all end tags for the page if it's kept
        # if self.in_page and self.keep:
        #     self.page_buffer.append(TestParser._serialize_end_tag(name))

        if name == 'page': # at </page>
            if self.keep:
                # NOTE: </page> is save in previous if

                # Submit the page processing to worker
                print(f'Should submit process_page_xml for entity_id {self.entity_id}')
                
            # Reset state because I reached a </page>
            self.set_initial_state()


if "__main__":
    import time
    import bz2
    import os
    import logging
    from scripts.utils import human_readable_size

    handler = TestParser()
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    file_path = 'data/wikidata_dumps_20250601/wikidatawiki-20250601-pages-meta-history9.xml-p12293632p12341782.bz2'

    start_process = time.time()
    with bz2.open(file_path, 'rt', encoding='utf-8') as in_f:
        try:
            parser.parse(in_f)
        except xml.sax.SAXParseException as e:
            print(f"Parsing error in DumpParser: {e}")

            # Get the error position
            err_line = e.getLineNumber()
            err_col = e.getColumnNumber()

            print(f"Error at line {err_line}, column {err_col}")

            # Reopen the file and get surrounding lines
            with bz2.open(file_path, 'rt', encoding='utf-8') as f_err:
                lines = []
                for i, line in enumerate(f_err, start=1):
                    if i >= err_line - 14 and i <= err_line + 4:  # 2 lines before, 2 after
                        lines.append((i, line.rstrip("\n")))
                    if i > err_line + 1:
                        break

            print("\n--- XML snippet around error ---")
            for ln, txt in lines:
                prefix = ">>" if ln == err_line else "  "
                print(f"{prefix} Line {ln}: {txt}")
            print("-------------------------------")


    end_process = time.time()
    process_time = end_process - start_process
    size = os.path.getsize(file_path)

    logging.info(
        f"Processed {file_path} in {process_time:.2f} seconds.\t"
        f"Process information: \t"
        f"File size: {human_readable_size(size)} MB\t"
        f"Number of entities: {len(handler.entities)}\t"
        f"Entities: {','.join(handler.entities)}\t"
    )