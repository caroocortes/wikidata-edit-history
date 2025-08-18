import xml.sax
import io
import concurrent.futures
from pathlib import Path
from xml.sax.saxutils import escape
from dotenv import load_dotenv
import sys
import queue
import threading

from scripts.page_parser import PageParser
from scripts.const import *
from scripts.utils import initialize_csv_files


def process_page_xml(page_xml_str, file_path):
    parser = xml.sax.make_parser()
    handler = PageParser(file_path=file_path)
    parser.setContentHandler(handler)
    
    try:
        # Parse page content (revisions)
        parser.parse(io.StringIO(page_xml_str))
    
        print(f'Finished processing {handler.entity_id, handler.entity_label}')
        sys.stdout.flush()

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

        self.file_path = file_path # save file path
        self.set_initial_state()  
        self.num_entities = 0  
        self.futures = []  

        if max_workers is None:
            max_workers = 8
            print('Number of workers to use: ', max_workers)

        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)

        self.page_queue = queue.Queue(maxsize=100)  # adjust maxsize depending on memory
        self.stop_event = threading.Event()

        # Launch worker threads
        for _ in range(max_workers):
            self.executor.submit(self._worker)

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

    def _worker(self):
        """
            Runs in a thread
            Gets pages from queue and calls process_page_xml which processes the page
        """
        while not self.stop_event.is_set() or not self.page_queue.empty():
            try:
                page_xml_str = self.page_queue.get(timeout=1) # get is atomic -  only one thread can remove an item at a time
                process_page_xml(page_xml_str, self.file_path)
                self.page_queue.task_done()
                self.num_entities += 1
            except queue.Empty:
                continue

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
            print(f"Finished processing file with {self.num_entities} entities")
            self.stop_event.set()
            self.page_queue.join()  # Wait for all pages to be processed
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
                self.page_queue.put(raw_page_xml) # send page to queue
                self.page_buffer = []
                # Submit the page processing to worker

                # future = self.executor.submit(process_page_xml, raw_page_xml, self.file_path)
                # self.futures.append(future)

                # if len(self.futures) >= 15: # limits number of running tasks at a time
                #     print('waiting for futures to complete')
                #     concurrent.futures.wait(self.futures)
                #     for future in concurrent.futures.as_completed(self.futures):
                #         future.result()
                #     self.futures = []
                    
            # Reset state because I reached a </page>
            self.set_initial_state()

