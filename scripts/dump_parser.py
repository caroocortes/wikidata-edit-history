import xml.sax
import io
import concurrent.futures
from xml.sax.saxutils import escape
import sys
import multiprocessing as mp
import time
import queue
from lxml import etree

from scripts.page_parser import PageParser
from scripts.const import *
from scripts.utils import initialize_csv_files


def process_page_xml(page_elem_str, file_path):
    sys.stdout.flush()
    parser = PageParser(file_path=file_path, page_elem_str=page_elem_str)
    
    try:
        sys.stdout.flush()

        parser.process_page()
    
        print(f'Finished processing {parser.entity_id, parser.entity_label}')
        sys.stdout.flush()

        return 1
    except xml.sax.SAXParseException as e:
        print('ERROR IN PAGE PARSER')
        # err_line = e.getLineNumber()
        # err_col = e.getColumnNumber()

        # print(f"Error at line {err_line}, column {err_col}")

        # all_lines = page_elem_str.splitlines()
        # start = max(err_line - 4, 0)  # 2 lines before
        # end = min(err_line + 1, len(all_lines))  # 1 line after

        # print("\n--- XML snippet around error ---")
        # for i in range(start, end):
        #     prefix = ">>" if i + 1 == err_line else "  "
        #     print(f"{prefix} Line {i+1}: {all_lines[i]}")
        # print("-------------------------------")
        raise e

class DumpParser():
    def __init__(self, file_path=None, max_workers=None):
        self.entity_file_path, self.change_file_path, self.revision_file_path = initialize_csv_files()

        self.file_path = file_path # save file path
        self.set_initial_state()  
        self.num_entities = 0  
        self.futures = []  

        if max_workers is None:
            max_workers = 8
            print('Number of workers to use: ', max_workers)

        self.page_queue = mp.Queue(maxsize=100) 
        self.stop_event = mp.Event()

        # Launch worker processes
        self.workers = []
        for _ in range(max_workers):
            p = mp.Process(target=self._worker)
            p.start()
            self.workers.append(p)

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
                page_elem_str = self.page_queue.get(timeout=1) # get is atomic -  only one thread can remove an item at a time
                start = time.time()
                print(f'To process page for entity')
                process_page_xml(page_elem_str, self.file_path)
                self.page_queue.task_done()
                print(f'Finished processing page: {time.time() - start}')
                sys.stdout.flush()
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
            self.start_time = time.time()
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
            for p in self.workers:
                p.join()
            
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
            print(f'Keeping {self.entity_id}')

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

                print(f'Time it took to read page for {self.entity_id}: {time.time() - self.start_time} ')
                sys.stdout.flush()
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
    
    
    
    def parse_dump(self, file_obj):
        ns = "http://www.mediawiki.org/xml/export-0.11/"
        page_tag = f"{{{ns}}}page"
        title_tag = f"{{{ns}}}title"

        context = etree.iterparse(file_obj, events=("end",), tag=page_tag)
        print(f'Inside dump parser!!')
        
        sys.stdout.flush()
        for event, page_elem in context:
            start_time = time.time()
            keep = False
            entity_id = ""

            # Get title
            title_elem = page_elem.find(title_tag)
            if title_elem is not None:
                entity_id = title_elem.text or ""
                if entity_id.startswith("Q"):
                    keep = True
                    self.entity_id = entity_id
                    print(f'Keeping {entity_id}')
                else:
                    print(f'Not keeping page {entity_id}')

            if keep:
                # Serialize the page element
                page_elem_str = etree.tostring(page_elem, encoding="unicode")
                self.page_queue.put(page_elem_str)

            print(f"Time it took to read page {entity_id}: {time.time() - start_time}")
            sys.stdout.flush()

            # Clear page element to free memory
            page_elem.clear()
            while page_elem.getprevious() is not None:
                del page_elem.getparent()[0]

            if self.stop_event.is_set():
                break

        self.stop_event.set()
        print("Finished processing file")
