import xml.sax
import pandas as pd
import io

import concurrent.futures
import multiprocessing

from scripts.page_parser import PageParser
from scripts.const import *

class DumpParser(xml.sax.ContentHandler):
    def __init__(self, max_workers=None):
        
        self.set_initial_state()  
        self.entities = []   
        self.futures = []  

        if max_workers is None:
            max_workers = multiprocessing.cpu_count() - 5
            print('Number of workers to use: ', max_workers)

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def set_initial_state(self):
        self.page_buffer = []

        self.entity_id = ''

        self.in_title = False                 # True if inside a <title> tag
        self.in_page = False                  # True if inside a <page> block
        self.keep = False                     # if True, keep the current page information
        
    def process_page_xml(self, page_xml_str):
        parser = xml.sax.make_parser()
    
        handler = PageParser()
        parser.setContentHandler(handler)
        
        # Parse page content (revisions)
        parser.parse(io.StringIO(page_xml_str))

        # Update with new entity
        self.entities.append({
            'entity_id': handler.entity_id,
            'label': handler.entity_label
        })
    
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

        if self.in_page and self.keep:
            self.page_buffer.append(DumpParser._serialize_start_tag(name, attrs))

    def characters(self, content):
        """ 
            Called when parser finds text inside tags (e.g. <title>Q12</title>)
        """

        if self.in_title :
            self.entity_id += content
        
        if self.in_page and self.keep:
            self.page_buffer.append(content)

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
        
        if self.in_page and self.keep:
            self.page_buffer.append(DumpParser._serialize_end_tag(name))
        
        if name == 'title' and self.entity_id.startswith("Q"): # at </title> 
            # If the page title starts with Q, we process the revision
            print(f"Keeping page with title: {self.entity_id}")
            self.keep = True
            self.in_title = False
            self.page_buffer.append(DumpParser._serialize_end_tag(name))
        elif name =='title' and not self.entity_id.startswith("Q"):
            print(f'Not keeping page {self.entity_id}')
            self.set_initial_state() # sets buffer to []
            self.in_page = True # reset that I'm still inside the page
        
        if name == 'page': # at </page>
            if self.keep:

                self.page_buffer.append(DumpParser._serialize_end_tag(name)) # save </page> tag
                
                raw_page_xml = ''.join(self.page_buffer)
                self.page_buffer.clear()
                # Submit the page processing to worker
                print(f'Submitted process_page_xml for entity_id {self.entity_id}')
                future = self.executor.submit(self.process_page_xml, raw_page_xml)
                self.futures.append(future)

            # Reset state
            self.set_initial_state()

