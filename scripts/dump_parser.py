import xml.sax
import sys
import multiprocessing as mp
import time
import queue
from lxml import etree

from scripts.page_parser import PageParser
from scripts.const import *


def process_page_xml(page_elem_str, file_path):
    sys.stdout.flush()
    parser = PageParser(file_path=file_path, page_elem_str=page_elem_str)
    
    try:
        sys.stdout.flush()

        parser.process_page()
    
        sys.stdout.flush()

        return 1
    except xml.sax.SAXParseException as e:
        print('ERROR IN PAGE PARSER')
        raise e

class DumpParser():
    def __init__(self, file_path=None, max_workers=None):

        self.file_path = file_path # save file path
        self.num_entities = 0  
        self.futures = []  

        if max_workers is None:
            max_workers = NUM_PAGE_PROCESS # change in const. TODO: maybe move to config file or something
            print('Number of workers to use: ', max_workers)
        
        self.num_workers = max_workers
        self.page_queue = mp.Queue(maxsize=100) 
        self.stop_event = mp.Event()

        # Launch worker processes
        self.workers = []
        for _ in range(max_workers):
            p = mp.Process(target=self._worker)
            p.start()
            self.workers.append(p)

    def _worker(self):
        """
            Runs in a thread
            Gets pages from queue and calls process_page_xml which processes the page
        """
        while not self.stop_event.is_set() or not self.page_queue.empty():
            try:
                page_elem_str = self.page_queue.get(timeout=1) # get is atomic -  only one thread can remove an item at a time
                if page_elem_str is None:  # no more pages to process
                    break
                
                # start = time.time()
                process_page_xml(page_elem_str, self.file_path)
                # print(f'Finished processing page in _worker: {time.time() - start}')
                sys.stdout.flush()
            except queue.Empty:
                continue

    def parse_dump(self, file_obj):
        ns = "http://www.mediawiki.org/xml/export-0.11/"
        page_tag = f"{{{ns}}}page"
        title_tag = f"{{{ns}}}title"

        context = etree.iterparse(file_obj, events=("end",), tag=page_tag)
        
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
                self.num_entities += 1

            print(f"Time it took to read page {entity_id}: {time.time() - start_time}")
            sys.stdout.flush()

            # Clear page element to free memory
            page_elem.clear()
            while page_elem.getprevious() is not None:
                del page_elem.getparent()[0]

            if self.stop_event.is_set():
                break
        
        for _ in range(self.num_workers):
            self.page_queue.put(None)

        for p in self.workers:
            p.join()

        self.stop_event.set()
        print("Finished processing file")
