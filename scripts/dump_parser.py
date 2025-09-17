import xml.sax
import sys
import multiprocessing as mp
import time
import queue
from lxml import etree
import os
import psutil
import threading
from collections import deque

from scripts.page_parser import PageParser
from scripts.const import *


def process_page_xml(page_elem_str, file_path, config):
    parser = PageParser(file_path=file_path, page_elem_str=page_elem_str, config=config)
    try:
        parser.process_page()
    except Exception as e:
        print('Error in page parser')
        print(e)
        raise e

class DumpParser():
    def __init__(self, file_path=None, config=None):
       
        self.config = config
        self.file_path = file_path
        self.num_entities = 0  
        
        self.num_workers = config.get('pages_in_parallel', 2) # processes that process pages in parallel
        self.page_queue = mp.Queue(maxsize=QUEUE_SIZE) # queue that stores pages as they are read
        self.stop_event = mp.Event()

        # TODO: remove
        self.start_time = time.time()
        self.queue_size_history = deque(maxlen=50) # store last 50 queue sizes

        self.workers = []
        for i in range(self.num_workers):
            p = mp.Process(target=self._worker, args=(i,))
            p.start()
            self.workers.append(p)
            
        # TODO: remove
        # monitoring thread
        # self.monitor_thread = threading.Thread(target=self._simple_monitor, daemon=True)
        # self.monitor_thread.start()

    def _simple_monitor(self):
        last_report_time = time.time()
        
        while not self.stop_event.is_set():
            time.sleep(300)  # Check every 5 minutess
            
            current_time = time.time()
            queue_size = self.page_queue.qsize()
            self.queue_size_history.append(queue_size)
            
            # Simple report every 5 minutes
            if current_time - last_report_time > 300:
                elapsed = current_time - self.start_time
                
                # Calculate average queue size
                avg_queue_size = sum(self.queue_size_history) / len(self.queue_size_history) if self.queue_size_history else 0
                
                print(f"\n=== STATUS REPORT ===")
                print(f"Runtime: {elapsed:.1f}s")
                print(f"Pages added to queue: {self.num_entities}")
                print(f"Current queue size: {queue_size}")
                print(f"Average queue size: {avg_queue_size:.1f}")

                sys.stdout.flush()
                
                # Simple resource check
                try:
                    cpu_percent = psutil.cpu_percent(interval=1)
                    memory = psutil.virtual_memory()
                    print(f"System CPU: {cpu_percent:.1f}% | System Memory: {memory.percent:.1f}%")
                except:
                    pass
                
                # Check resources used by the parse_dump process
                current_process = psutil.Process()
                process_memory = current_process.memory_info().rss / 1024**2  # MB
                process_cpu = current_process.cpu_percent()

                # Check resources of child processes (workers)
                children = current_process.children(recursive=True)
                total_worker_memory = sum(child.memory_info().rss for child in children) / 1024**2  # MB
                
                print(f"Parser process: {process_cpu:.1f}% CPU, {process_memory:.1f} MB RAM")
                print(f"All worker processes: {total_worker_memory:.1f} MB RAM")
                print(f"Total project memory: {(process_memory + total_worker_memory):.1f} MB")
                
                # Simple recommendations
                if avg_queue_size < 5:
                    print("Queue size is low - workers might be idle")
                elif avg_queue_size > 15:
                    print("Queue is nearly full - consider more workers")
                
                print("=" * 30)
                last_report_time = current_time

    def get_simple_stats(self):
        runtime = time.time() - self.start_time
        queue_size = self.page_queue.qsize()
        
        stats = {
            'runtime': runtime,
            'entities_processed': self.num_entities,
            'current_queue_size': queue_size,
            'num_workers': self.num_workers,
        }
        
        if self.queue_size_history:
            stats['avg_queue_size'] = sum(self.queue_size_history) / len(self.queue_size_history)
        else:
            stats['avg_queue_size'] = 0
            
        return stats
    
    def _add_worker(self):
        i = len(self.workers)
        p = mp.Process(target=self._worker, args=(i,))
        p.start()
        self.workers.append(p)
        print(f"Added worker {i} due to queue size, total workers: {len(self.workers)}")

    def _worker(self, worker_id):
        """
            Process started in init
            Gets pages from queue and calls process_page_xml which processes the page (entity)
        """
        pages_processed = 0
        total_wait_time = 0
        total_process_time = 0
        while not self.stop_event.is_set() or not self.page_queue.empty():
            wait_start = time.time()
            try:
                page_elem_str = self.page_queue.get(timeout=1) # get is atomic -  only one thread can remove an item at a time
                
                # ---- stats ----
                wait_time = time.time() - wait_start
                total_wait_time += wait_time
                # ---- stats ----
                
                if page_elem_str is None:  # no more pages to process
                    break
                
                process_start = time.time()
                process_page_xml(page_elem_str, self.file_path, self.config)
                
                # ---- stats ----
                process_time = time.time() - process_start
                total_process_time += process_time
                pages_processed += 1
                # ---- stats ----

                sys.stdout.flush()
            except queue.Empty:
                total_wait_time += time.time() - wait_start
                continue
        
        # ---- stats ----
        total_time = total_process_time + total_wait_time
        efficiency = (total_process_time / total_time) * 100 if total_time > 0 else 0
        print(f"Worker {worker_id} finished: {pages_processed} pages, {efficiency:.1f}% efficiency, {total_process_time:.2f}s processing, {total_wait_time:.2f}s waiting")
        sys.stdout.flush()
        # ---- stats ----

    def parse_dump(self, file_obj):
        """
            Reads XML file and extracts pages of entities (title = Q-id).
            Each page is stored in a queue which is accessed by processes in parallel that extract the changes from the revisions
        """

        ns = "http://www.mediawiki.org/xml/export-0.11/"
        page_tag = f"{{{ns}}}page"
        title_tag = f"{{{ns}}}title"

        context = etree.iterparse(file_obj, events=("end",), tag=page_tag)
        
        last_report = time.time()

        for event, page_elem in context:
            keep = False
            entity_id = ""

            # Get title
            title_elem = page_elem.find(title_tag)
            if title_elem is not None:
                entity_id = title_elem.text or ""
                if entity_id.startswith("Q"):
                    keep = True

            if keep:
                queue_size = self.page_queue.qsize()
                if queue_size > 15:  # Queue is getting full
                    print(f"Warning: Queue is {queue_size}/{QUEUE_SIZE} full - processing may be bottlenecked")

                fullness = self.page_queue.qsize() / QUEUE_SIZE
                if fullness > 0.7 and self.num_workers < 6: # go up to max 5 workers
                    self._add_worker()

                # Serialize the page element
                page_elem_str = etree.tostring(page_elem, encoding="unicode")
                self.page_queue.put(page_elem_str)
                self.num_entities += 1

                print(f"Keeping entity {entity_id}, queue size: {self.page_queue.qsize()}/{QUEUE_SIZE} -  total entities read: {self.num_entities + 1}", end='\r')
                sys.stdout.flush()
            

            # Periodic progress report
            if time.time() - last_report > 300:  # Every 30 seconds
                rate = self.num_entities / (time.time() - self.start_time)
                print(f"Progress: {self.num_entities} entities read, {rate:.1f} entities/sec, queue: {queue_size}/{QUEUE_SIZE}")
                sys.stdout.flush()
                last_report = time.time()

            # Clear page element to free memory
            page_elem.clear()
            while page_elem.getprevious() is not None:
                del page_elem.getparent()[0]

            if self.stop_event.is_set():
                break
        
        # Send stop signals to workers
        for _ in range(self.num_workers):
            self.page_queue.put(None)

        # Wait for workers to finish
        for i, p in enumerate(self.workers):
            p.join()
            print(f"Worker {i} finished")

        self.stop_event.set()

        final_stats = self.get_simple_stats()
        print(f"\n=== FINAL STATISTICS ===")
        print(f"Total runtime: {final_stats['runtime']:.1f}s")
        print(f"Total entities processed: {self.num_entities}")
        print(f"Average processing rate: {self.num_entities/final_stats['runtime']:.2f} entities/sec")
        print(f"Workers used: {final_stats['num_workers']}")
        
        sys.stdout.flush()
        # Simple recommendations
        if final_stats['avg_queue_size'] < 10:
            print("Consider reducing workers or increasing files_in_parallel")
        elif final_stats['avg_queue_size'] > 80:
            print("Consider increasing workers (pages_in_parallel)")
        else:
            print("Worker configuration seems well balanced")
