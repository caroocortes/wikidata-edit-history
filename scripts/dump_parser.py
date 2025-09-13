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
        self.futures = []  
        
        self.num_workers = config.get('pages_in_parallel', 6) # processes that process pages in parallel
        self.page_queue = mp.Queue(maxsize=100) 
        self.stop_event = mp.Event()

        # Monitoring additions
        self.worker_stats = mp.Manager().dict()
        self.queue_size_history = deque(maxlen=1000)  # Track queue size over time
        self.start_time = time.time()

        # Initialize stats 
        self.start_time = time.time()
        self.pages_added_to_queue = 0
        self.last_queue_check = time.time()
        self.queue_size_history = deque(maxlen=100)

        self.workers = []
        for i in range(self.num_workers):
            p = mp.Process(target=self._worker, args=(i,))
            p.start()
            self.workers.append(p)
            
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._simple_monitor, daemon=True)
        self.monitor_thread.start()

    def _simple_monitor(self):
        last_report_time = time.time()
        
        while not self.stop_event.is_set():
            time.sleep(10)  # Check every 10 seconds
            
            current_time = time.time()
            queue_size = self.page_queue.qsize()
            self.queue_size_history.append(queue_size)
            
            # Simple report every minute
            if current_time - last_report_time > 300:
                elapsed = current_time - self.start_time
                
                # Calculate average queue size
                avg_queue_size = sum(self.queue_size_history) / len(self.queue_size_history) if self.queue_size_history else 0
                
                print(f"\n=== STATUS REPORT ===")
                print(f"Runtime: {elapsed:.1f}s")
                print(f"Pages added to queue: {self.pages_added_to_queue}")
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
                
                # Check resources used by the main process
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
                elif avg_queue_size > 85:
                    print("Queue is nearly full - consider more workers")
                
                print("=" * 30)
                last_report_time = current_time

    def get_simple_stats(self):
        """Get basic statistics without complex shared objects"""
        runtime = time.time() - self.start_time
        queue_size = self.page_queue.qsize()
        
        stats = {
            'runtime': runtime,
            'pages_queued': self.pages_added_to_queue,
            'current_queue_size': queue_size,
            'num_workers': self.num_workers,
        }
        
        if self.queue_size_history:
            stats['avg_queue_size'] = sum(self.queue_size_history) / len(self.queue_size_history)
        else:
            stats['avg_queue_size'] = 0
            
        return stats

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

                # ---- stats ----
                if pages_processed % 100 == 0:
                    efficiency = (total_process_time / (total_process_time + total_wait_time)) * 100 if total_wait_time > 0 else 100 # time spent processing / total time
                    print(f"Worker {worker_id}: {pages_processed} pages processed, {efficiency:.1f}% efficiency")
                # ---- stats ----
                
                sys.stdout.flush()
            except queue.Empty:
                total_wait_time += time.time() - wait_start
                continue
        
        # ---- stats ----
        total_time = total_process_time + total_wait_time
        efficiency = (total_process_time / total_time) * 100 if total_time > 0 else 0
        print(f"Worker {worker_id} finished: {pages_processed} pages, {efficiency:.1f}% efficiency, {total_process_time:.2f}s processing")
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

        pages_read = 0
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
                    self.entity_id = entity_id

            if keep:
                queue_size = self.page_queue.qsize()
                if queue_size > 90:  # Queue is getting full
                    print(f"Warning: Queue is {queue_size}/100 full - processing may be bottlenecked")

                # Serialize the page element
                page_elem_str = etree.tostring(page_elem, encoding="unicode")
                self.page_queue.put(page_elem_str)
                self.num_entities += 1

                # monitoring
                self.pages_added_to_queue += 1
                pages_read += 1

            # Periodic progress report
            if time.time() - last_report > 30:  # Every 30 seconds
                rate = pages_read / (time.time() - self.start_time)
                print(f"Progress: {pages_read} entities read, {rate:.1f} entities/sec, queue: {queue_size}/100")
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
        print(f"Total entities processed: {final_stats['pages_queued']}")
        print(f"Average processing rate: {final_stats['pages_queued']/final_stats['runtime']:.2f} entities/sec")
        print(f"Workers used: {final_stats['num_workers']}")
        
        sys.stdout.flush()
        # Simple recommendations
        if final_stats['avg_queue_size'] < 10:
            print("Consider reducing workers or increasing files_in_parallel")
        elif final_stats['avg_queue_size'] > 80:
            print("Consider increasing workers (pages_in_parallel)")
        else:
            print("Worker configuration seems well balanced")
