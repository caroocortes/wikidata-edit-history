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


def process_page_xml(page_elem_str, file_path, config, worker_stats):
    worker_id = os.getpid()
    start_time = time.time()
    parser = PageParser(file_path=file_path, page_elem_str=page_elem_str, config=config)
    try:
        sys.stdout.flush()
        parser.process_page()

        processing_time = time.time() - start_time
        worker_stats['processing_times'].append(processing_time)
        worker_stats['pages_processed'] += 1
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

        # Initialize stats for each worker
        for i in range(self.num_workers):
            self.worker_stats[i] = mp.Manager().dict({
                'pages_processed': 0,
                'total_processing_time': 0,
                'total_wait_time': 0,
                'last_activity': time.time(),
                'processing_times': mp.Manager().list()
            })

        self.workers = []
        for i in range(self.num_workers):
            p = mp.Process(target=self._worker, args=(i,))
            p.start()
            self.workers.append(p)
            
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
        self.monitor_thread.start()

    def _monitor_workers(self):
        """Background thread to monitor worker performance"""
        while not self.stop_event.is_set():
            time.sleep(5)  # Monitor every 5 seconds
            
            current_time = time.time()
            total_pages = sum(stats['pages_processed'] for stats in self.worker_stats.values())
            queue_size = self.page_queue.qsize()
            self.queue_size_history.append((current_time, queue_size))
            
            print(f"\n=== WORKER PERFORMANCE REPORT ===")
            print(f"Total pages processed: {total_pages}")
            print(f"Current queue size: {queue_size}")
            print(f"Runtime: {current_time - self.start_time:.1f}s")
            
            active_workers = 0
            idle_workers = 0
            
            for worker_id, stats in self.worker_stats.items():
                pages = stats['pages_processed']
                total_proc_time = stats['total_processing_time']
                total_wait_time = stats['total_wait_time']
                last_activity = stats['last_activity']
                
                # Calculate efficiency metrics
                total_time = total_proc_time + total_wait_time
                if total_time > 0:
                    efficiency = (total_proc_time / total_time) * 100
                else:
                    efficiency = 0
                    
                # Check if worker is active (processed something in last 10 seconds)
                idle_time = current_time - last_activity
                if idle_time < 10:
                    active_workers += 1
                    status = "ACTIVE"
                else:
                    idle_workers += 1
                    status = f"IDLE ({idle_time:.1f}s)"
                
                avg_proc_time = total_proc_time / pages if pages > 0 else 0
                
                print(f"Worker {worker_id}: {pages:4d} pages | "
                      f"Efficiency: {efficiency:5.1f}% | "
                      f"Avg: {avg_proc_time:.3f}s/page | "
                      f"Status: {status}")
            
            print(f"Active workers: {active_workers}/{self.num_workers}")
            
            # Resource utilization
            process = psutil.Process()
            cpu_percent = process.cpu_percent()
            memory_info = process.memory_info()
            print(f"CPU: {cpu_percent:.1f}% | Memory: {memory_info.rss / 1024**2:.1f} MB")
            print("=" * 50)
            
    def get_optimization_recommendations(self):
        """Analyze performance and suggest optimizations"""
        total_runtime = time.time() - self.start_time
        if total_runtime < 30:  # Need some runtime to make recommendations
            return "Not enough runtime data for recommendations"
        
        recommendations = []
        
        # Analyze worker efficiency
        efficiencies = []
        idle_workers = 0
        
        for worker_id, stats in self.worker_stats.items():
            total_proc_time = stats['total_processing_time']
            total_wait_time = stats['total_wait_time']
            total_time = total_proc_time + total_wait_time
            
            if total_time > 0:
                efficiency = (total_proc_time / total_time) * 100
                efficiencies.append(efficiency)
                
                if efficiency < 50:  # Worker is idle more than 50% of the time
                    idle_workers += 1
        
        avg_efficiency = sum(efficiencies) / len(efficiencies) if efficiencies else 0
        
        # Queue size analysis
        if len(self.queue_size_history) > 10:
            recent_queue_sizes = [size for _, size in list(self.queue_size_history)[-10:]]
            avg_queue_size = sum(recent_queue_sizes) / len(recent_queue_sizes)
            
            if avg_queue_size < 5:
                recommendations.append("Queue size is low - consider reducing workers or increasing files_in_parallel")
            elif avg_queue_size > 80:
                recommendations.append("Queue is nearly full - consider increasing workers")
        
        # Worker efficiency analysis
        if avg_efficiency < 60:
            recommendations.append(f"Low worker efficiency ({avg_efficiency:.1f}%) - workers spend too much time waiting")
            
        if idle_workers > self.num_workers // 3:
            recommendations.append(f"{idle_workers} workers are mostly idle - consider reducing pages_in_parallel")
        
        # CPU utilization check
        cpu_usage = psutil.cpu_percent(interval=1)
        if cpu_usage < 70:
            recommendations.append(f"CPU usage is low ({cpu_usage:.1f}%) - you could increase files_in_parallel")
        
        return recommendations

    def _worker(self, worker_id):
        """
            Process started in init
            Gets pages from queue and calls process_page_xml which processes the page (entity)
        """
        worker_stats = self.worker_stats[worker_id]
        while not self.stop_event.is_set() or not self.page_queue.empty():
            try:
                page_elem_str = self.page_queue.get(timeout=1) # get is atomic -  only one thread can remove an item at a time
                if page_elem_str is None:  # no more pages to process
                    break
                
                process_page_xml(page_elem_str, self.file_path, self.config, worker_stats)
                sys.stdout.flush()
            except queue.Empty:
                continue

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
        read_start_time = time.time()
        
        sys.stdout.flush()
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

            # Periodic performance logging
            if pages_read % 5000 == 0 and pages_read > 0:
                elapsed = time.time() - read_start_time
                rate = pages_read / elapsed
                print(f"Reading rate: {rate:.1f} pages/second")

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

        recommendations = self.get_optimization_recommendations()
        print("\n=== OPTIMIZATION RECOMMENDATIONS ===")
        for rec in recommendations:
            print(f"• {rec}")
        
        print("Finished processing file")
