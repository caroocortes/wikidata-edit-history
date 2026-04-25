import sys
import multiprocessing as mp
import time
import queue
import bz2
from lxml import etree
import json
from pathlib import Path
import psycopg2
import pandas as pd
import os
import traceback
import csv
import threading
import gc

from scripts.page_parser import PageParser
from scripts.const import *
from scripts.db_writer import batch_insert
from scripts.utils import print_exception_details

def process_page_xml(page_elem_str, file_path, set_up, property_labels, astronomical_object_types, scholarly_article_types):

    parser = PageParser(file_path=file_path, page_elem_str=page_elem_str, set_up=set_up, property_labels=property_labels, 
                        astronomical_object_types=astronomical_object_types, scholarly_article_types=scholarly_article_types)
    try:
        results = parser.process_page()
        return results

    except Exception as e:
        print('Error in page parser')
        print(e)
        raise e

class FileParser():
    def __init__(self, file_path=None, set_up=None, shared_results_queue=None):
       
        self.set_up = set_up
        self.file_path = file_path
        
        self.batch_size = 5000

        # STATS
        self.total_revisions = 0
        self.num_entities = 0  

        self.num_workers = self.set_up.get('pages_in_parallel', 2) # processes that process pages in parallel

        if self.set_up.get('change_extraction_processing', {}).get('memory_consumption_monitoring', False):
            self.peak_memory_mb = 0.0
            self._stop_memory_monitor = False

            # NOTE: because this runs on a process that will process multiple files, 
            # i get the initial memory in case theres memory that hasn't been freed and it's from a previous file,
            # so i can subtract it from the peak to get the memory used by this file only
            with open('/proc/self/status') as f:
                for line in f:
                    if line.startswith('VmRSS:'):
                        self.initial_mem = int(line.split()[1]) / 1024
                        break

            self._memory_monitor_thread = threading.Thread(target=self._monitor_memory, daemon=True)
            self._memory_monitor_thread.start()

        # START TIME
        self.start_time = time.time()

        if shared_results_queue is not None:
            self.results_queue = shared_results_queue
            self.owns_writer = False
        else:
            # Fallback for single-file mode
            self.results_queue = mp.Queue()
            self.writer_process = mp.Process(target=self._db_writer)
            self.writer_process.start()
            self.owns_writer = True
        
        self.queue_size = self.set_up.get('change_extraction_processing', {}).get('page_queue_size', 10000)
        
        self.page_queue = mp.Queue(maxsize=self.queue_size) # queue that stores pages as they are read
        self.stop_event = mp.Event()

        # global to all page parsers
        self.ASTRONOMICAL_OBJECT_TYPES = pd.read_csv(ASTRONOMICAL_OBJECT_TYPES_PATH)['s'].tolist()
        self.SCHOLARLY_ARTICLE_TYPES = pd.read_csv(SCHOLARLY_ARTICLE_TYPES_PATH)['s'].tolist()

        df = pd.read_csv(PROPERTY_LABELS_PATH, names=['property_id', 'property_label'], header=None)
        self.PROPERTY_LABELS = dict(zip(df['property_id'], df['property_label']))

        # START WORKERS THAT PROCESS PAGES IN PARALLEL
        self.workers = []
        for i in range(self.num_workers):
            p = mp.Process(target=self._worker, args=(i,))
            p.start()
            self.workers.append(p)

    def _monitor_memory(self):
        # VmRSS (Virtual Memory Resident Set Size) returns the exact amount of physical RAM (in kilobytes) that a specific process is currently using
        while not self._stop_memory_monitor:
            try:
                # Collect all PIDs to monitor
                pids = [os.getpid()]  # main process
                pids += [p.pid for p in self.workers if p.pid is not None]
                if hasattr(self, 'writer_process') and self.writer_process.pid is not None:
                    pids.append(self.writer_process.pid)

                total_mem = 0
                for pid in pids:
                    try:
                        with open(f'/proc/{pid}/status') as f:
                            for line in f:
                                if line.startswith('VmRSS:'):
                                    total_mem += int(line.split()[1]) / 1024
                                    break
                    except FileNotFoundError:
                        pass  # process already exited

                if total_mem > self.peak_memory_mb:
                    self.peak_memory_mb = total_mem
            except:
                pass
            time.sleep(0.1)       
    
    def _db_writer(self):
        print(f"[DB_WRITER] Starting - Num workers: {self.num_workers}")
        sys.stdout.flush()

        script_dir = Path(__file__).parent
        db_config_path = Path(self.set_up.get('database_config_path', 'config/db_config.json'))
        try:
            with open(script_dir.parent / db_config_path) as f:
                db_config = json.load(f)
        except Exception as e:
            print(f"Error loading database config from {db_config_path}: {e}")
            raise e
        
        conn = psycopg2.connect(
            dbname=db_config["DB_NAME"],
            user=db_config["DB_USER"],
            password=db_config["DB_PASS"],
            host=db_config["DB_HOST"],
            port=db_config["DB_PORT"],
            connect_timeout=30,
            gssencmode='disable',
            client_encoding='UTF8'
        )

        base_table_names = [
            'revision',
            'value_change',
            'qualifier_change',
            'reference_change',
            'datatype_metadata_change',
            'features_entity',
            'features_text',
            'features_time',
            'features_globecoordinate',
            'features_quantity',
            # 'features_property_replacement',
            'entity_stats'
        ]

        batches = {
            '': {table: [] for table in base_table_names},
            '_sa': {table: [] for table in base_table_names if 'features' not in table},
            '_ao': {table: [] for table in base_table_names if 'features' not in table},
            '_less': {table: [] for table in base_table_names}
        }

        workers_finished = 0
        last_write = time.time()

        try:
            while workers_finished < self.num_workers:
                try:
                    result = self.results_queue.get(timeout=5)
                    
                    if result is None:
                        # Worker finished
                        workers_finished += 1
                        print(f"[DB_WRITER] Worker finished, total finished: {workers_finished}/{self.num_workers}")
                        sys.stdout.flush()
                        continue

                    if result['is_scholarly_article']:
                        table_suffix = '_sa'

                    if result['is_astronomical_object']:
                        table_suffix = '_ao'
                    
                    if not result['is_astronomical_object'] and not result['is_scholarly_article'] and result['has_less_revisions']:
                        table_suffix = '_less'
                    
                    if not result['is_astronomical_object'] and not result['is_scholarly_article'] and not result['has_less_revisions']:
                        table_suffix = ''
                    
                    for table_name in base_table_names:
                        if table_name in batches[table_suffix]:
                            batches[table_suffix][table_name].extend(result.get(table_name, []))
                    
                    current_batch_size = len(batches[table_suffix]['revision'])
                    time_since_write = time.time() - last_write
                    if len(batches[table_suffix]['revision']) >= self.batch_size or (time_since_write > 20 and current_batch_size > 0):
                        batch_insert(conn, batches[table_suffix], self.set_up, table_suffix=table_suffix)

                        # Clear this batch
                        for table in batches[table_suffix]:
                            batches[table_suffix][table] = []

                        last_write = time.time()
                    
                except queue.Empty:
                    for suffix, batch in batches.items():
                        if any(len(v) > 0 for v in batch.values()):
                            batch_insert(conn, batch, self.set_up, table_suffix=suffix)

                            for table in batch:
                                batch[table] = []
        
            for suffix, batch in batches.items():
                if any(len(v) > 0 for v in batch.values()):
                    batch_insert(conn, batch, self.set_up, table_suffix=suffix)
            
            print(f"[DB_WRITER] Completed successfully!")
            sys.stdout.flush()

        except Exception as e:
            print(f'Error in DB writer: {e}')
            print(traceback.format_exc())
            sys.stdout.flush()
            raise e
        finally:
            print(f"[DB_WRITER] Closing connection")
            sys.stdout.flush()
            conn.close()
            print(f"[DB_WRITER] Exiting")
            sys.stdout.flush()
    
    def _worker(self, worker_id):
        """
            Process started in init
            Gets pages from queue and calls process_page_xml which processes the page (entity)
        """
        
        pages_processed = 0

        try:
        
            while not self.stop_event.is_set() or not self.page_queue.empty():
                try:
                    page_elem_str = self.page_queue.get(timeout=1) # get is atomic -  only one thread can remove an item at a time
                    
                    if page_elem_str is None:  # no more pages to process
                        break
                    
                    results = process_page_xml(
                        page_elem_str, 
                        self.file_path, 
                        self.set_up, 
                        self.PROPERTY_LABELS, 
                        self.ASTRONOMICAL_OBJECT_TYPES, 
                        self.SCHOLARLY_ARTICLE_TYPES
                    )
                        
                    pages_processed += 1

                    if results is not None:

                        self.results_queue.put(results)
                        
                        if len(results.get('revision', [])) > 200:
                            gc.collect()
                        
                        results = None

                    if pages_processed % 50 == 0:  # Every 50 entities or with more than 200 revisions
                        gc.collect()

                except queue.Empty:
                    continue
        
        except MemoryError as e:
            print(f"Out of memory processing page in file {self.file_path}: {e}", flush=True)
            print(traceback.format_exc(), flush=True)
        except ValueError as e:
            print(f"Value error processing page in file {self.file_path}: {e}", flush=True)
            print(traceback.format_exc(), flush=True)
        except Exception as e:
            print(f"Error in worker {worker_id} in file {self.file_path}: {e}", flush=True)
            print(traceback.format_exc(), flush=True)
        finally:
            self.results_queue.put(None)

            print(f"Worker {worker_id} FINAL: {pages_processed} pages")
            sys.stdout.flush()

            os._exit(0)

    @staticmethod
    def get_page_size(page_elem_str):
        return len(page_elem_str.encode('utf-8'))

    def parse_dump(self):
        """
            Reads XML file and extracts pages of entities (title = Q-id).
            Each page is stored in a queue which is accessed by processes in parallel that extract the changes from the revisions
        """
        try:
            dump_dir = Path(self.set_up.get('change_extraction_processing', {}).get("files_directory", ''))
            with bz2.open(dump_dir / Path(self.file_path), 'rb') as file_obj:
                ns = "http://www.mediawiki.org/xml/export-0.11/"
                page_tag = f"{{{ns}}}page"
                title_tag = f"{{{ns}}}title"

                context = etree.iterparse(file_obj, events=("end",), tag=page_tag, huge_tree=True) # streams the file, doesn't load everything to memory
                
                last_report = time.time()
                start_time_reading = time.time()
                
                for _, page_elem in context:
                    keep = False
                    entity_id = ""

                    # Get title
                    title_elem = page_elem.find(title_tag)
                    if title_elem is not None:
                        entity_id = title_elem.text or ""
                        if entity_id.startswith("Q"):
                            keep = True

                    if keep:
                        # Serialize the page element
                        page_elem_str = etree.tostring(page_elem, encoding="unicode")

                        revision_count = page_elem_str.count('<revision>')
                        self.total_revisions += revision_count

                        self.page_queue.put(page_elem_str)
                        self.num_entities += 1

                    # Periodic progress report
                    if time.time() - last_report > 600:
                        rate = self.num_entities / (time.time() - self.start_time)
                        queue_size = self.page_queue.qsize()
                        alive_workers = sum(1 for p in self.workers if p.is_alive())
                        print(f"Progress: {self.num_entities} entities read, {rate:.1f} entities/sec, "
                            f"queue: {queue_size}/{self.queue_size}, "
                            f"workers alive: {alive_workers}/{self.num_workers}", flush=True)

                        sys.stdout.flush()
                        last_report = time.time()

                    # Clear page element to free memory
                    page_elem.clear()
                    while page_elem.getprevious() is not None:
                        del page_elem.getparent()[0]

                    if self.stop_event.is_set():
                        break
        except Exception as e:
            print(f"Parsing error in FileParser: {e}")
            print_exception_details(e, self.file_path)
            return 0, 0, self.file_path, "0"
        
        end_time_file_reading = time.time()

        # Send stop signals to workers
        for _ in range(self.num_workers):
            self.page_queue.put(None)

        # Wait for workers to finish
        print("Waiting for worker processes to finish.")
        for i, p in enumerate(self.workers):
            p.join()

        self.stop_event.set()

        if self.owns_writer:
            print("Waiting for writer process to finish.")
            self.writer_process.join()
            if self.writer_process.exitcode != 0:
                raise Exception(f"DB writer process failed with exit code {self.writer_process.exitcode}")
    
        total_time = time.time() - self.start_time

        if self.set_up.get('memory_consumption_monitoring', False):
            self._stop_memory_monitor = True
            self._memory_monitor_thread.join()

        full_file_path = self.set_up.get('change_extraction_processing', {}).get('files_directory', '') + self.file_path
        file_size = os.path.getsize(full_file_path) / (1024 * 1024)  # convert to MB

        mem_data = {
            'file': self.file_path,
            'file_size_mb': file_size, 
            'num_entities': self.num_entities,
            'processed_revisions': self.total_revisions,
            'avg_revisions_per_entity': (self.total_revisions / self.num_entities) if self.num_entities > 0 else 0,
            'file_reading_sec': end_time_file_reading - start_time_reading,
            'total_process_time_sec': total_time,
            'peak_memory_mb': self.peak_memory_mb - self.initial_mem if self.set_up.get('change_extraction_processing', {}).get('memory_consumption_monitoring', False) else 0
        }

        SCRIPT_DIR = Path(__file__).parent  # /scripts
        PROJECT_DIR = SCRIPT_DIR.parent     # home
        LOGS_DIR = PROJECT_DIR / 'logs'

        csv_file_path = LOGS_DIR / PARSER_LOG_FILES_PATH
        file_exists = csv_file_path.exists()

        with open(csv_file_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=mem_data.keys())
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerow(mem_data)

        print(f"\n=== FINAL STATISTICS ===")
        print(f"Total file reading time: {end_time_file_reading - start_time_reading:.1f}s, \n Total processing time: {total_time:.1f}s, \n Total entities processed: {self.num_entities}")
        
        sys.stdout.flush()
