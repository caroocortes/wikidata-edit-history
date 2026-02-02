import sys
import multiprocessing as mp
import time
import queue
from lxml import etree
import json
from pathlib import Path
import psycopg2
import pandas as pd
import psutil
import os
import traceback

from scripts.page_parser import PageParser
from scripts.const import *
from scripts.utils import insert_rows_copy


def process_page_xml(page_elem_str, file_path, config, conn, property_labels, astronomical_object_types, scholarly_article_types):
    parser = PageParser(file_path=file_path, page_elem_str=page_elem_str, config=config, connection=conn, property_labels=property_labels, 
                        astronomical_object_types=astronomical_object_types, scholarly_article_types=scholarly_article_types)
    try:
        results = parser.process_page()
        return results
    except Exception as e:
        print('Error in page parser')
        print(e)
        raise e

class FileParser():
    def __init__(self, file_path=None, config=None, shared_results_queue=None):
       
        self.config = config
        self.file_path = file_path
        self.num_entities = 0  
        
        self.batch_size = 5000

        self.num_workers = config.get('pages_in_parallel', 5) # processes that process pages in parallel
        self.max_workers = config.get('max_pages_in_parallel', 8)

        if shared_results_queue is not None:
            self.results_queue = shared_results_queue
            self.owns_writer = False
        else:
            # Fallback for single-file mode
            self.results_queue = mp.Queue()
            self.writer_process = mp.Process(target=self._db_writer)
            self.writer_process.start()
            self.owns_writer = True

        self.page_queue = mp.Queue(maxsize=QUEUE_SIZE) # queue that stores pages as they are read
        self.stop_event = mp.Event()

        # global to all page parsers
        self.ASTRONOMICAL_OBJECT_TYPES = pd.read_csv(ASTRONOMICAL_OBJECT_TYPES_PATH)['s'].tolist()
        self.SCHOLARLY_ARTICLE_TYPES = pd.read_csv(SCHOLARLY_ARTICLE_TYPES_PATH)['s'].tolist()

        df = pd.read_csv(PROPERTY_LABELS_PATH, names=['property_id', 'property_label'], header=None)
        self.PROPERTY_LABELS = dict(zip(df['property_id'], df['property_label']))
   
        self.start_time = time.time()

        self.cumulative_page_size = 0
        self.num_pages = 0

        self.workers = []
        for i in range(self.num_workers):
            p = mp.Process(target=self._worker, args=(i,))
            p.start()
            self.workers.append(p)
            
    def get_simple_stats(self):
        runtime = time.time() - self.start_time
        
        stats = {
            'runtime': runtime,
            'entities_processed': self.num_entities,
            'num_workers': self.num_workers,
        }
            
        return stats
    
    
    def batch_insert(
        self,
        conn, 
        batch,
        table_suffix=''
        ):

        """Function to insert into DB in parallel."""
        
        try:

            if len(batch['revision']) > 0:
                insert_rows_copy(conn, f'revision{table_suffix}', batch['revision'], REVISION_COLS, REVISION_PK)
            
            if len(batch['value_change']) > 0:
                insert_rows_copy(conn, f'value_change{table_suffix}', batch['value_change'], VALUE_CHANGE_COLS, VALUE_CHANGE_PK)
                
            # if len(change_metadata) > 0:
            #     insert_rows(conn, f'value_change_metadata{table_suffix}', change_metadata, ['revision_id', 'property_id', 'value_id', 'change_target', 'change_metadata', 'value'])
            
            if len(batch['qualifier_change']) > 0:
                insert_rows_copy(conn, f'qualifier_change{table_suffix}', batch['qualifier_change'], QUALIFIER_CHANGE_COLS, QUALIFIER_CHANGE_PK)
            
            if len(batch['reference_change']) > 0:
                insert_rows_copy(conn, f'reference_change{table_suffix}', batch['reference_change'], REFERENCE_CHANGE_COLS, REFERENCE_CHANGE_PK)
            
            if len(batch['datatype_metadata_change']) > 0:
                insert_rows_copy(conn, f'datatype_metadata_change{table_suffix}', batch['datatype_metadata_change'], DATATYPE_METADATA_CHANGE_COLS, DATATYPE_METADATA_CHANGE_PK)

            # if len(datatype_metadata_changes_metadata) > 0:
            #     insert_rows(conn, f'datatype_metadata_change_metadata{table_suffix}', datatype_metadata_changes_metadata, ['revision_id', 'property_id', 'value_id', 'change_target', 'change_metadata', 'value'])

            if table_suffix == '' or table_suffix == '_less':
                if len(batch['features_entity']) > 0:
                    insert_rows_copy(conn, f'features_entity{table_suffix}', batch['features_entity'], ENTITY_FEATURE_COLS, ENTITY_FEATURE_PK)
                
                if len(batch['features_text']) > 0:
                    insert_rows_copy(conn, f'features_text{table_suffix}', batch['features_text'], TEXT_FEATURE_COLS, TEXT_FEATURE_PK)
                
                if len(batch['features_time']) > 0:
                    insert_rows_copy(conn, f'features_time{table_suffix}', batch['features_time'], TIME_FEATURE_COLS, TIME_FEATURE_PK)
                
                if len(batch['features_globecoordinate']) > 0:
                    insert_rows_copy(conn, f'features_globecoordinate{table_suffix}', batch['features_globecoordinate'], GLOBE_FEATURE_COLS, GLOBE_FEATURE_PK)
                
                if len(batch['features_quantity']) > 0:
                    insert_rows_copy(conn, f'features_quantity{table_suffix}', batch['features_quantity'], QUANTITY_FEATURE_COLS, QUANTITY_FEATURE_PK)
                
                # if len(batch['features_reverted_edit']) > 0:
                #     insert_rows_copy(conn, f'features_reverted_edit{table_suffix}', batch['features_reverted_edit'], REVERTED_EDIT_FEATURE_COLS, REVERTED_EDIT_FEATURE_PK)
                
                if len(batch['features_property_replacement']) > 0:
                    insert_rows_copy(conn, f'features_property_replacement{table_suffix}', batch['features_property_replacement'], PROPERTY_REPLACEMENT_FEATURE_COLS, PROPERTY_REPLACEMENT_PK)
            
            if len(batch['entity_property_time_stats']) > 0:
                insert_rows_copy(conn, f'entity_property_time_stats{table_suffix}', batch['entity_property_time_stats'], ENTITY_PROPERTY_TIME_STATS_COLS, ENTITY_PROPERTY_TIME_STATS_PK)

            if len(batch['entity_stats']) > 0:
                insert_rows_copy(conn, f'entity_stats{table_suffix}', batch['entity_stats'], ENTITY_STATS_COLS, ENTITY_STATS_PK)

        except Exception as e:
            print(f'There was an error when batch inserting revisions and changes: {e}')
            print(traceback.format_exc())
            raise e

    def _db_writer(self):
        print(f"[DB_WRITER] Starting - Num workers: {self.num_workers}")
        sys.stdout.flush()

        SCRIPT_DIR = Path(__file__).parent
        CONFIG_PATH = SCRIPT_DIR.parent / 'db_config.json'
        with open(CONFIG_PATH) as f:
            db_config = json.load(f)
        
        conn = psycopg2.connect(
            dbname=db_config["DB_NAME"],
            user=db_config["DB_USER"],
            password=db_config["DB_PASS"],
            host=db_config["DB_HOST"],
            port=db_config["DB_PORT"],
            connect_timeout=30,
            gssencmode='disable'
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
            'features_reverted_edit',
            'features_property_replacement',
            'entity_property_time_stats',
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
                        
                        self.batch_insert(conn, batches[table_suffix], table_suffix=table_suffix)
                        # Clear this batch
                        for table in batches[table_suffix]:
                            batches[table_suffix][table] = []

                        last_write = time.time()
                    
                except queue.Empty:
                    for suffix, batch in batches.items():
                        if any(len(v) > 0 for v in batch.values()):
                            self.batch_insert(conn, batch, table_suffix=suffix)
                            for table in batch:
                                batch[table] = []
        
            for suffix, batch in batches.items():
                if any(len(v) > 0 for v in batch.values()):
                    self.batch_insert(conn, batch, table_suffix=suffix)
            
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
    
    def _add_worker(self):
        i = len(self.workers)
        p = mp.Process(target=self._worker, args=(i,))
        p.start()
        self.workers.append(p)
        print(f"Added worker {i} due to queue size, total workers: {len(self.workers)}")
        self.num_workers += 1

    def _worker(self, worker_id):
        """
            Process started in init
            Gets pages from queue and calls process_page_xml which processes the page (entity)
        """

        # one connection per worker
        SCRIPT_DIR = Path(__file__).parent
        CONFIG_PATH = SCRIPT_DIR.parent / 'db_config.json'
        with open(CONFIG_PATH) as f:
            db_config = json.load(f)
        
        conn = psycopg2.connect(
            dbname=db_config["DB_NAME"],
            user=db_config["DB_USER"],
            password=db_config["DB_PASS"],
            host=db_config["DB_HOST"],
            port=db_config["DB_PORT"],
            connect_timeout=30,
            gssencmode='disable'
        )

        conn.autocommit = True

        process = psutil.Process(os.getpid())
        
        # Resource tracking
        max_memory_mb = 0
        total_cpu_percent = 0
        cpu_samples = 0

        pages_processed = 0
        total_wait_time = 0
        total_process_time = 0

        try:
        
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
                    results = process_page_xml(
                        page_elem_str, 
                        self.file_path, 
                        self.config, 
                        conn, 
                        self.PROPERTY_LABELS, 
                        self.ASTRONOMICAL_OBJECT_TYPES, 
                        self.SCHOLARLY_ARTICLE_TYPES
                    )
                    process_time = time.time() - process_start

                    if results is not None:
                        self.results_queue.put(results)
                    
                    # ---- stats ----
                    total_process_time += process_time
                    pages_processed += 1
                    # ---- stats ----

                    # -------- Resource monitorung -------
                    memory_info = process.memory_info()
                    memory_mb = memory_info.rss / 1024 / 1024  # Convert to MB
                    max_memory_mb = max(max_memory_mb, memory_mb)
                    
                    cpu_percent = process.cpu_percent() # returns a float representing the current process CPU utilization as a percentage
                    total_cpu_percent += cpu_percent
                    cpu_samples += 1

                except queue.Empty:
                    total_wait_time += time.time() - wait_start
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
            conn.close()

            total_time = total_process_time + total_wait_time
            efficiency = (total_process_time / total_time) * 100 if total_time > 0 else 0
            avg_cpu = total_cpu_percent / cpu_samples if cpu_samples > 0 else 0
            
            print(f"Worker {worker_id} FINAL: {pages_processed} pages, "
                f"{efficiency:.1f}% efficiency, {total_process_time:.2f}s processing, "
                f"{total_wait_time:.2f}s waiting, Peak Memory: {max_memory_mb:.1f}MB, "
                f"Avg CPU: {avg_cpu:.1f}%")
            sys.stdout.flush()

            os._exit(0)

    @staticmethod
    def get_page_size(page_elem_str):
        return len(page_elem_str.encode('utf-8'))

    def parse_dump(self, file_obj):
        """
            Reads XML file and extracts pages of entities (title = Q-id).
            Each page is stored in a queue which is accessed by processes in parallel that extract the changes from the revisions
        """

        ns = "http://www.mediawiki.org/xml/export-0.11/"
        page_tag = f"{{{ns}}}page"
        title_tag = f"{{{ns}}}title"

        context = etree.iterparse(file_obj, events=("end",), tag=page_tag, huge_tree=True) # streams the file, doesn't load everything to memory
        
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

                # fullness = self.page_queue.qsize() / QUEUE_SIZE
                # if fullness > 0.7 and self.num_workers < self.max_workers:
                #     self._add_worker()

                # Serialize the page element
                page_elem_str = etree.tostring(page_elem, encoding="unicode")
                
                self.page_queue.put(page_elem_str)
                self.num_entities += 1

                page_size = FileParser.get_page_size(page_elem_str)
                self.cumulative_page_size += page_size
                self.num_pages += 1

            # Periodic progress report
            if time.time() - last_report > 600:  # Every 10 min
                rate = self.num_entities / (time.time() - self.start_time)
                queue_size = self.page_queue.qsize()
                queue_fullness = queue_size / QUEUE_SIZE
                print(f"Progress: {self.num_entities} entities read, {rate:.1f} entities/sec, queue: {queue_fullness}")
                print(f"Average page size so far: {self.cumulative_page_size / self.num_pages:.2f} bytes")
                
                sys.stdout.flush()
                last_report = time.time()

            # Clear page element to free memory
            if self.num_entities % 100 == 0:
                page_elem.clear()
                while page_elem.getprevious() is not None:
                    del page_elem.getparent()[0]

            if self.stop_event.is_set():
                break
        
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

        final_stats = self.get_simple_stats()
        print(f"\n=== FINAL STATISTICS ===")
        print(f"Total runtime: {final_stats['runtime']:.1f}s")
        print(f"Total entities processed: {self.num_entities}")
        print(f"Average processing rate: {self.num_entities/final_stats['runtime']:.2f} entities/sec")
        print(f"Workers used: {final_stats['num_workers']}")
        print(f"Total pages processed: {self.num_pages}")
        print(f"Average page size: {self.cumulative_page_size / self.num_pages:.2f} bytes")
        
        sys.stdout.flush()
