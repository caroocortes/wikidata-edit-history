import os
import time
import bz2
from argparse import ArgumentParser
from pathlib import Path
import concurrent.futures
import json
import sys
import fcntl
from functools import wraps
import traceback
import multiprocessing as mp

from scripts.db_writer import db_writer
from scripts.utils import human_readable_size, create_db_schema, print_exception_details
from scripts.file_parser import FileParser
from scripts.const import PROCESSED_FILES_PATH, PARSER_LOG_FILES_PATH, CLAIMED_FILES_PATH, LOCK_FILE_PATH

# Load config
with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)


def safe_worker(func):
    """Decorator to catch and log worker exceptions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        worker_pid = os.getpid()
        try:
            print(f"Worker {worker_pid} processing: {args[0] if args else 'unknown'}", flush=True)
            result = func(*args, **kwargs)
            print(f"Worker {worker_pid} completed successfully", flush=True)
            return result
        except MemoryError as e:
            print(f"MEMORY ERROR in worker {worker_pid}", flush=True)
            print(f"File: {args[0] if args else 'unknown'}", flush=True)
            print(traceback.format_exc(), flush=True)
            raise
        except Exception as e:
            print(f"EXCEPTION in worker {worker_pid}", flush=True)
            print(f"Type: {type(e).__name__}", flush=True)
            print(f"File: {args[0] if args else 'unknown'}", flush=True)
            print(f"Message: {str(e)}", flush=True)
            print(traceback.format_exc(), flush=True)
            raise
    return wrapper

# def init_worker():
#     """Initialize cache in each worker process"""
#     global TRANSITIVE_CACHE
#     if TRANSITIVE_CACHE is None:
#         print(f"[Worker {os.getpid()}] Loading transitive closure cache...")
#         TRANSITIVE_CACHE = TransitiveClosureCache(CSV_PATHS)
#         print(f"[Worker {os.getpid()}] Cache loaded")

def log_file_process(process_time, num_entities, file_path, size):
    if not isinstance(file_path, Path):
        file_path = Path(file_path) 
    print(f"Finished processing {file_path} ({size}, {num_entities} entities) in {process_time} minutes") 

    try:
        if not os.path.exists(PROCESSED_FILES_PATH):
            with open(PROCESSED_FILES_PATH, "w") as f:
                pass

        with open(PROCESSED_FILES_PATH, "a") as f: 
            f.write(f"{file_path.resolve()}\n") 
    except Exception as e:
        print(f"Error logging processed file to processed_files.txt {file_path}: {e}")

def process_file(file_path, shared_queue=None):
    """
    Process a single .xml.bz2 file, parse it, and log the results.
    """
    input_bz2 = os.path.basename(file_path)

    parser = FileParser(file_path=input_bz2, config=CONFIG, shared_results_queue=shared_queue)
    
    print(f"Processing: {file_path}")
    sys.stdout.flush()
    start_process = time.time()
    with bz2.open(file_path, 'rb') as in_f:
        try:
            parser.parse_dump(in_f)
        except Exception as e:
            print(f"Parsing error in FileParser: {e}")
            print_exception_details(e, file_path)
            return 0, 0, file_path, "0"
            
    end_process = time.time()
    process_time = end_process - start_process
    size = os.path.getsize(file_path)

    size_hr = human_readable_size(size)
    time_unit = "minutes" if process_time > 60 else "seconds"
    process_time = process_time / 60 if process_time > 60 else process_time
    print(f"Processed {input_bz2} in {process_time:.2f} {time_unit}, {human_readable_size(size)}, {parser.num_entities} entities")
    sys.stdout.flush()
    
    if not os.path.exists(PARSER_LOG_FILES_PATH):
        with open(PARSER_LOG_FILES_PATH, "w") as f:
            pass  
    with open(PARSER_LOG_FILES_PATH, "a", encoding="utf-8") as f:
        json_line = {
            "file": input_bz2,
            "size": size_hr,
            "num_entities": parser.num_entities,
            "process_time_min": f"{process_time:.2f}"
        }
        f.write(json.dumps(json_line) + "\n")
    
    log_file_process(process_time, parser.num_entities, file_path, size_hr)

    return process_time, parser.num_entities, file_path, size_hr


def claim_files(available_files, num_files_to_claim):
    """
    Atomically claim X files from the available pool by writing them to claimed_files.txt
    Returns the list of files this process claimed.
    """
    claimed_by_me = []
    pid = os.getpid()
    
    lock_file = Path(LOCK_FILE_PATH)
    lock_file.touch(exist_ok=True)
    
    with open(lock_file, 'r') as lock:
        try:
            print(f"[PID {pid}] Acquiring lock...")
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            print(f"[PID {pid}] Lock acquired")
            
            claimed_path = Path(CLAIMED_FILES_PATH)
            already_claimed = set()
            if claimed_path.exists():
                with claimed_path.open() as f:
                    for line in f:
                        already_claimed.add(str(Path(line.strip()).resolve()))
            
            print(f"[PID {pid}] Already claimed: {len(already_claimed)} files")
            
            unclaimed = [f for f in available_files if str(f.resolve()) not in already_claimed]
            
            print(f"[PID {pid}] Unclaimed: {len(unclaimed)} files")
            
            if len(unclaimed) == 0:
                print(f"[PID {pid}] No unclaimed files available.")
                return []
            
            to_claim = unclaimed[:num_files_to_claim]
            
            with claimed_path.open('a') as f:
                for file_path in to_claim:
                    f.write(f"{file_path.resolve()}\n")
            
            claimed_by_me = to_claim
            print(f"[PID {pid}] Successfully claimed {len(claimed_by_me)} files for processing")
            
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            print(f"[PID {pid}] Lock released")
    
    return claimed_by_me

if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-f", "--file", help="xml.bz2 file to process", metavar="FILE")
    arg_parser.add_argument("--max-files", help='Maximum number of files to process', type=int, default=None)
    args = arg_parser.parse_args()
    
    dump_dir = Path(CONFIG.get('files_directory', '.'))
    if not dump_dir.exists():
        print("The dump directory doesn't exist")
        raise SystemExit(1)
    
    processed_log = Path(PROCESSED_FILES_PATH)

    processed_files = set()
    if processed_log.exists():
        with processed_log.open() as f:
            processed_files = set(line.strip() for line in f)
        print(f'Found {len(processed_files)} files that have already been processed')
    else:
        processed_files = set()

    create_db_schema()

    if args.file:
        # Single file processing - load cache in main process
        
        input_bz2 = args.file
        if input_bz2 in processed_files:
            print(f"{input_bz2} has already been processed.")
        else:
            process_time, num_entities, file_path, size = process_file(os.path.join(dump_dir, input_bz2))
    else:
        all_files = [f.resolve() for f in dump_dir.iterdir() if f.is_file() and f.suffix == '.bz2']
        files_sorted = sorted(all_files, key=lambda f: f.stat().st_mtime)
        files_to_parse = [f for f in files_sorted if str(f) not in processed_files]

        max_workers = CONFIG.get('files_in_parallel', 5)
        
        if args.max_files is not None:
            max_files = args.max_files
        
        if max_files == 1:
            process_time, num_entities, file_path, size = process_file(files_to_parse[0])
        else:
            if max_files < max_workers:
                max_workers = max_files

            print(f"Found {len(files_to_parse)} unprocessed .bz2 files in {dump_dir}, processing up to {max_files} files with {max_workers} files in parallel.")
            
            if len(files_to_parse) == 0:
                print("No new files to process. Exiting.")
                raise SystemExit(0)
            
            print("Claiming files...")
            files_to_parse = claim_files(files_to_parse, max_files)
            if len(files_to_parse) == 0:
                print("No files to process after claiming. Exiting.")
                raise SystemExit(0)
            
            print(f"Successfully claimed {len(files_to_parse)} files. Starting processing...")

            files_in_parallel = max_workers
            workers_per_file = CONFIG.get('pages_in_parallel', 6)
            total_workers = len(files_to_parse) * workers_per_file
            
            print(f"Starting shared db_writer expecting {total_workers} workers")
            
            # Create shared queue
            shared_queue = mp.Manager().Queue(maxsize=10000)
            
            # Start shared db_writer
            db_writer_process = mp.Process(
                target=db_writer,
                args=(total_workers, shared_queue)
            )
            db_writer_process.start()

            try:

                # Create executor with initializer that loads cache per worker
                executor = concurrent.futures.ProcessPoolExecutor(
                    max_workers=max_workers
                )
                
                futures = {executor.submit(process_file, f, shared_queue): f for f in files_to_parse}

                for future in concurrent.futures.as_completed(futures):
                    file_path = futures[future]
                    try:
                    
                        process_time, num_entities, _, size = future.result(timeout=7200)
                        print(f"Finished {file_path}: {process_time:.2f}min, {num_entities} entities", flush=True)
                    
                    except concurrent.futures.TimeoutError:
                        print(f"Timeout processing {file_path}", flush=True)
                        print(traceback.format_exc(), flush=True)
                        executor.shutdown(wait=True)
                        break
                    except MemoryError as e:
                        print(f"MemoryError processing {file_path}: {e}", flush=True)
                        print(traceback.format_exc(), flush=True)
                        executor.shutdown(wait=True)
                        break
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}", flush=True)
                        print(traceback.format_exc(), flush=True)
                        break
                    sys.stdout.flush()

            except Exception as e:
                print(f"Fatal error in processing: {e}", flush=True)
                print(traceback.format_exc(), flush=True)
            finally:
                print("Waiting for db_writer to finish")
                db_writer_process.join()
                if db_writer_process.exitcode != 0:
                    raise Exception(f"DB writer failed!")
            
            executor.shutdown(wait=True)