import os
import time
from argparse import ArgumentParser
from pathlib import Path
import concurrent.futures
import json
import sys
import fcntl
import traceback
import multiprocessing as mp
import gc
import yaml

from scripts.db_writer import db_writer
from scripts.utils import create_db_schema
from scripts.file_parser import FileParser
from scripts.const import PROCESSED_FILES_PATH, CLAIMED_FILES_PATH, LOCK_FILE_PATH, SETUP_PATH

with open(SETUP_PATH, 'r') as f:
    set_up = yaml.safe_load(f)

def log_file_process(file_path):
    if not isinstance(file_path, Path):
        file_path = Path(file_path) 
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

    file_parser = FileParser(file_path=input_bz2, set_up=set_up, shared_results_queue=shared_queue)
    
    print(f"Processing: {file_path}")
    sys.stdout.flush()

    start_process = time.time()
    file_parser.parse_dump()
    end_process = time.time()
    process_time = end_process - start_process

    num_entities = file_parser.num_entities

    del file_parser
    gc.collect() 

    print(f"Processed {input_bz2} in {process_time:.2f} secs, {num_entities} entities")
    sys.stdout.flush()
    
    log_file_process(file_path)
    return 0


def claim_files(available_files, num_files_to_claim):
    """
    Claim X files from the the directiory by writing them to claimed_files.txt
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
            
            print(f"Already claimed: {len(already_claimed)} files")

            unclaimed = [f for f in available_files if str(f.resolve()) not in already_claimed]
            
            print(f"Unclaimed: {len(unclaimed)} files")
            
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
    arg_parser.add_argument("-f", "--file", help="name of the file to process (e.g., example.xml.bz2), not the path.", metavar="FILE")
    arg_parser.add_argument("-n", "--max_files", help='Maximum number of files to process', type=int, default=None)
    args = arg_parser.parse_args()
    
    dump_dir = Path(set_up.get('change_extraction_processing', {}).get("files_directory", ''))
    if not dump_dir.exists():
        print(f"The dump directory {dump_dir} doesn't exist")
        raise SystemExit(1)
    
    processed_log = Path(PROCESSED_FILES_PATH)

    processed_files = set()
    if processed_log.exists():
        with processed_log.open() as f:
            processed_files = set(line.strip() for line in f)
        print(f'Found {len(processed_files)} files that have already been processed')
    else:
        open(processed_log, 'w').close()
        processed_files = set()

    # CLAIMED files and LOCK file are used to coordinate between processes when claiming files to process in parallel.
    if not Path(CLAIMED_FILES_PATH).exists():
        open(CLAIMED_FILES_PATH, 'w').close()
        
    if not Path(LOCK_FILE_PATH).exists():
        open(LOCK_FILE_PATH, 'w').close()

    # Creating DB schema
    create_db_schema(set_up)

    if args.file:
        # Single file processing
        input_bz2 = args.file
        if input_bz2 in processed_files:
            print(f"{input_bz2} has already been processed.")
        else:
            # Check if the file is already claimed by another process
            lock_file = Path(LOCK_FILE_PATH)
            lock_file.touch(exist_ok=True)

            with open(lock_file, 'r') as lock:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                    
                    claimed_path = Path(CLAIMED_FILES_PATH)
                    already_claimed = set()
                    if claimed_path.exists():
                        with claimed_path.open() as f:
                            for line in f:
                                already_claimed.add(str(Path(line.strip()).resolve()))
                    
                    if str(Path(dump_dir / input_bz2).resolve()) not in already_claimed:
                        with claimed_path.open('a') as f:
                            f.write(f"{Path(dump_dir / input_bz2)}\n")
                        print(f"Claimed {input_bz2} for processing")

                        process_time, num_entities, file_path, size = process_file(os.path.join(dump_dir, input_bz2))
                    else:
                        print(f"{input_bz2} is already claimed by another process.")
                        raise SystemExit(1)
                finally:                    
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    else:
        all_files = [f.resolve() for f in dump_dir.iterdir() if f.is_file() and f.suffix == '.bz2']
        files_sorted = sorted(all_files, key=lambda f: f.stat().st_mtime)
        files_to_parse = [f for f in files_sorted if str(f) not in processed_files]

        max_workers = set_up.get('change_extraction_processing', {}).get('files_in_parallel', 5)
        
        if args.max_files is not None:
            max_files = args.max_files
        
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
        workers_per_file = set_up.get('change_extraction_processing', {}).get('pages_in_parallel', 2)
        total_workers = len(files_to_parse) * workers_per_file
        
        print(f"Starting shared db_writer expecting {total_workers} workers")
        
        # Create shared queue
        shared_queue = mp.Manager().Queue(maxsize=set_up.get('change_extraction_processing', {}).get('db_max_queue_size', 10000))
        
        # Start shared db_writer
        db_writer_process = mp.Process(
            target=db_writer,
            args=(set_up, total_workers, shared_queue)
        )
        db_writer_process.start()

        try:

            executor = concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers,
            )
            
            futures = {executor.submit(process_file, f, shared_queue): f for f in files_to_parse}

            for future in concurrent.futures.as_completed(futures):
                file_path = futures[future]
                try:
                    result = future.result(timeout=7200)
                
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
                    executor.shutdown(wait=True)
                    break
                sys.stdout.flush()

        except Exception as e:
            print(f"Fatal error in processing: {e}", flush=True)
            print(traceback.format_exc(), flush=True)
            executor.shutdown(wait=True)
        finally:
            print("Waiting for db_writer to finish")
            db_writer_process.join()
            if db_writer_process.exitcode != 0:
                raise Exception(f"DB writer failed!")
        
        executor.shutdown(wait=True)