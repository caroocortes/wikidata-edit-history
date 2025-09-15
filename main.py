import os
import time
import bz2
from argparse import ArgumentParser
from pathlib import Path
import concurrent.futures
import json
import sys

from scripts.utils import human_readable_size, create_db_schema, print_exception_details
from scripts.dump_parser import DumpParser
    
def log_file_process(process_time, num_entities, file_path, size):
    if not isinstance(file_path, Path):
        file_path = Path(file_path) 
    print(f"Finished processing {file_path} ({size}, {num_entities} entities) in {process_time} seconds") 
    with open('processed_files.txt', "a") as f: 
        f.write(f"{file_path.resolve()}\n") 

def process_file(file_path, config):
    """
    Process a single .xml.bz2 file, parse it, and log the results.
    """
    input_bz2 = os.path.basename(file_path)

    parser = DumpParser(file_path=input_bz2, config=config)
    
    print(f"Processing: {file_path}")
    sys.stdout.flush()
    start_process = time.time()
    with bz2.open(file_path, 'rb') as in_f:
        try:
            parser.parse_dump(in_f)
        except Exception as e:
            print(f"Parsing error in DumpParser: {e}")
            print_exception_details(e, file_path)
            return 0, 0, file_path, "0"
            
    end_process = time.time()
    process_time = end_process - start_process
    size = os.path.getsize(file_path)

    size_hr = human_readable_size(size)

    print(f"Processed {input_bz2} in {process_time:.2f} seconds, {human_readable_size(size)}, {parser.num_entities} entities")
    sys.stdout.flush()
    
    parser_log_files = "parser_log_files.json"
    if not os.path.exists(parser_log_files):
        with open(parser_log_files, "w") as f:
            pass  
    with open(parser_log_files, "a", encoding="utf-8") as f:
        json_line = {
            "file": input_bz2,
            "size": size_hr,
            "num_entities": parser.num_entities,
            "process_time_sec": f"{process_time:.2f}"
        }
        f.write(json.dumps(json_line) + "\n")

    return process_time, parser.num_entities, file_path, size_hr


if  __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-f", "--file", help="xml.bz2 file to process", metavar="FILE")
    args = arg_parser.parse_args()

    # Load config
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f) 

    dump_dir = Path(config.get('files_directory', '.'))
    if not dump_dir.exists():
        print("The dump directory doesn't exist")
        raise SystemExit(1)
    
    processed_log = Path("processed_files.txt")

    # Read already processed files
    processed_files = set()
    if processed_log.exists():
        with processed_log.open() as f:
            processed_files = set(line.strip() for line in f)

    # create tables if they don't exist
    create_db_schema()

    if args.file:
        # Process a single file
        input_bz2 = args.file
        if input_bz2 in processed_files:
            print(f"{input_bz2} has already been processed.")
        else:
            process_time, num_entities, file_path, size = process_file(os.path.join(dump_dir, input_bz2), config)
            log_file_process(process_time, num_entities, file_path, size)
    else:
        # Process all .bz2 files in the specified directory
        # files_in_parallel at a time and at most max_files total
            
        # List all .bz2 files in dump_dir
        all_files = [f.resolve() for f in dump_dir.iterdir() if f.is_file() and f.suffix == '.bz2']

        # Sort by modification time (oldest first) -> Initial entities = more revisions
        files_sorted = sorted(all_files, key=lambda f: f.stat().st_mtime)

        # Only keep files that haven't been processed
        files_to_parse = [f for f in files_sorted if f not in processed_files]

        max_workers = config.get('files_in_parallel', 5)
        max_files = config.get('max_files', 5)
        
        sys.stdout.flush()
        if max_files == 1:        
            process_time, num_entities, file_path, size = process_file(files_to_parse[0], config)
            log_file_process(process_time, num_entities, file_path, size)
        else:
            if max_files < max_workers:
                max_workers = max_files

            print(f"Found {len(files_to_parse)} unprocessed .bz2 files in {dump_dir}, processing up to {max_files} files with {max_workers} workers in parallel.")

            files_to_parse = files_to_parse[:max_files]
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)
            try:
                configs = [config] * len(files_to_parse) # has to be an iterable
                for process_time, num_entities, file_path, size in executor.map(process_file, files_to_parse, configs): 
                    if process_time == 0:
                        print(f"Error processing {file_path}, skipping logging.")
                    else:
                        log_file_process(process_time, num_entities, file_path, size)
            finally:
                executor.shutdown(wait=True)
                
                