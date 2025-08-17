import os
import xml.sax
import time
import bz2
import logging
from argparse import ArgumentParser
from pathlib import Path
import multiprocessing
import concurrent.futures

from scripts.utils import human_readable_size
from scripts.dump_parser import DumpParser

logging.basicConfig(
    filename=f'parser_log_files.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

def process_file(file_path):

    input_bz2 = os.path.basename(file_path)
    base = input_bz2.replace(".xml", "").replace(".bz2", "")

    handler = DumpParser(file_path=input_bz2)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    print(f"Processing: {file_path}")
    start_process = time.time()
    with bz2.open(file_path, 'rt', encoding='utf-8') as in_f:
        try:
            parser.parse(in_f)
        except xml.sax.SAXParseException as e:
            print(f"Parsing error in DumpParser: {e}")

            # Get the error position
            err_line = e.getLineNumber()
            err_col = e.getColumnNumber()

            print(f"Error at line {err_line}, column {err_col}")

            # Reopen the file and get surrounding lines
            with bz2.open(file_path, 'rt', encoding='utf-8') as f_err:
                lines = []
                for i, line in enumerate(f_err, start=1):
                    if i >= err_line - 14 and i <= err_line + 4:  # 2 lines before, 2 after
                        lines.append((i, line.rstrip("\n")))
                    if i > err_line + 1:
                        break

            print("\n--- XML snippet around error ---")
            for ln, txt in lines:
                prefix = ">>" if ln == err_line else "  "
                print(f"{prefix} Line {ln}: {txt}")
            print("-------------------------------")


    end_process = time.time()
    process_time = end_process - start_process
    size = os.path.getsize(file_path)

    size_hr = human_readable_size(size)

    logging.info(
        f"Processed {input_bz2} in {process_time:.2f} seconds.\t"
        f"Process information: \t"
        f"{base} size: {human_readable_size(size)} MB\t"
        f"Number of entities: {handler.num_entities}\t"
    )

    return process_time, handler.num_entities, file_path, size_hr


if  __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-f", "--file", help="xml.bz2 file to process", metavar="FILE")
    arg_parser.add_argument("-n", "--number_files", type=int, help="Number of xml.bz2 files to process", metavar="NUMBER_OF_FILES")
    arg_parser.add_argument("-dir", "--directory", help="Directory where xml.bz2 files are stored", metavar="DUMP_DIR")

    args = arg_parser.parse_args()

    dump_dir = Path(args.directory)
    if not dump_dir.exists():
        print("The dump directory doesn't exist")
        raise SystemExit(1)
    
    processed_log = "processed_files.txt"

    # Read already processed files
    if os.path.isfile(processed_log):
        with open(processed_log, "r") as f:
            processed_files = set(line.strip() for line in f)
    else:
        processed_files = set()

    if args.file:
        input_bz2 = args.file
        if input_bz2 in processed_files:
            print(f"{input_bz2} has already been processed. Skipping.")
        else:
            process_file(os.path.join(dump_dir, input_bz2))
            with open(processed_log, "a") as f:
                f.write(f"{input_bz2}\n")
    else:
        max_workers = 3
        dump_dir = Path(dump_dir)  # make sure it's a Path object

        # List all .bz2 files in dump_dir
        all_files = [f for f in dump_dir.iterdir() if f.is_file() and f.suffix == '.bz2']

        # Sort by modification time (oldest first)
        files_sorted = sorted(all_files, key=lambda f: f.stat().st_mtime)

        # Only keep files that haven't been processed
        files_to_parse = [f for f in files_sorted if f.name not in processed_files]

        # Limit number of files if -n was provided
        if args.number_files:
            files_to_parse = files_to_parse[:args.number_files]

            if args.number_files <= max_workers:
                max_workers = args.number_files

        if max_workers == 1:
            process_time, num_entities, file_path, size = process_file(files_to_parse[0])
            print(f"Finished processing {file_path} ({size} MB, {num_entities} entities) in {process_time} seconds")
            with open(processed_log, "a") as f:
                f.write(f"{file_path}\n")
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                for process_time, num_entities, file_path, size in executor.map(process_file, files_to_parse):
                    print(f"Finished processing {file_path} ({size} MB, {num_entities} entities) in {process_time} seconds")
                    with open(processed_log, "a") as f:
                        f.write(f"{file_path}\n")

            executor.shutdown(wait=True, cancel_futures=True)
                