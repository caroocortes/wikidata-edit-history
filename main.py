import os
import xml.sax
import time
import bz2
import logging
from argparse import ArgumentParser
from pathlib import Path

from scripts.utils import human_readable_size
from scripts.dump_parser import DumpParser

def process_file(input_bz2, dump_dir):
    file_path = os.path.join(dump_dir, input_bz2)
    base = input_bz2.replace(".xml", "").replace(".bz2", "")

    logging.basicConfig(
        filename=f'parser_log_{base}.log',
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO,
    )

    print(f"Processing: {input_bz2}")
    start_process = time.time()
    with bz2.open(file_path, 'rt', encoding='utf-8') as in_f:
        try:
            parser.parse(in_f)
        except xml.sax.SAXParseException as e:
            print(f"Parsing error: {e}")

    end_process = time.time()
    process_time = end_process - start_process
    size = os.path.getsize(input_bz2)

    logging.info(
        f"Processed {input_bz2} in {process_time:.2f} seconds.\t"
        f"Process information: \t"
        f"{base} size: {human_readable_size(size):.2f} MB\t"
        f"Number of entities: {len(handler.entities)}\t"
        f"Entities: {','.join(handler.entities)}\t"
    )

arg_parser = ArgumentParser()
arg_parser.add_argument("-f", "--file", type=int, help="xml.bz2 file to process", metavar="FILE")
arg_parser.add_argument("-n", "--number_files", type=int, help="Number of xml.bz2 files to process", metavar="NUMBER_OF_FILES")
arg_parser.add_argument("-dir", "--directory", help="Directory where xml.bz2 files are stored", metavar="DUMP_DIR")

args = arg_parser.parse_args()

dump_dir = Path(args.directory)
if not dump_dir.exists():
    print("The dump directory doesn't exist")
    raise SystemExit(1)

if args.file:
    input_bz2 = args.file
    process_file(input_bz2, dump_dir)
else:

    all_files = [f for f in os.listdir(dump_dir) if os.path.isfile(os.path.join(dump_dir, f)) and f.endswith('.bz2') ]
    files_to_parse = all_files[:args.number_files] if args.number_files else all_files

    handler = DumpParser(max_workers=1)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)

    for input_bz2 in all_files:
        process_file(input_bz2, dump_dir)