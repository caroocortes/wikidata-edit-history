import os
import xml.sax
import time
import bz2
import logging
import re
from argparse import ArgumentParser
from pathlib import Path

from scripts.utils import human_readable_size
from scripts.dump_parser import DumpParser

def process_file(input_bz2, dump_dir, parser, handler):
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

    logging.info(
        f"Processed {input_bz2} in {process_time:.2f} seconds.\t"
        f"Process information: \t"
        f"{base} size: {human_readable_size(size)} MB\t"
        f"Number of entities: {len(handler.entities)}\t"
        f"Entities: {','.join([e['entity_id'] for e in handler.entities])}\t"
    )

arg_parser = ArgumentParser()
arg_parser.add_argument("-f", "--file", help="xml.bz2 file to process", metavar="FILE")
arg_parser.add_argument("-n", "--number_files", type=int, help="Number of xml.bz2 files to process", metavar="NUMBER_OF_FILES")
arg_parser.add_argument("-dir", "--directory", help="Directory where xml.bz2 files are stored", metavar="DUMP_DIR")

args = arg_parser.parse_args()

dump_dir = Path(args.directory)
if not dump_dir.exists():
    print("The dump directory doesn't exist")
    raise SystemExit(1)

handler = DumpParser()
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

if args.file:
    input_bz2 = args.file
    process_file(input_bz2, dump_dir, parser, handler)
else:

    all_files = [f for f in os.listdir(dump_dir) if os.path.isfile(os.path.join(dump_dir, f)) and f.endswith('.bz2') ]
    files_to_parse = all_files[:args.number_files] if args.number_files else all_files

    for input_bz2 in all_files:
        process_file(input_bz2, dump_dir, parser, handler)