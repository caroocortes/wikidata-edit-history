import os
import argparse
import xml.sax
import time
import bz2
from scripts.filter import PageParser

# Argument parser
# parser = argparse.ArgumentParser(description="Run script with folder path input.")
# parser.add_argument('-p', '--path', required=True, help='Path to the download folder')

# args = parser.parse_args()
# folder_path = os.path.abspath(args.path)

# if not folder_path:
#     folder_path = '/san2/data/wikidata-history-dumps' # server folder (default)

# print(f"Using folder: {folder_path}")

handler = PageParser()
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

# base = input_bz2_path.replace(".xml", "").replace(".bz2", "")

# logging.basicConfig(
#     filename=f'parse_log_{base}.log',
#     filemode='a',
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     level=logging.INFO,
# )

# print(f"Processing: {input_bz2_path}")
# start_process = time.time()
# with bz2.open(input_bz2_path, 'rt', encoding='utf-8') as in_f:
#     try:
#         parser.parse(in_f)
#     except xml.sax.SAXParseException as e:
#         print(f"Parsing error: {e}")

print(f"Processing: test.xml")
start_process = time.time()
with open('scripts/test.xml', 'rt', encoding='utf-8') as in_f:
    try:
        parser.parse(in_f)
    except xml.sax.SAXParseException as e:
        print(f"Parsing error: {e}")

# end_process = time.time()
# process_time = end_process - start_process
# size = os.path.getsize(input_bz2_path)

# logging.info(
#     f"Processed {input_bz2_path} in {end_process - start_process:.2f} seconds.\t"
#     f"Process information: \t"
#     f"{base} size: {human_readable_size(size):.2f} MB\t"
#     f"Number of entities: {len(entities)}\t"
#     f"Entities: {','.join(entities)}\t"
# )



# GET SNAPSHOTS
# DOWNLOAD_DIR = f"{DATA_DIR}/wikidata_dumps_20250601"
# files = [f for f in os.listdir(DOWNLOAD_DIR) if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]

# dump_parser = DumpParser()

# for file in files:
#     print(file)
#     if not file.endswith(".bz2"):
#         continue
#     print('Processing file:', file)
#     dump_parser.get_snapshot_from_dump(os.path.join(DOWNLOAD_DIR, 'wikidatawiki-20250601-pages-meta-history1.xml-p1p154.bz2'))