from scripts.download import DumpDownloader
import os
import argparse

# Argument parser
parser = argparse.ArgumentParser(description="Run script with folder path input.")
parser.add_argument('-p', '--path', required=True, help='Path to the download folder')

args = parser.parse_args()
folder_path = os.path.abspath(args.path)

if not folder_path:
    folder_path = '/san2/data/wikidata-history-dumps' # server folder (default)

print(f"Using folder: {folder_path}")

# ---------- Download dump files ----------
downloader = DumpDownloader(folder_path)
downloader.download_dumps()

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