from scripts.downloader import DumpDownloader
from scripts.parser import DumpParser
import os
from urllib.parse import urljoin
from const import DATA_DIR
import bz2

BASE_URL = "https://dumps.wikimedia.org/wikidatawiki/20250601/"
DOWNLOAD_DIR = f"{DATA_DIR}/wikidata_dumps_20250601" # the only most current one that has the pages edit history, 
                                                    # the ones from july have this skipped

downloader = DumpDownloader(BASE_URL, DOWNLOAD_DIR, 1)
downloader.download_dumps()

# GET SNAPSHOTS
# folder_path = "data/wikidata_dumps_20250601/"
# files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

# dump_parser = DumpParser()

# for file in files[:1]:
#     print('Processing file:', file)
#     dump_parser.get_snapshot_from_dump(os.path.join(folder_path, file))