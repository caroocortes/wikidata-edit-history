from scripts.downloader import DumpDownloader
from const import DATA_DIR

BASE_URL = "https://dumps.wikimedia.org/wikidatawiki/20250601/"
DOWNLOAD_DIR = f"{DATA_DIR}/wikidata_dumps_20250601" # the only one that has the pages edit history, the ones from july have this skipped

downloader = DumpDownloader(BASE_URL, DOWNLOAD_DIR, 4)
downloader.download_dumps()

