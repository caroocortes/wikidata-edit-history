import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import time
import traceback
import logging

from .const import WIKIDATA_SERVICE_URL, DOWNLOAD_LINKS_FILE_PATH

logging.basicConfig(
    filename=f'download_log.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

class DumpDownloader():

    def __init__(self, download_dir: str):
        self.download_dir = download_dir

    def get_dump_links(self):
        #  Get list of .bz2 files from the wikidata dump service (Scrapper)
        response = requests.get(WIKIDATA_SERVICE_URL)
        soup = BeautifulSoup(response.text, "html.parser")

        bz2_links = []
        for link in soup.find_all("a"):
            href = link.get("href", "")
            if "pages-meta-history" in href and href.endswith(".bz2"):
                full_url = urljoin(WIKIDATA_SERVICE_URL, href)
                bz2_links.append(full_url)

        print(f"Found {len(bz2_links)} .bz2 dump files.")
        print(f"Saving download links to {DOWNLOAD_LINKS_FILE_PATH}")
        with open(DOWNLOAD_LINKS_FILE_PATH, 'w', encoding='utf-8') as f:
            for file in bz2_links:
                f.write(f"{file}\n")
        
        return bz2_links

    def download_file(self, url: str):

        # Download each bz2 file
        filename = url.split("/")[-1]
        path = os.path.join(self.download_dir, filename)
        base = filename.replace(".xml", "").replace(".bz2", "")

        if os.path.exists(path):
            print(f"Already downloaded: {filename}")
        else:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1,
                            status_forcelist=[429, 500, 502, 503, 504])
            session.mount('http://', HTTPAdapter(max_retries=retries))
            session.mount('https://', HTTPAdapter(max_retries=retries))

            print(f"Downloading: {filename}")
            download_start = time.time()
            try:
                with session.get(url, stream=True) as r:
                    r.raise_for_status()

                    size_bytes = int(r.headers.get('Content-Length', 0))
                    size_mb = size_bytes / (1024 * 1024)

                    with open(path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024*1024):
                            if chunk:
                                f.write(chunk)
                download_time = time.time() - download_start
                logging.info(f"Downloaded {filename} ({size_mb:.2f} MB) in {download_time:.2f} seconds.")
                return True
            except Exception as e:
                logging.error(f"Exception occurred when downloading file {filename} with url {url}: {e}\n{traceback.format_exc()}")
                return False

    def download_dumps(self):
        # Create download directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)

        if os.path.exists(self.download_dir) and os.access(self.download_dir, os.R_OK | os.W_OK):
            print(f"Folder {self.download_dir} exists and is readable and writable.")
        else:
            print(f"No access to the folder {self.download_dir} or it doesn't exist.")
            return

        file_path = Path(DOWNLOAD_LINKS_FILE_PATH)
        if file_path.is_file():
            with file_path.open("r", encoding="utf-8") as f:
                bz2_links = f.read().splitlines()
        else:
            bz2_links = self.get_dump_links()

        folder_path = Path(self.download_dir)
        file_count = sum(1 for f in folder_path.iterdir() if f.is_file())

        count_ok = 0
        if len(bz2_links) == file_count:
            print(f'Files have already been downloaded at {self.download_dir}.')
        else:
            for link in bz2_links:
                download_ok = self.download_file(link)
                if download_ok:
                    count_ok += 1
                time.sleep(1) # 10 minutes

            logging.info(f'Finished downloading {count_ok} files.')