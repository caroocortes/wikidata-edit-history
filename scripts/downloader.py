import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import traceback

import logging
logging.basicConfig(
    filename='download_log.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

from scripts.parser import DumpParser

class DumpDownloader():

    def __init__(self, base_url: str, download_dir: str, number_of_files: int):
        self.base_url = base_url
        self.download_dir = download_dir
        self.number_of_files = number_of_files
    
    def get_dump_links(self):
        #  Get list of .bz2 files from the wikidata dump service (Scrapper)
        response = requests.get(self.base_url)
        soup = BeautifulSoup(response.text, "html.parser")

        bz2_links = []
        for link in soup.find_all("a"):
            href = link.get("href", "")
            if "pages-meta-history" in href and href.endswith(".bz2"):
                full_url = urljoin(self.base_url, href)
                bz2_links.append(full_url)

        print(f"Found {len(bz2_links)} .bz2 dump files.")
        
        return bz2_links

    def download_file(self, url: str):
        # Download each bz2 file
        filename = url.split("/")[-1]
        path = os.path.join(self.download_dir, filename)
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
                download_end = time.time()
                download_time = download_end - download_start
                logging.info(f"Downloaded {filename} ({size_mb:.2f} MB) in {download_time:.2f} seconds.")
            except Exception as e:
                logging.error(f"Exception occurred when downloading file {filename}: {e}\n{traceback.format_exc()}")

        # Process the downloaded file
        print(f"Processing: {filename}")
        start_process = time.time()
        dump_parser = DumpParser(logging)
        entities, changes_saved, revision_avg = dump_parser.parse_pages_in_xml(path)
        end_process = time.time()
        process_time = end_process - start_process
        logging.info(f"Processed {filename} in {end_process - start_process:.2f} seconds.")
        
        # Remove the downloaded file 
        if os.path.exists(path):
            os.remove(path)
        else:
            print("File does not exist, nothing to remove.")

        logging.info(
            f"Process information: \t"
            f"{filename} size: {size_mb:.2f} MB\t"
            f"Number of entities: {len(entities)}\t"
            f"Avg. number of revisions: {revision_avg:.2f}\t"
            f"Number of changes saved: {changes_saved}\t"
            f"Entities: {','.join(entities)}\t"
            f"Processing time: {process_time:.2f}s\t"
            f"Download time: {download_time:.2f}s\n"
        )
    
    def download_dumps(self):
        # Create download directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)

        self.bz2_links = self.get_dump_links()

        # Download the files in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            if self.number_of_files is None: # Flag to download only a number of files
                executor.map(self.download_file, self.bz2_links)
            else:
                executor.map(self.download_file, self.bz2_links[:self.number_of_files])
       
    
    

    