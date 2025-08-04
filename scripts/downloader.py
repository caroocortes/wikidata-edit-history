import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

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
            return
        print(f"Downloading: {filename}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()

            size_bytes = int(r.headers.get('Content-Length', 0))
            size_mb = size_bytes / (1024 * 1024)

            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Finished: {filename}")

        log_path = os.path.join(self.download_dir, "download_log.txt")
        with open(log_path, "a") as log_file:
            log_file.write(f"{filename}\t{size_mb:.2f} MB\n")
    
    def download_dumps(self):
        # Create download directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)

        self.bz2_links = self.get_dump_links()

        # Download the files in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            if self.number_of_files is None: # Flag to download only a number of files
                executor.map(self.download_file, self.bz2_links)
            else:
                executor.map(self.download_file, self.bz2_links[:self.number_of_files])
       

    
    

    