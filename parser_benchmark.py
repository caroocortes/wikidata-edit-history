import bz2
import time
from lxml import etree
from pathlib import Path
import shutil

# Path to your compressed file
file_path = Path("../../../san2/data/wikidata-history-dumps/wikidatawiki-20250601-pages-meta-history1.xml-p1p154.bz2")
temp_uncompressed = Path("../../../san2/data/wikidata-history-dumps/wikidatawiki-20250601-pages-meta-history1.xml-p1p154")

def parse_from_bz2(file_path):
    start = time.time()
    count = 0
    with bz2.open(file_path, 'rb') as f:
        for event, elem in etree.iterparse(f, events=('end',)):
            # You can count elements or just pass
            count += 1
            pass
    end = time.time()
    return end - start, count

def decompress_to_disk(file_path, target_path):
    start = time.time()
    with bz2.open(file_path, 'rb') as f_in:
        with open(target_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    end = time.time()
    return end - start

def parse_from_disk(file_path):
    start = time.time()
    count = 0
    with open(file_path, 'rb') as f:
        for event, elem in etree.iterparse(f, events=('end',)):
            # Count elements or just pass
            count += 1
            pass
    end = time.time()
    return end - start, count

if __name__ == "__main__":
    print("Parsing directly from .bz2...")
    t1, count = parse_from_bz2(file_path)
    print(f"Time: {t1:.2f}s. Entities: {count}")

    print("\nDecompressing to disk...")
    t_decomp = decompress_to_disk(file_path, temp_uncompressed)
    print(f"Decompression time: {t_decomp:.2f}s")

    print("\nParsing from uncompressed file...")
    t2, count = parse_from_disk(temp_uncompressed)
    print(f"Time: {t2:.2f}s. Entities: {count}")

    print("\nTotal time decompress + parse:", t_decomp + t2)

    # Optional: remove temporary uncompressed file
    temp_uncompressed.unlink()