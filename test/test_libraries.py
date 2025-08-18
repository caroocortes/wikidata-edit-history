import time
import xml.sax
import xmltodict
from lxml import etree
import random
import bz2

test_file = "../data/wikidata_dumps_20250601/wikidatawiki-20250601-pages-meta-history1.xml-p1p154.bz2"

# Step 2: Parsing with lxml
def parse_with_lxml(file_path):
    num_entities = 0
    start = time.time()
    with open(file_path, "rb") as f:
        for event, page_elem in etree.iterparse(f, events=("end",), tag="page"):
            title = page_elem.findtext("title")
            num_entities += 1
            page_elem.clear()
    print("lxml:", time.time() - start)
    print('Entities: ', num_entities)

# Step 3: Parsing with xml.sax
class SAXHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.page_title = ""
        self.in_title = False
        self.num_entities = 0

    def startElement(self, name, attrs):
        self.current_tag = name
        if name == 'title':
            self.in_title = True

    def characters(self, content):
        if self.in_title:
            self.page_title += content

    def endElement(self, name):
        if name == 'title' and self.page_title.startswith("Q"):
            self.num_entities += 1
            self.in_title = False

def parse_with_sax(file_path):
    start = time.time()
    parser = xml.sax.make_parser()
    handler = SAXHandler()
    parser.setContentHandler(handler)
    with open(file_path, "rb") as f:
        parser.parse(f)
    print("SAX:", time.time() - start)
    print('Entities: ', handler.num_entities)

# Step 4: Parsing with xmltodict
def parse_with_xmltodict(file_path):
    start = time.time()
    num_entities = 0
    with open(file_path, "rb") as f:
        doc = xmltodict.parse(f.read())
        for page in doc['mediawiki']['page']:
            num_entities += 1
    print("xmltodict:", time.time() - start)
    print('Entities: ', num_entities)

def check_files_in_dir():
    import os

    folder_path = "../../../san2/data/wikidata-history-dumps"
    xml_files = [f for f in os.listdir(folder_path) if f.endswith(".xml")]
    print("Number of XML files:", len(xml_files))

check_files_in_dir()