from scripts.page_parser import PageParser
from argparse import ArgumentParser
from lxml import etree

arg_parser = ArgumentParser()
arg_parser.add_argument("-f", "--file", help="File to run test. See folder test/", metavar="FILE")

args = arg_parser.parse_args()

file_path = args.file
print(f"Processing: {file_path}")
with open(f'test/{file_path}', 'rt', encoding='utf-8') as in_f:
    try:
        tree = etree.parse(in_f)
        ns = "http://www.mediawiki.org/xml/export-0.11/"
        page_tag = f"{{{ns}}}page"

        for page_elem in tree.findall(page_tag):
            page_str = etree.tostring(page_elem, encoding="unicode")
            parser = PageParser(file_path=file_path, page_elem_str=page_str)
            parser.process_page()

    except Exception as e:
        print(f"Parsing error: {e}")