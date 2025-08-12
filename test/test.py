import xml.sax
from scripts.dump_parser import PageParser
from argparse import ArgumentParser

arg_parser = ArgumentParser()
arg_parser.add_argument("-f", "--file", type=int, help="File to run test. See folder test/", metavar="FILE")

args = arg_parser.parse_args()

handler = PageParser()
parser = xml.sax.make_parser()
parser.setContentHandler(handler)

file_path = args.file
print(f"Processing: {file_path}")
with open(f'test/{file_path}', 'rt', encoding='utf-8') as in_f:
    try:
        parser.parse(in_f)
    except xml.sax.SAXParseException as e:
        print(f"Parsing error: {e}")