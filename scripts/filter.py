from xml.sax import make_parser
from xml.sax.handler import ContentHandler

class QPageFilter(ContentHandler):
    def __init__(self):
        self.in_title = False
        self.in_page = False
        self.buffer = []
        self.keep = False

    def startElement(self, name, attrs):
        if name == 'page':
            self.in_page = True
            self.buffer = []
            self.keep = False
        if self.in_page:
            self.buffer.append(f"<{name}>")
        if name == 'title':
            self.in_title = True

    def characters(self, content):
        if self.in_page:
            self.buffer.append(content)
        if self.in_title and content.startswith("Q"):
            self.keep = True

    def endElement(self, name):
        if self.in_page:
            self.buffer.append(f"</{name}>")
        if name == 'title':
            self.in_title = False
        if name == 'page':
            if self.keep:
                print("".join(self.buffer))  # Or write to file
            self.in_page = False
            self.buffer = []

parser = make_parser()
parser.setContentHandler(QPageFilter())
parser.parse("dumpfile.xml")