from xml.sax.handler import ContentHandler

class QPageFilter(ContentHandler):
    def __init__(self, writer):
        self.in_title = False
        self.in_page = False
        self.keep = False
        self.buffer = []
        self.writer = writer

    def startElement(self, name, attrs):
        if name == 'page':
            self.in_page = True
            self.keep = False
            self.buffer = ["<page>"]

        elif self.in_page:
            self.buffer.append(f"<{name}>")

        if name == 'title':
            self.in_title = True

    def characters(self, content):
        if self.in_page:
            self.buffer.append(content)
        if self.in_title and content.startswith("Q"):
            print(f"Keeping page with title: {content}")
            self.keep = True

    def endElement(self, name):
        if not self.in_page:
            return

        self.buffer.append(f"</{name}>")

        if name == 'title':
            self.in_title = False

        elif name == 'page':
            if self.keep:
                self.writer.write("".join(self.buffer))
            # Reset state
            self.in_page = False
            self.buffer = []