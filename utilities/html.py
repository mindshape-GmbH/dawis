from io import StringIO
from html.parser import HTMLParser
from lxml.html import fromstring as document_from_html, tostring as document_to_str


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, data):
        self.text.write(data)

    def get_data(self):
        return self.text.getvalue()

    def error(self, message):
        pass


def strip_html(html: str) -> str:
    tree = document_from_html(html)

    for element_to_remove in tree.xpath('//script | //style | //svg | //noscript'):
        element_to_remove.getparent().remove(element_to_remove)

    html = document_to_str(tree).decode('utf-8')

    parser = _HTMLStripper()
    parser.feed(html)

    return parser.get_data()
