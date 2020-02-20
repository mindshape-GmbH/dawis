from urllib.parse import urlparse


class URL:
    def __init__(self, url):
        if type(url) is dict:
            url = url['protocol'] + '://' + url['domain'] + url['path'] + ('?' if '' != url['query'] else '')

        parsed_url = urlparse(url)

        self.protocol = parsed_url.scheme
        self.domain = parsed_url.hostname
        self.path = parsed_url.path
        self.query = parsed_url.query

    def __str__(self) -> str:
        return self.protocol + '://' + self.domain + self.path + ('?' if '' != self.query else '')
