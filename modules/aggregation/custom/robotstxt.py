from database.connection import Connection
from utilities.configuration import Configuration
from utilities.url import URL
from datetime import datetime
import requests


class Robotstxt:
    COLLECTION_NAME = 'robotstxt'

    def __init__(self, configuration: Configuration, connection: Connection):
        self.configuration = configuration
        self.connection = connection
        self.robots_config = self.configuration.aggregations.get_custom_configuration_aggregation('robotstxt')

    def run(self):
        print('Running aggregation robotstxt: ')
        robotstxt_data = []

        for urlset_name in self.robots_config.urlsets:
            print(' - "' + urlset_name + '":')
            for url in self.configuration.urlsets.urlset_urls(urlset_name):
                urlstr = str(url)
                if not urlstr.endswith('/robots.txt'):
                    robotsstr = url.protocol + '://' + url.domain + str.rstrip(url.path, '/') + '/robots.txt'
                    url = URL(robotsstr)
                robotstxt_data.append(self._process_robotstxt(urlset_name, url))

        self.connection.mongodb.insert_documents(Robotstxt.COLLECTION_NAME, robotstxt_data)

    def _process_robotstxt(self, urlset_name: str, url: URL) -> dict:
        print('   + ' + str(url), end='')

        try:
            headers = {
                'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
            }

            response = requests.get(url, headers=headers)
            headers = {key: value for key, value in response.headers.items()}
            status_code = response.status_code
            body = response.content

            if type(body) is bytes:
                body = body.decode('utf-8')
        except requests.RequestException as error:
            body = 'Error: ' + str(error)
            status_code = None
            headers = None

        print(' ... ' + str(status_code) if status_code is not None else 'Error')

        return {
            'urlset': urlset_name,
            'url': url.__dict__,
            'status_code': status_code,
            'body': body,
            'headers': headers,
            'date': datetime.now()
        }
