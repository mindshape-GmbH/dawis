from database.connection import Connection
from utilities.configuration import ConfigurationUrlset, Configuration, ConfigurationUrl
from utilities.url import URL
from utilities.thread import ResultThread
from datetime import datetime
from typing import Sequence
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import requests


class HtmlParser:
    DEFAULT_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
    COLLECTION_NAME = 'html_parser'

    def __init__(self, configuration: Configuration, connection: Connection):
        self.configuration = configuration
        self.connection = connection

    def run(self):
        print('Running aggregation html_parser:')

        html_parser_config = self.configuration.aggregations.get_custom_configuration_aggregation('html_parser')
        html_parser_urlsets = html_parser_config.urlsets
        html_parser_settings = html_parser_config.settings

        data = []

        for urlset in self.configuration.urlsets.urlsets:
            for urlset_name in html_parser_urlsets:
                if urlset_name == urlset.name:
                    config_hash = self.configuration.hash
                    data.extend(_process_urlset(urlset, html_parser_settings, config_hash))

        self.connection.mongodb.insert_documents(HtmlParser.COLLECTION_NAME, data)


def _process_urlset(configuration_urlset: ConfigurationUrlset, settings: dict, config_hash: str) -> Sequence[dict]:
    urls = []

    print(' - "' + configuration_urlset.name + '":')

    threads = []

    for configuration_url in configuration_urlset.configuration_urls:
        thread = ResultThread(_process_url, [
            configuration_urlset.name,
            str(configuration_url.url),
            configuration_url.render,
            settings,
            config_hash
        ])

        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
        urls.append(thread.result)

    return urls


def _process_url(urlset: str, url: str, renderbool: bool, settings: dict, config_hash: str) -> dict:
    try:
        headers = {
            'User-agent': settings['useragent'] if settings['useragent'] else HtmlParser.DEFAULT_USER_AGENT
        }

        response = requests.get(url, headers=headers)
        headers = {key: value for key, value in response.headers.items()}
        status_code = response.status_code
        num_redirects = 0
        redirects = []
        ttfb = response.elapsed.microseconds / 1000.0

        if response.history:
            for resp in response.history:
                redirects.append({'status_code': resp.status_code, 'url': resp.url,
                                  'headers': {key: value for key, value in resp.headers.items()},
                                  'ttfb': resp.elapsed.microseconds / 1000.0})
                num_redirects += 1

            redirects.append({'status_code': response.status_code, 'url': response.url,
                              'headers': {key: value for key, value in response.headers.items()},
                              'ttfb': response.elapsed.microseconds / 1000.0})

        content_type = response.headers.get('content-type')

        if str.startswith(content_type, 'text/html'):
            body = _render_url(url) if renderbool else response.content
            if type(body) is bytes:
                body = body.decode('utf-8')
        else:
            body = 'Can\'t use content-type "' + content_type + '" for parsing'
    except requests.RequestException as error:
        body = 'Error: ' + str(error)
        headers = {}
        status_code = 0
        num_redirects = 0
        redirects = []
        ttfb = 0.0

    return {
        'urlset': urlset,
        'url': URL(url).__dict__,
        'status_code': status_code,
        'num_redirects': num_redirects,
        'redirects': redirects,
        'ttfb': ttfb,
        'body': body,
        'rendered': renderbool,
        'date': datetime.utcnow(),
        'headers': headers,
        'configuration_hash': config_hash,
    }


# Todo: refactor making things work with docker, see: "webdriver.Remote"
def _render_url(url: str) -> str:
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        html = driver.page_source.encode('utf-8')
        driver.close()
    except WebDriverException:
        html = 'Error: chromedriver not configured properly'

    return html
