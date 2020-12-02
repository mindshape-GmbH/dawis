from database.connection import Connection
from utilities.configuration import Configuration
from utilities.url import URL
from utilities.thread import ResultThread
from datetime import datetime
import requests
import time


class Pagespeed:
    API_URL = 'https://www.googleapis.com/pagespeedonline/v5/runPagespeed'
    COLLECTION_NAME = 'pagespeed'

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection

        self.apikey = self.module_configuration.settings['apikey']

    def run(self):
        print('Running aggregation pagespeed: ')
        pagespeed_tests = []
        threads = []

        for urlset_name in self.module_configuration.urlsets:
            print(' - "' + urlset_name + '":')

            for url in self.configuration.urlsets.urlset_urls(urlset_name):
                thread = ResultThread(_process_pagespeed, [self.apikey, urlset_name, url])
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()
            pagespeed_tests.append(thread.result)

        self.connection.mongodb.insert_documents(Pagespeed.COLLECTION_NAME, pagespeed_tests)


def _process_pagespeed(apikey: str, urlset_name: str, url: URL) -> dict:
    result_desktop = _process_api(apikey, str(url))
    result_mobile = _process_api(apikey, str(url), 'mobile')

    if result_desktop['status_code'] == 429:
        time.sleep(1)
        result_desktop = _process_api(apikey, str(url))
    if result_mobile['status_code'] == 429:
        time.sleep(1)
        result_mobile = _process_api(apikey, str(url), 'mobile')

    return {
        'urlset': urlset_name,
        'url': url.__dict__,
        'desktop': result_desktop,
        'mobile': result_mobile
    }


def _process_api(apikey: str, url: str, strategy: str = 'desktop', categories=None) -> dict:
    parameters = '?strategy=' + strategy + '&url=' + url

    if categories is not None:
        for category in categories:
            parameters += '&category=' + category
    else:
        parameters += '&category=performance&fields=lighthouseResult'

    if '' != apikey and apikey is not None:
        parameters += '&key=' + apikey

    print('   + ' + url + ' (' + strategy + ')', end='')

    try:
        headers = {
            'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
        }

        response = requests.get(Pagespeed.API_URL + parameters, headers=headers)
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
        'status_code': status_code,
        'body': body,
        'headers': headers,
        'date': datetime.utcnow()
    }
