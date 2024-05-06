from database.connection import Connection
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationInvalidError, ConfigurationMissingError
from utilities.thread import ResultThread
from utilities.validator import Validator
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import HttpRequest
from datetime import datetime, timedelta
from dict_hash import sha256
from time import time, sleep
from typing import Sequence
import utilities.datetime as datetime_utility
import dateutil
import re


class _InvalidDataException(Exception):
    def __str__(self):
        return 'Invalid data returned from API, the site is maybe broken'


class GooglePagespeed:
    COLLECTION_NAME = 'google_pagespeed'
    COLLECTION_NAME_RETRY = 'google_pagespeed_retry'

    STRATEGIES_ALLOWED = ['desktop', 'mobile', 'both']
    MAX_PARALLEL_REQUESTS = 10
    MAX_RETRY_COUNT = 3
    SECONDS_BETWEEN_REQUESTS = 3
    SECONDS_BETWEEN_REQUESTS_CHUNKS = 10

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.timezone = configuration.databases.timezone
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection
        self.mongodb = connection.mongodb
        self.bigquery = None

    def run(self):
        print('Running Google Pagespeed Module:')
        timer_run = time()
        api_key = None

        if 'bigquery' == self.module_configuration.database:
            self.bigquery = self.connection.bigquery

        if 'apiKey' in self.module_configuration.settings and type(self.module_configuration.settings['apiKey']) is str:
            api_key = self.module_configuration.settings['apiKey']

        if 'configurations' in self.module_configuration.settings and \
            type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                self._process_configuration_cluster(configuration, api_key, self.module_configuration.database)

        if self.mongodb.has_collection(self.COLLECTION_NAME_RETRY):
            for configuration in self.mongodb.find(self.COLLECTION_NAME_RETRY, {}):
                requests = configuration.pop('retry_requests', [])
                self._process_configuration_requests(configuration, requests, self.module_configuration.database)

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration_cluster(self, configuration: dict, api_key: str, database: str):
        strategies = ['DESKTOP']
        cluster = {}

        if 'cluster' in configuration and type(configuration['cluster']) is dict:
            for cluster_name, urls in configuration['cluster'].items():
                for url in urls:
                    if type(url) is not str:
                        raise ConfigurationInvalidError('Invalid url')
                    elif not Validator.validate_url(url):
                        raise ConfigurationInvalidError('Invalid url')

                cluster[cluster_name] = urls

        if 'strategy' in configuration and type(configuration['strategy']) is str:
            if configuration['strategy'] in self.STRATEGIES_ALLOWED:
                if 'both' == configuration['strategy']:
                    strategies = ['DESKTOP', 'MOBILE']
                else:
                    strategies = [str.upper(configuration['strategy'])]
            else:
                raise ConfigurationInvalidError('invalid strategy for pagespeed')

        if 'apiKey' in configuration and type(configuration['apiKey']) is str:
            api_key = configuration['apiKey']
        else:
            configuration['apiKey'] = api_key

        requests = []

        if 'requests' in configuration:
            requests = configuration['retry_requests']
        else:
            for cluster_name, urls in cluster.items():
                for url in urls:
                    for strategy in strategies:
                        requests.append([url, cluster_name, strategy, api_key, self.MAX_RETRY_COUNT])

        self._process_configuration_requests(configuration, requests, database)

    def _process_configuration_requests(self, configuration: dict, requests: list, database: str):
        table_reference = None
        log_table_reference = None
        responses = []
        log = []

        if 'bigquery' == database:
            if 'tablename' in configuration and type(configuration['tablename']) is str:
                table_name = configuration['tablename']
            else:
                raise ConfigurationMissingError('Missing tablename for pagespeed to bigquery')

            dataset_name = None

            if 'dataset' in configuration and type(configuration['dataset']) is str:
                dataset_name = configuration['dataset']

            table_reference = self.connection.bigquery.table_reference(table_name, dataset_name)

            if 'logTablename' in configuration and type(configuration['logTablename']) is str:
                log_table_reference = self.connection.bigquery.table_reference(
                    configuration['logTablename'],
                    dataset_name
                )

        responses, failed_requests, log = self._process_requests(requests, responses, log)

        if 0 < len(failed_requests):
            configuration.pop('cluster', None)
            configuration.pop('retry_requests', None)

            if '_id' in configuration:
                self.mongodb.update_one(
                    self.COLLECTION_NAME_RETRY,
                    configuration['_id'],
                    {'retry_requests': failed_requests}
                )
            else:
                configuration['hash'] = sha256(configuration)
                existing_configuration = None

                if self.mongodb.has_collection(self.COLLECTION_NAME_RETRY):
                    existing_configuration = self.mongodb.find_one(
                        self.COLLECTION_NAME_RETRY,
                        {'hash': configuration['hash']},
                        True
                    )

                if type(existing_configuration) is dict:
                    self.mongodb.update_one(
                        self.COLLECTION_NAME_RETRY,
                        existing_configuration['_id'],
                        {'retry_requests': failed_requests}
                    )
                else:
                    configuration['retry_requests'] = failed_requests
                    self.mongodb.insert_document(
                        self.COLLECTION_NAME_RETRY,
                        {'retry_requests': failed_requests, **configuration}
                    )
        elif '_id' in configuration:
            self.mongodb.delete_one(self.COLLECTION_NAME_RETRY, configuration['_id'])

        if 'bigquery' == database:
            self._process_responses_for_bigquery(responses, table_reference)

            if type(log_table_reference) is TableReference:
                self._process_log_for_bigquery(log, log_table_reference)
        else:
            self._process_responses_for_mongodb(responses)

    def _process_requests(self, requests: list, responses: list, log: list) -> tuple:
        status_code_regex = re.compile(r'status[\s\-_]code:?\s?(\d+)', re.IGNORECASE)
        failed_requests = []

        requests_chunks = [
            requests[i:i + GooglePagespeed.MAX_PARALLEL_REQUESTS]
            for i in range(0, len(requests), GooglePagespeed.MAX_PARALLEL_REQUESTS)
        ]

        for requests_chunk in requests_chunks:
            threads = []

            for request in requests_chunk:
                thread = ResultThread(self._process_pagespeed_api, request[:4], {'retry_counter': request[4]})
                thread.start()
                threads.append(thread)
                sleep(self.SECONDS_BETWEEN_REQUESTS)

            for thread in threads:
                thread.join()
                request = thread.get_arguements()
                retry_counter = thread.get_data('retry_counter')

                if isinstance(thread.exception, Exception) or type(thread.result) is not dict:
                    status_code = None

                    if type(thread.exception) is HttpError:
                        match = status_code_regex.search(thread.exception.__str__())

                        if type(match) is re.Match:
                            status_code = int(match.group(1))

                    log.append({
                        'url': request[0],
                        'cluster': request[1],
                        'strategy': request[2],
                        'date': datetime_utility.now(self.timezone),
                        'statusCode': status_code,
                        'message': thread.exception.__str__()
                    })

                    if 0 < retry_counter:
                        retry_counter = retry_counter - 1
                        failed_requests.append([*request, retry_counter])
                else:
                    response = thread.result
                    responses.append(response)

                    log.append({
                        'url': request[0],
                        'cluster': request[1],
                        'strategy': request[2],
                        'date': datetime_utility.now(self.timezone),
                        'statusCode': response['statusCode'],
                        'message': None
                    })

            if len(requests_chunks) != requests_chunks.index(requests_chunk) + 1:
                sleep(GooglePagespeed.SECONDS_BETWEEN_REQUESTS_CHUNKS)

        return responses, failed_requests, log

    def _process_pagespeed_api(
        self,
        url: str,
        cluster: str,
        strategy: str,
        api_key: str
    ) -> dict:
        pagespeed_api = build(
            'pagespeedonline',
            'v5',
            developerKey=api_key,
            cache_discovery=False
        ).pagespeedapi()

        request: HttpRequest = pagespeed_api.runpagespeed(url=url, strategy=strategy)
        return self._process_response(request.execute(), url, cluster, strategy)

    def _process_responses_for_mongodb(self, responses: Sequence[dict]):
        self.mongodb.insert_documents(GooglePagespeed.COLLECTION_NAME, responses)

    def _process_responses_for_bigquery(self, data: Sequence[dict], table_reference: TableReference):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')

        loading_experience_schema_fields = (
            SchemaField('cls', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('clsGood', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('clsMedium', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('clsBad', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('lcp', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('lcpGood', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('lcpMedium', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('lcpBad', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('fcp', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('fcpGood', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('fcpMedium', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('fcpBad', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('fid', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('fidGood', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('fidMedium', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('fidBad', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('inp', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('inpGood', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('inpMedium', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('inpBad', SqlTypeNames.FLOAT, 'REQUIRED'),
        )

        job_config.schema = (
            SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('strategy', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('statusCode', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('cluster', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('labdata', SqlTypeNames.RECORD, 'REQUIRED', fields=(
                SchemaField('cls', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('lcp', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('fcp', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('tbt', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('mpfid', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('ttfb', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('performanceScore', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('serverResponseTime', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('usesTextCompression', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('usesLongCacheTtl', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('domSize', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('offscreenImages', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('usesOptimizedImages', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('usesResponsiveImages', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('renderBlockingResources', SqlTypeNames.FLOAT),
                SchemaField('bootupTime', SqlTypeNames.FLOAT, 'REQUIRED'),
                SchemaField('mainthreadWorkBreakdown', SqlTypeNames.FLOAT, 'REQUIRED'),
            )),
            SchemaField(
                'originLoadingExperience',
                SqlTypeNames.RECORD,
                'REQUIRED',
                fields=loading_experience_schema_fields
            ),
            SchemaField('loadingExperience', SqlTypeNames.RECORD, fields=loading_experience_schema_fields)
        )

        for data_item in data:
            data_item['date'] = data_item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        load_job = self.bigquery.client.load_table_from_json(data, table_reference, job_config=job_config)
        load_job.result()

    def _process_log_for_bigquery(self, log: Sequence[dict], table_reference: TableReference):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')

        job_config.schema = (
            SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('cluster', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('strategy', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('statusCode', SqlTypeNames.INTEGER),
            SchemaField('message', SqlTypeNames.STRING),
        )

        for log_item in log:
            log_item['date'] = log_item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        load_job = self.bigquery.client.load_table_from_json(log, table_reference, job_config=job_config)
        load_job.result()

    def _process_response(self, response: dict, url: str, cluster: str, strategy: str) -> dict:
        loading_experience_dummy = lambda x: {
            'cls': response[x]['metrics']['CUMULATIVE_LAYOUT_SHIFT_SCORE']['percentile'],
            'clsGood': response[x]['metrics']['CUMULATIVE_LAYOUT_SHIFT_SCORE']['distributions'][0]['proportion'],
            'clsMedium': response[x]['metrics']['CUMULATIVE_LAYOUT_SHIFT_SCORE']['distributions'][1]['proportion'],
            'clsBad': response[x]['metrics']['CUMULATIVE_LAYOUT_SHIFT_SCORE']['distributions'][2]['proportion'],
            'lcp': response[x]['metrics']['LARGEST_CONTENTFUL_PAINT_MS']['percentile'],
            'lcpGood': response[x]['metrics']['LARGEST_CONTENTFUL_PAINT_MS']['distributions'][0]['proportion'],
            'lcpMedium': response[x]['metrics']['LARGEST_CONTENTFUL_PAINT_MS']['distributions'][1]['proportion'],
            'lcpBad': response[x]['metrics']['LARGEST_CONTENTFUL_PAINT_MS']['distributions'][2]['proportion'],
            'fcp': response['originLoadingExperience']['metrics']['FIRST_CONTENTFUL_PAINT_MS']['percentile'],
            'fcpGood': response[x]['metrics']['FIRST_CONTENTFUL_PAINT_MS']['distributions'][0]['proportion'],
            'fcpMedium': response[x]['metrics']['FIRST_CONTENTFUL_PAINT_MS']['distributions'][1]['proportion'],
            'fcpBad': response[x]['metrics']['FIRST_CONTENTFUL_PAINT_MS']['distributions'][2]['proportion'],
            'fid': response['originLoadingExperience']['metrics']['FIRST_INPUT_DELAY_MS']['percentile'],
            'fidGood': response[x]['metrics']['FIRST_INPUT_DELAY_MS']['distributions'][0]['proportion'],
            'fidMedium': response[x]['metrics']['FIRST_INPUT_DELAY_MS']['distributions'][1]['proportion'],
            'fidBad': response[x]['metrics']['FIRST_INPUT_DELAY_MS']['distributions'][2]['proportion'],
            'inp': response['originLoadingExperience']['metrics']['INTERACTION_TO_NEXT_PAINT']['percentile'],
            'inpGood': response[x]['metrics']['INTERACTION_TO_NEXT_PAINT']['distributions'][0]['proportion'],
            'inpMedium': response[x]['metrics']['INTERACTION_TO_NEXT_PAINT']['distributions'][1]['proportion'],
            'inpBad': response[x]['metrics']['INTERACTION_TO_NEXT_PAINT']['distributions'][2]['proportion'],
        }

        status_code = int(
            response['lighthouseResult']['audits']['network-requests']['details']['items'][0]['statusCode']
        )

        data = {
            'url': url,
            'strategy': strategy,
            'statusCode': status_code,
            'date': dateutil.parser.parse(response['analysisUTCTimestamp']),
            'cluster': cluster,
            'labdata': {
                'cls': response['lighthouseResult']['audits']['cumulative-layout-shift']['numericValue'],
                'lcp': response['lighthouseResult']['audits']['largest-contentful-paint']['numericValue'],
                'fcp': response['lighthouseResult']['audits']['first-contentful-paint']['numericValue'],
                'tbt': response['lighthouseResult']['audits']['total-blocking-time']['numericValue'],
                'mpfid': response['lighthouseResult']['audits']['max-potential-fid']['numericValue'],
                'ttfb': response['lighthouseResult']['audits']['server-response-time']['numericValue'],
                'performanceScore': response['lighthouseResult']['categories']['performance']['score'],
                'serverResponseTime': response['lighthouseResult']['audits']['server-response-time']['score'],
                'usesTextCompression': response['lighthouseResult']['audits']['uses-text-compression']['score'],
                'usesLongCacheTtl': response['lighthouseResult']['audits']['uses-long-cache-ttl']['score'],
                'domSize': response['lighthouseResult']['audits']['dom-size']['score'],
                'offscreenImages': response['lighthouseResult']['audits']['offscreen-images']['score'],
                'usesOptimizedImages': response['lighthouseResult']['audits']['uses-optimized-images']['score'],
                'usesResponsiveImages': response['lighthouseResult']['audits']['uses-responsive-images']['score'],
                'renderBlockingResources': response['lighthouseResult']['audits']['render-blocking-resources']['score'],
                'bootupTime': response['lighthouseResult']['audits']['bootup-time']['score'],
                'mainthreadWorkBreakdown': response['lighthouseResult']['audits']['mainthread-work-breakdown']['score'],
            },
            'originLoadingExperience': loading_experience_dummy('originLoadingExperience'),
            'loadingExperience': None,
        }

        if 'loadingExperience' in response and type(response['loadingExperience']) is dict and (
            'origin_fallback' not in response['loadingExperience'] or
            response['loadingExperience']['origin_fallback'] is not True
        ):
            data['loadingExperience'] = loading_experience_dummy('loadingExperience')

        if not self._validate_response_data(data):
            raise _InvalidDataException()

        return data

    def _validate_response_data(self, data: dict) -> bool:
        if type(data['date']) is not datetime:
            return False

        if type(data['statusCode']) is not int:
            return False

        if 'labdata' not in data:
            return False

        for data_key in ['url', 'strategy', 'cluster']:
            if type(data[data_key]) is not str:
                return False

        for data_key in [
            'cls',
            'lcp',
            'fcp',
            'tbt',
            'mpfid',
            'ttfb',
            'performanceScore',
            'serverResponseTime',
            'usesTextCompression',
            'usesLongCacheTtl',
            'domSize',
            'offscreenImages',
            'usesOptimizedImages',
            'usesResponsiveImages',
            'bootupTime',
            'mainthreadWorkBreakdown',
        ]:
            if data_key not in data['labdata']:
                return False
            if type(data['labdata'][data_key]) is not float and type(data['labdata'][data_key]) is not int:
                return False

        if not self._validate_response_data_loading_experience(data, 'originLoadingExperience'):
            return False

        if 'loadingExperience' in data and type(data['loadingExperience']) is dict:
            if not self._validate_response_data_loading_experience(data, 'loadingExperience'):
                return False

        return True

    @staticmethod
    def _validate_response_data_loading_experience(data: dict, parent_data_key: str) -> bool:
        for data_key in [
            'cls',
            'lcp',
            'fcp',
            'fid',
            'inp',
        ]:
            if data_key not in data[parent_data_key]:
                return False
            if type(data[parent_data_key][data_key]) is not int:
                return False

        for data_key in [
            'clsGood',
            'clsMedium',
            'clsBad',
            'lcpGood',
            'lcpMedium',
            'lcpBad',
            'fcpGood',
            'fcpMedium',
            'fcpBad',
            'fidGood',
            'fidMedium',
            'fidBad',
            'inpGood',
            'inpMedium',
            'inpBad',
        ]:
            if data_key not in data[parent_data_key]:
                return False
            if type(data[parent_data_key][data_key]) is not float and type(data[parent_data_key][data_key]) is not int:
                return False

        return True
